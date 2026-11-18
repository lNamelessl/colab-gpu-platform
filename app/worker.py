"""Job execution worker + checkpoint snapshot/restore + reclaim-resume."""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from . import config, gauth
from .colab_api import RuntimeNotFound, friendly_create_error
from .db import db
from .jupyter_exec import (
    ConnectionLost,
    ExecutionError,
    ExecutionTimeout,
    JupyterClient,
    JupyterError,
)
from .webhooks import fire_event

log = logging.getLogger("worker")

# live run log subscribers: run_id -> set of asyncio.Queue
_subscribers: dict[str, set] = {}


async def subscribe(run_id: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _subscribers.setdefault(run_id, set()).add(q)
    return q


def unsubscribe(run_id: str, q: asyncio.Queue) -> None:
    subs = _subscribers.get(run_id)
    if subs:
        subs.discard(q)
        if not subs:
            _subscribers.pop(run_id, None)


def _platform_base_url() -> str:
    if config.PUBLIC_BASE_URL:
        return config.PUBLIC_BASE_URL
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    return f"https://{domain}" if domain else ""


def _env_prelude(job: dict, run: dict) -> str:
    base = _platform_base_url()
    lines = [
        "import os as _os",
        f"_os.environ['PLATFORM_URL'] = {base!r}",
        f"_os.environ['JOB_ID'] = {job['id']!r}",
        f"_os.environ['RUN_ID'] = {run['id']!r}",
        f"_os.environ['JOB_NAME'] = {job['name']!r}",
        f"_os.environ['ATTEMPT'] = {run['attempt']!r}",
        f"_os.environ['ACCELERATOR'] = {job['accelerator']!r}",
        f"_os.environ['TIMEOUT_MIN'] = {job['timeout_min']!r}",
        f"_os.environ['CHECKPOINT_PATH'] = {config.RUNTIME_CHECKPOINT_DIR + '/' + job['id']!r}",
        f"_os.environ['ARTIFACT_DIR'] = {config.RUNTIME_ARTIFACT_DIR!r}",
        "import pathlib as _pl",
        f"_pl.Path({config.RUNTIME_CHECKPOINT_DIR!r}, {job['id']!r}).mkdir(parents=True, exist_ok=True)",
        f"_pl.Path({config.RUNTIME_ARTIFACT_DIR!r}).mkdir(parents=True, exist_ok=True)",
    ]
    if config.APP_ACCESS_PASSWORD:
        lines.append(
            f"_os.environ['PLATFORM_PASSWORD'] = {config.APP_ACCESS_PASSWORD!r}"
        )
    return "\n".join(lines)


CHECKPOINT_SNIPPET = '''

def platform_checkpoint():
    """Snapshot CHECKPOINT_PATH to the platform volume (job-code convention).

    Call this periodically from long jobs: the platform stores the files so a
    resumed attempt (after reclaim) gets them re-uploaded to the fresh runtime
    before the main code runs. Must be called while a cell is still executing
    (it is NOT keep-alive: it only copies files you already wrote).
    """
    import os as _cko, json as _ckj, urllib.request as _cku
    _root = _cko.environ.get('CHECKPOINT_PATH', '')
    if not _root or not _cko.path.isdir(_root):
        return {'saved': 0, 'reason': 'no checkpoint dir'}
    _files = []
    for _dp, _dns, _fns in _cko.walk(_root):
        for _fn in _fns:
            _fp = _cko.path.join(_dp, _fn)
            if _cko.path.getsize(_fp) <= 45 * 1024 * 1024:
                _files.append(_fp)
    _payload = _ckj.dumps({'job_id': _cko.environ.get('JOB_ID', ''), 'files': _files}).encode()
    _url = _cko.environ.get('PLATFORM_URL', '').rstrip('/') + '/api/jobs/' + _cko.environ.get('JOB_ID', '') + '/checkpoint'
    _req = _cku.Request(_url, data=_payload, headers={'Content-Type': 'application/json'})
    _pw = _cko.environ.get('PLATFORM_PASSWORD', '')
    if _pw:
        _req.add_header('X-Platform-Password', _pw)
    try:
        _resp = _cku.urlopen(_req, timeout=60)
        return _ckj.loads(_resp.read().decode())
    except Exception as _e:  # noqa: BLE001
        return {'saved': 0, 'reason': str(_e)}
'''


async def broadcast(run_id: str, line: str) -> None:
    for q in list(_subscribers.get(run_id, set())):
        await q.put(line)


async def _log(run: dict, line: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    full = f"{ts} {line}"
    await db.append_log(run["id"], full)
    await broadcast(run["id"], full)


class Worker:
    def __init__(self):
        self.in_flight: set[str] = set()
        self.job_locks: dict[str, asyncio.Lock] = {}

    async def enqueue(self, job_id: str, attempt: int = 1, trigger: str = "manual") -> dict:
        run = await db.create_run(job_id, attempt=attempt, trigger=trigger)
        log.info("queued run %s (job %s attempt %s)", run["id"], job_id, attempt)
        return run

    async def run_loop(self):
        while True:
            try:
                active = await db.active_runs()
                slots = max(0, config.MAX_CONCURRENT_JOBS - len(self.in_flight))
                if slots > 0:
                    queued = await db.queued_runs()
                    for run in queued[:slots]:
                        if run["id"] in self.in_flight:
                            continue
                        self.in_flight.add(run["id"])
                        asyncio.create_task(self._execute(run))
            except Exception as e:  # noqa: BLE001
                log.error("run loop error: %s", e)
            await asyncio.sleep(1.0)

    def _lock_for(self, job_id: str) -> asyncio.Lock:
        if job_id not in self.job_locks:
            self.job_locks[job_id] = asyncio.Lock()
        return self.job_locks[job_id]

    async def _execute(self, run: dict):
        job = await db.get_job(run["job_id"])
        if not job:
            self.in_flight.discard(run["id"])
            return
        async with self._lock_for(job["id"]):
            try:
                await self._run_one(job, run)
            except Exception as e:  # noqa: BLE001
                log.exception("run %s crashed", run["id"])
                await db.update_run(
                    run["id"], status="FAILED", error=str(e)[:2000],
                    finished_at=db.now(),
                )
                await _log(run, f"FATAL {e}")
                await fire_event("FAILED", job, run, str(e))
            finally:
                self.in_flight.discard(run["id"])

    async def _run_one(self, job: dict, run: dict):
        await db.update_run(run["id"], status="PROVISIONING", started_at=db.now())
        await _log(
            run,
            f"run attempt {run['attempt']} trigger={run['trigger']} "
            f"accelerator={job['accelerator']} shape={job['shape']}",
        )

        # ---- provision (with broken-VM retry: a fresh runtime whose kernel
        # never leaves "starting" is hung — tear it down and reprovision) ----
        client: JupyterClient | None = None
        handle = None
        for attempt_vm in range(3):
            try:
                handle = await self._manager.acquire(
                    job["accelerator"], job["shape"], log_fn=lambda s: _log(run, s)
                )
            except Exception as e:  # noqa: BLE001
                msg = friendly_create_error(e)
                await db.update_run(run["id"], status="FAILED", error=msg, finished_at=db.now())
                await _log(run, f"PROVISION FAILED: {msg}")
                await fire_event("FAILED", job, run, msg)
                return
            await db.update_run(run["id"], runtime_id=handle.runtime_id)
            handle = await self._manager.ensure_fresh(handle)
            client = JupyterClient(handle)
            adopted = False
            try:
                await client.create_session(f"job-{job['id'][:8]}-{run['id'][:8]}")
                state = await client.wait_kernel_idle(timeout_s=300)
                await _log(run, f"boot probe on {handle.runtime_id[:14]}: kernel state {state}")
                if state == "idle":
                    adopted = True
                    break
                await _log(run, f"runtime VM looks hung (state={state}); reprovisioning...")
            finally:
                if not adopted:
                    try:
                        await client.delete_session()
                    except Exception:  # noqa: BLE001
                        pass
                    await client.close()
                    await self._manager.release(handle.runtime_id, delete=True)
                    client = None
                    handle = None
        if client is None or handle is None:
            msg = "provisioning failed: 3 runtimes booted broken kernels"
            await db.update_run(run["id"], status="FAILED", error=msg, finished_at=db.now())
            await _log(run, f"PROVISION FAILED: {msg}")
            await fire_event("FAILED", job, run, msg)
            return

        deadline = job["timeout_min"] * 60
        try:
            # ---- setup: upload code ----
            job_dir = f"{config.RUNTIME_JOB_DIR}/{run['id']}"
            code_path = f"{job_dir}/main.py"
            await client.upload_file(code_path, job["code"].encode("utf-8"))

            # ---- restore checkpoint for resumed attempts ----
            restored = 0
            if run["attempt"] > 1:
                restored = await self._restore_checkpoint(job, run, client)
                if restored:
                    await db.update_run(run["id"], restored_checkpoint=1)

            # ---- execute ----
            await db.update_run(run["id"], status="RUNNING")
            prelude = _env_prelude(job, run)
            code = (
                prelude
                + CHECKPOINT_SNIPPET
                + "\n# ==== job code ====\nexec(compile(open(" + repr(code_path) + ").read(), "
                + repr(code_path) + ", 'exec'))\n"
            )
            if job.get("checkpoint_command"):
                code = (
                    prelude
                    + CHECKPOINT_SNIPPET
                    + "\n# ==== checkpoint command (pre-run) ====\n"
                    + job["checkpoint_command"]
                    + "\n\n# ==== job code ====\nexec(compile(open("
                    + repr(code_path) + ").read(), " + repr(code_path) + ", 'exec'))\n"
                )
            await _log(run, "executing on runtime...")
            try:
                out = await client.execute(code, timeout_s=deadline, on_output=lambda s: _log(run, s))
            except ExecutionError as e:
                err = f"{e.ename}: {e.evalue}"
                await self._finish_failed(job, run, client, err, handle)
                return
            await _log(run, "execution finished ok")
            if out:
                pass  # outputs already streamed

            # ---- harvest artifacts ----
            n_art = await self._harvest_artifacts(job, run, client)
            # ---- snapshot final checkpoint ----
            n_ckpt = await self._snapshot_checkpoint(job, run, client, final=True)
            if n_art or n_ckpt:
                await _log(run, f"harvested {n_art} artifact(s), {n_ckpt} checkpoint file(s)")
            await db.update_run(
                run["id"], status="COMPLETE", finished_at=db.now(), error=None
            )
            await fire_event("COMPLETE", job, run, None)
            # Delete the runtime on completion unless RUNTIME_REUSE keeps it
            # (the refresher reaps idle reused runtimes after RUNTIME_REUSE_TTL_S).
            if config.RUNTIME_REUSE:
                self._manager.touch(handle.runtime_id)
            else:
                await self._manager.release(handle.runtime_id, delete=True)
        except ExecutionTimeout as e:
            # timeout: the run is done — mark FAILED and tear down (no keep-alive).
            await self._finish_failed(job, run, client, str(e), handle)
        except (ConnectionLost, RuntimeNotFound) as e:
            # mid-run connection death (reclaimed by Colab, network, kernel dead)
            await self._handle_reclaim(job, run, handle, str(e))
        except JupyterError as e:
            await self._finish_failed(job, run, client, str(e), handle)
        finally:
            try:
                await client.delete_session()
            except Exception:  # noqa: BLE001
                pass
            await client.close()

    async def _finish_failed(self, job, run, client: JupyterClient, err: str, handle):
        await db.update_run(run["id"], status="FAILED", error=err[:2000], finished_at=db.now())
        await _log(run, f"FAILED: {err}")
        try:
            await self._snapshot_checkpoint(job, run, client, final=True)
        except Exception:  # noqa: BLE001
            pass
        await fire_event("FAILED", job, run, err)
        await self._manager.release(handle.runtime_id, delete=True)

    async def _handle_reclaim(self, job, run, handle, reason: str):
        await db.update_run(
            run["id"], status="RECLAIMED", error=reason[:2000], finished_at=db.now()
        )
        await _log(run, f"RECLAIMED: {reason}")
        await self._manager.release(handle.runtime_id, delete=False)
        await fire_event("RECLAIMED", job, run, reason)
        # auto-reprovision: new attempt re-runs same code; checkpoint restore
        # happens before main code if a snapshot exists.
        new_run = await self.enqueue(job["id"], attempt=run["attempt"] + 1, trigger="resume")
        await _log(new_run, f"auto-resume enqueued after reclaim of attempt {run['attempt']}")

    # ---- checkpoint plumbing ----
    async def _snapshot_checkpoint(self, job, run, client: JupyterClient,
                                   final: bool = False) -> int:
        ck_dir = f"{config.RUNTIME_CHECKPOINT_DIR}/{job['id']}"
        try:
            entries = await client.list_dir(ck_dir)
        except Exception as e:  # noqa: BLE001
            await _log(run, f"checkpoint list failed: {e}")
            return 0
        if not entries:
            return 0
        cap = config.CHECKPOINT_MAX_TOTAL_MB * 1024 * 1024
        total = 0
        stored: list[dict] = []
        dest_root = Path(config.DATA_DIR) / "checkpoints" / job["id"]
        dest_root.mkdir(parents=True, exist_ok=True)
        for e in entries:
            if e.get("type") != "file":
                continue
            size = e.get("size") or 0
            if size > 45 * 1024 * 1024 or total + size > cap:
                await _log(run, f"checkpoint {e.get('name')} skipped (size cap)")
                continue
            try:
                data = await client.download_file(f"{ck_dir}/{e.get('name')}")
                (dest_root / e.get("name")).write_bytes(data)
                total += len(data)
                stored.append({"name": e.get("name"), "size": len(data)})
            except Exception as ex:  # noqa: BLE001
                await _log(run, f"checkpoint pull {e.get('name')} failed: {ex}")
        if stored:
            await db.save_checkpoint_manifest(
                job["id"],
                {"files": stored, "final": final, "run_id": run["id"],
                 "attempt": run["attempt"], "saved_at": db.now()},
            )
        return len(stored)

    async def _restore_checkpoint(self, job, run, client: JupyterClient) -> int:
        manifest = await db.get_checkpoint_manifest(job["id"])
        if not manifest or not manifest.get("files"):
            return 0
        src_root = Path(config.DATA_DIR) / "checkpoints" / job["id"]
        ck_dir = f"{config.RUNTIME_CHECKPOINT_DIR}/{job['id']}"
        try:
            await client.mkdirs(ck_dir)
        except Exception as e:  # noqa: BLE001
            await _log(run, f"checkpoint dir create failed: {e}")
            return 0
        n = 0
        for f in manifest["files"]:
            src = src_root / f["name"]
            if not src.exists():
                continue
            try:
                await client.upload_file(f"{ck_dir}/{f['name']}", src.read_bytes())
                n += 1
            except Exception as e:  # noqa: BLE001
                await _log(run, f"checkpoint restore {f['name']} failed: {e}")
        await _log(run, f"restored {n} checkpoint file(s) from volume snapshot")
        return n

    # ---- artifacts ----
    async def _harvest_artifacts(self, job, run, client: JupyterClient) -> int:
        try:
            entries = await client.list_dir(config.RUNTIME_ARTIFACT_DIR)
        except Exception as e:  # noqa: BLE001
            await _log(run, f"artifact list failed: {e}")
            return 0
        cap = config.ARTIFACT_TOTAL_CAP_MB * 1024 * 1024
        total = 0
        n = 0
        dest_root = Path(config.DATA_DIR) / "artifacts" / job["id"] / run["id"]
        for e in entries:
            if e.get("type") != "file":
                continue
            size = e.get("size") or 0
            if size > config.ARTIFACT_MAX_FILE_MB * 1024 * 1024:
                await _log(run, f"artifact {e.get('name')} skipped (over per-file cap)")
                continue
            if total + size > cap:
                await _log(run, "artifact total cap reached; remaining files skipped")
                break
            try:
                data = await client.download_file(f"{config.RUNTIME_ARTIFACT_DIR}/{e.get('name')}")
                dest_root.mkdir(parents=True, exist_ok=True)
                (dest_root / e.get("name")).write_bytes(data)
                await db.add_artifact(
                    job["id"], run["id"], e.get("name"), len(data),
                    str(dest_root / e.get("name")),
                )
                total += len(data)
                n += 1
            except Exception as ex:  # noqa: BLE001
                await _log(run, f"artifact pull {e.get('name')} failed: {ex}")
        return n

    @property
    def _manager(self):
        from .runtime_manager import manager
        return manager


worker = Worker()
