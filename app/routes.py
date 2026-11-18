"""HTTP routes: dashboard pages + job API + WS stream + connect flow."""
import asyncio
import json
import logging
import os
from pathlib import Path

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

from . import config, gauth
from .colab_api import ColabError, friendly_create_error
from .db import db
from .scheduler import is_valid_schedule
from .worker import subscribe, unsubscribe, worker

log = logging.getLogger("routes")
router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ---------- helpers ----------

def password_ok(request: Request, password: str | None) -> bool:
    if not config.APP_ACCESS_PASSWORD:
        return True
    candidates = [password, request.headers.get("x-platform-password")]
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        candidates.append(auth[7:])
    return any(c == config.APP_ACCESS_PASSWORD for c in candidates if c)


def check_password(request: Request, password: str | None):
    if not password_ok(request, password):
        raise HTTPException(
            status_code=401,
            detail={"error": "unauthorized", "hint": "set X-Platform-Password header"},
        )


def google_guard() -> None:
    """503-shaped error for Google-dependent operations when keyless."""
    if gauth.connected():
        return
    raise HTTPException(
        status_code=503,
        detail={
            "error": "google_credential_missing",
            "hint": "open /connect in the dashboard and complete the one-time consent",
        },
    )


def _check_keyless_env() -> str | None:
    """If GOOGLE_CREDENTIALS_JSON is set but invalid, surface a hint."""
    if config.GOOGLE_CREDENTIALS_JSON:
        try:
            json.loads(config.GOOGLE_CREDENTIALS_JSON)
            return None
        except ValueError:
            return "GOOGLE_CREDENTIALS_JSON is not valid JSON — fix the variable"
    return None


async def _status_context() -> dict:
    connected = gauth.connected()
    identity = await gauth.fetch_identity() if connected else None
    jobs = await db.list_jobs()
    runs = []
    for j in jobs[:20]:
        r = await db.latest_run(j["id"])
        if r:
            runs.append((j, r))
    return {
        "connected": connected,
        "identity": identity,
        "credential_hint": _check_keyless_env(),
        "jobs": jobs,
        "recent": runs,
        "runtime_reuse": config.RUNTIME_REUSE,
        "max_concurrent": config.MAX_CONCURRENT_JOBS,
        "notify_configured": bool(config.NOTIFY_WEBHOOK_URL),
        "password_protected": bool(config.APP_ACCESS_PASSWORD),
    }


# ---------- health (no Google call) ----------

@router.get("/health")
async def health():
    rss_mb = 0.0
    try:  # Unix
        import resource
        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:  # noqa: BLE001 — Windows / other
        try:
            import ctypes
            import ctypes.wintypes as wt

            class PMC(ctypes.Structure):
                _fields_ = [("cb", wt.DWORD), ("PageFaultCount", wt.DWORD),
                            ("PeakWorkingSetSize", ctypes.c_size_t),
                            ("WorkingSetSize", ctypes.c_size_t),
                            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                            ("PagefileUsage", ctypes.c_size_t),
                            ("PeakPagefileUsage", ctypes.c_size_t)]

            pmc = PMC()
            pmc.cb = ctypes.sizeof(PMC)
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                ctypes.windll.kernel32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb
            ):
                rss_mb = pmc.WorkingSetSize / (1024.0 * 1024.0)
        except Exception:  # noqa: BLE001
            try:
                proc = Path("/proc/self/status")
                if proc.exists():
                    for ln in proc.read_text().splitlines():
                        if ln.startswith("VmRSS:"):
                            rss_mb = float(ln.split()[1]) / 1024.0
            except Exception:  # noqa: BLE001
                rss_mb = 0.0
    active = await db.active_runs()
    return {
        "ok": True,
        "connected": gauth.connected(),
        "rss_mb": rss_mb,
        "active_runs": len(active),
        "runtime_reuse": config.RUNTIME_REUSE,
        "password_protected": bool(config.APP_ACCESS_PASSWORD),
    }


@router.get("/api/status")
async def platform_status():
    from .runtime_manager import manager
    return {
        "connected": gauth.connected(),
        "runtimes": manager.runtime_status(),
        "worker_in_flight": len(worker.in_flight),
        "notify_configured": bool(config.NOTIFY_WEBHOOK_URL),
        "credential_hint": _check_keyless_env(),
    }


# ---------- dashboard pages ----------

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    ctx = await _status_context()
    return templates.TemplateResponse(
        request=request, name="index.html", context={"ctx": ctx}
    )


@router.get("/connect", response_class=HTMLResponse)
async def connect_page(request: Request):
    from .config import consent_url
    ctx = await _status_context()
    ctx["consent_url"] = consent_url()
    return templates.TemplateResponse(
        request=request, name="connect.html", context={"ctx": ctx}
    )


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_page(request: Request, job_id: str):
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    runs = await db.list_runs(job_id)
    artifacts = await db.list_artifacts(job_id)
    ck = await db.get_checkpoint_manifest(job_id)
    ctx = await _status_context()
    ctx.update({"job": job, "runs": runs, "artifacts": artifacts, "checkpoint": ck})
    return templates.TemplateResponse(
        request=request, name="job.html", context={"ctx": ctx}
    )


@router.get("/artifacts", response_class=HTMLResponse)
async def artifacts_page(request: Request):
    artifacts = await db.list_artifacts()
    ctx = await _status_context()
    ctx["artifacts"] = artifacts
    return templates.TemplateResponse(
        request=request, name="artifacts.html", context={"ctx": ctx}
    )


# ---------- connect flow ----------

@router.post("/api/connect")
async def api_connect(request: Request, body: dict):
    check_password(request, body.get("password"))
    try:
        if body.get("code"):
            raw = gauth.exchange_code(body["code"].strip())
        elif body.get("credential_json"):
            raw = json.loads(body["credential_json"])
        elif body.get("json"):
            raw = json.loads(body["json"])
        else:
            raise HTTPException(400, "provide 'code' (authorization code) or 'credential_json'")
        summary = gauth.store_credential(raw)
    except gauth.CredentialError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": f"invalid JSON: {e}"})
    # prove Colab access with a real call
    specs_error = None
    try:
        from .colab_api import ColabClient
        client = ColabClient()
        specs = await client.list_runtime_specs()
        await client.close()
        summary["eligible_specs"] = [
            s["key"] for s in specs if s.get("eligible")
        ]
    except Exception as e:  # noqa: BLE001
        specs_error = friendly_create_error(e)
    return {"ok": True, "credential": summary, "colab_check": specs_error or "passed"}


@router.post("/api/disconnect")
async def api_disconnect(request: Request, body: dict = None):
    body = body or {}
    check_password(request, body.get("password"))
    gauth.disconnect()
    return {"ok": True, "connected": False}


# ---------- Google-backed reads ----------

@router.get("/api/subscription")
async def api_subscription():
    google_guard()
    from .colab_api import ColabClient
    client = ColabClient()
    try:
        sub = await client.get_subscription()
        return {"tier": sub.get("tier"), "raw": sub}
    except ColabError as e:
        return JSONResponse(status_code=502, content={"error": str(e), "status": e.status})
    finally:
        await client.close()


@router.get("/api/specs")
async def api_specs():
    google_guard()
    from .colab_api import ColabClient
    client = ColabClient()
    try:
        specs = await client.list_runtime_specs()
        return {
            "specs": [
                {"key": s.get("key"), "eligible": bool(s.get("eligible"))}
                for s in specs
            ]
        }
    except ColabError as e:
        return JSONResponse(status_code=502, content={"error": str(e), "status": e.status})
    finally:
        await client.close()


# ---------- jobs ----------

ALLOWED_ACCELERATORS = {"NONE", "T4", "L4", "A100", "H100"}
ALLOWED_SHAPES = {"STANDARD", "HIGHMEM"}


def _notebook_to_code(nb_json: str) -> str:
    """Extract code cells from an .ipynb JSON string into one script."""
    try:
        nb = json.loads(nb_json)
    except ValueError as e:
        raise HTTPException(400, f"notebook is not valid JSON: {e}")
    cells = nb.get("cells") or []
    parts = []
    for i, c in enumerate(cells):
        if c.get("cell_type") != "code":
            continue
        src = "".join(c.get("source") or [])
        parts.append(f"# %% [cell {i}]\n{src}")
    if not parts:
        raise HTTPException(400, "notebook has no code cells")
    return "\n\n".join(parts) + "\n"


@router.post("/api/jobs")
async def create_job(request: Request, body: dict):
    check_password(request, body.get("password"))
    google_guard()
    name = (body.get("name") or "job").strip()[:100] or "job"
    code = body.get("code") or ""
    notebook = bool(body.get("notebook")) or bool(body.get("notebook_json"))
    if body.get("notebook_json"):
        code = _notebook_to_code(body["notebook_json"])
    if not code.strip():
        raise HTTPException(400, "code (or notebook_json) is required")
    accelerator = (body.get("accelerator") or "NONE").upper()
    shape = (body.get("shape") or "STANDARD").upper()
    if accelerator not in ALLOWED_ACCELERATORS:
        raise HTTPException(400, f"accelerator must be one of {sorted(ALLOWED_ACCELERATORS)}")
    if shape not in ALLOWED_SHAPES:
        raise HTTPException(400, f"shape must be one of {sorted(ALLOWED_SHAPES)}")
    timeout_min = int(body.get("timeout_min") or config.DEFAULT_TIMEOUT_MIN)
    timeout_min = max(1, min(timeout_min, config.MAX_TIMEOUT_MIN))
    schedule = (body.get("schedule") or "").strip() or None
    if schedule and not is_valid_schedule(schedule):
        raise HTTPException(400, "schedule is not a valid cron expression")
    webhook_url = (body.get("webhook_url") or "").strip() or None
    checkpoint_command = (body.get("checkpoint_command") or "").strip() or None

    # per-tier eligibility check (ListRuntimeSpecs)
    from .colab_api import ColabClient, spec_key
    client = ColabClient()
    try:
        specs = await client.list_runtime_specs()
    except ColabError as e:
        return JSONResponse(status_code=502, content={"error": str(e), "status": e.status})
    finally:
        await client.close()
    want = spec_key(accelerator, shape)
    eligible_keys = []
    wanted = None
    for s in specs:
        k = s.get("key") or {}
        if k == want:
            wanted = s
        if s.get("eligible"):
            eligible_keys.append(k)
    if wanted is None:
        return JSONResponse(
            status_code=400,
            content={
                "error": "unknown runtime spec",
                "requested": want,
                "available": [s.get("key") for s in specs],
            },
        )
    if not wanted.get("eligible", True):
        return JSONResponse(
            status_code=400,
            content={
                "error": "runtime spec not eligible for your Colab tier",
                "requested": want,
                "eligible": eligible_keys,
            },
        )

    job = await db.create_job(
        name=name,
        code=code,
        accelerator=accelerator,
        shape=shape,
        timeout_min=timeout_min,
        schedule=schedule,
        notebook=notebook,
        webhook_url=webhook_url,
        checkpoint_command=checkpoint_command,
    )
    run = None
    if not schedule:  # immediate run for unscheduled jobs
        run = await worker.enqueue(job["id"], attempt=1, trigger="manual")
    return JSONResponse(status_code=201, content={"job": job, "run": run})


@router.get("/api/jobs")
async def list_jobs():
    jobs = await db.list_jobs()
    out = []
    for j in jobs:
        r = await db.latest_run(j["id"])
        out.append({**j, "latest_run": r})
    return {"jobs": out}


@router.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    runs = await db.list_runs(job_id)
    return {"job": job, "runs": runs}


@router.delete("/api/jobs/{job_id}")
async def delete_job(request: Request, job_id: str, password: str | None = None):
    check_password(request, password)
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    await db.delete_job(job_id)
    return {"ok": True}


@router.post("/api/jobs/{job_id}/run")
async def rerun_job(request: Request, job_id: str, body: dict = None):
    body = body or {}
    check_password(request, body.get("password") or Query(None))
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    google_guard()
    run = await worker.enqueue(job_id, attempt=job["run_count"] + 1, trigger="manual")
    return JSONResponse(status_code=202, content={"run": run})


@router.post("/api/jobs/{job_id}/checkpoint")
async def job_checkpoint(request: Request, job_id: str, body: dict = None):
    """Called by job code (platform_checkpoint()) to snapshot CHECKPOINT_PATH."""
    body = body or {}
    check_password(request, request.headers.get("x-platform-password"))
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    latest = await db.latest_run(job_id)
    if not latest or latest["status"] not in ("PROVISIONING", "RUNNING"):
        return JSONResponse(status_code=409, content={"error": "no active run for this job"})
    from .runtime_manager import manager
    handle = manager.handles.get(latest.get("runtime_id") or "")
    if not handle:
        return JSONResponse(status_code=409, content={"error": "runtime not available"})
    from .jupyter_exec import JupyterClient
    client = JupyterClient(handle)
    try:
        n = await worker._snapshot_checkpoint(job, latest, client, final=False)
    finally:
        await client.close()
    return {"ok": True, "files": n}


@router.get("/api/runs/{run_id}")
async def get_run(run_id: str):
    run = await db.get_run(run_id)
    if not run:
        raise HTTPException(404, "run not found")
    return {"run": run}


@router.get("/api/jobs/{job_id}/log")
async def run_log_text(job_id: str, run_id: str | None = None):
    if run_id:
        run = await db.get_run(run_id)
    else:
        run = await db.latest_run(job_id)
    if not run or run["job_id"] != job_id:
        raise HTTPException(404, "run not found")
    return PlainTextResponse(run.get("log") or "")


# ---------- live stream ----------

@router.websocket("/api/jobs/{job_id}/stream")
async def job_stream(ws: WebSocket, job_id: str, run_id: str | None = None):
    await ws.accept()
    if run_id:
        run = await db.get_run(run_id)
    else:
        run = await db.latest_run(job_id)
    if not run or run["job_id"] != job_id:
        await ws.send_text("ERROR run not found")
        await ws.close()
        return
    backlog = run.get("log") or ""
    if backlog:
        await ws.send_text(backlog)
    q = await subscribe(run["id"])
    terminal = run["status"] in ("COMPLETE", "FAILED", "RECLAIMED", "CANCELLED")
    try:
        while True:
            try:
                line = await asyncio.wait_for(q.get(), timeout=2.0)
                await ws.send_text(line)
            except asyncio.TimeoutError:
                cur = await db.get_run(run["id"])
                if cur and cur["status"] in ("COMPLETE", "FAILED", "RECLAIMED", "CANCELLED"):
                    final_log = cur.get("log") or ""
                    if final_log != backlog:
                        tail = final_log[len(backlog):] if final_log.startswith(backlog) else ""
                        for ln in tail.splitlines():
                            await ws.send_text(ln)
                    await ws.send_text(f"--- run {cur['status']} ---")
                    await ws.close()
                    return
    except WebSocketDisconnect:
        pass
    finally:
        unsubscribe(run["id"], q)


# ---------- artifacts ----------

@router.get("/api/artifacts")
async def api_artifacts(job_id: str | None = None):
    return {"artifacts": await db.list_artifacts(job_id)}


@router.get("/api/artifacts/{artifact_id}/download")
async def artifact_download(artifact_id: str):
    a = await db.get_artifact(artifact_id)
    if not a or not Path(a["stored_path"]).exists():
        raise HTTPException(404, "artifact not found")
    return FileResponse(a["stored_path"], filename=a["path"])
