"""Runtime lifecycle manager: create/refresh/delete Colab runtimes.

Token refresh = GetRuntime at expiry-5min (colab-vscode pattern). A 404 on
refresh means the runtime was reclaimed (Colab deletes idle runtimes by
design) — the caller reacts by marking the run RECLAIMED. GetRuntime is a
metadata read only: it never executes code, never pings the Jupyter server,
and is NOT a keep-alive (idle runtimes get deleted regardless — Colab's own
VS Code client refreshes tokens the same way).
"""
import asyncio
import logging
from datetime import datetime, timezone

from . import config
from .colab_api import ColabClient, ConnectionHandle, RuntimeNotFound, spec_key

log = logging.getLogger("runtime_manager")

REFRESH_BUFFER_S = 5 * 60  # refresh at expiry-5min, per colab-vscode
RETRY_BUFFER_S = 30


class RuntimeManager:
    def __init__(self):
        self.client = ColabClient()
        # runtime_id -> handle (live connections this process knows about)
        self.handles: dict[str, ConnectionHandle] = {}
        self._last_used: dict[str, float] = {}
        # runtime_id -> spec (for reuse matching)
        self._specs: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    def touch(self, runtime_id: str) -> None:
        self._last_used[runtime_id] = asyncio.get_event_loop().time()

    def spec_of(self, accelerator: str, shape: str) -> dict:
        return spec_key(accelerator, shape)

    async def find_reusable(self, accelerator: str, shape: str) -> ConnectionHandle | None:
        """Find a live cached handle with matching spec (RUNTIME_REUSE mode)."""
        want = self.spec_of(accelerator, shape)
        async with self._lock:
            for rt_id, h in list(self.handles.items()):
                if h.expires_in_s <= REFRESH_BUFFER_S:
                    continue
                spec = self._specs.get(rt_id)
                if spec == want:
                    self.touch(rt_id)
                    return h
        return None

    async def acquire(self, accelerator: str, shape: str,
                      log_fn=None) -> ConnectionHandle:
        """Return a usable connection handle (create or reuse)."""
        want = self.spec_of(accelerator, shape)
        if config.RUNTIME_REUSE:
            h = await self.find_reusable(accelerator, shape)
            if h:
                if log_fn:
                    await log_fn(f"reusing live runtime {h.runtime_id}")
                return h
        if log_fn:
            await log_fn(
                f"creating runtime (accelerator={accelerator}, shape={shape})..."
            )
        handle = await self.client.create_runtime(accelerator, shape)
        async with self._lock:
            self.handles[handle.runtime_id] = handle
            self._specs[handle.runtime_id] = want
            self.touch(handle.runtime_id)
        if log_fn:
            await log_fn(
                f"runtime ready: {handle.runtime_id} (token expires in "
                f"{handle.expires_in_s:.0f}s)"
            )
        return handle

    async def refresh(self, runtime_id: str) -> ConnectionHandle:
        """GetRuntime — fresh token. 404 raises RuntimeNotFound (reclaimed)."""
        h = await self.client.get_runtime(runtime_id)
        async with self._lock:
            self.handles[runtime_id] = h
            self.touch(runtime_id)
        return h

    async def ensure_fresh(self, handle: ConnectionHandle) -> ConnectionHandle:
        if handle.expires_in_s > REFRESH_BUFFER_S:
            return handle
        try:
            return await self.refresh(handle.runtime_id)
        except RuntimeNotFound:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("token refresh failed for %s: %s", handle.runtime_id, e)
            return handle

    async def release(self, runtime_id: str, delete: bool = True) -> None:
        async with self._lock:
            self.handles.pop(runtime_id, None)
            self._specs.pop(runtime_id, None)
            self._last_used.pop(runtime_id, None)
        if delete:
            try:
                await self.client.delete_runtime(runtime_id)
                log.info("deleted runtime %s", runtime_id)
            except Exception as e:  # noqa: BLE001
                log.warning("delete runtime %s failed: %s", runtime_id, e)

    async def refresher_loop(self, on_reclaimed):
        """Background task: refresh tokens of in-use runtimes at expiry-5min.

        on_reclaimed(runtime_id) is awaited when a 404 proves the runtime is
        gone (Colab deleted it).
        """
        while True:
            await asyncio.sleep(30)
            # RUNTIME_REUSE mode: reap idle reused runtimes after the TTL so
            # nothing lingers needlessly (Colab would reclaim them anyway).
            if config.RUNTIME_REUSE and config.RUNTIME_REUSE_TTL_S > 0:
                now_t = asyncio.get_event_loop().time()
                async with self._lock:
                    idle = [
                        rt_id
                        for rt_id, t in self._last_used.items()
                        if now_t - t > config.RUNTIME_REUSE_TTL_S
                    ]
                for rt_id in idle:
                    log.info("reaping idle reused runtime %s", rt_id)
                    await self.release(rt_id, delete=True)
            async with self._lock:
                items = list(self.handles.items())
            for rt_id, h in items:
                if h.expires_in_s > REFRESH_BUFFER_S:
                    continue
                try:
                    await self.refresh(rt_id)
                    log.info("token refreshed for %s", rt_id)
                except RuntimeNotFound:
                    log.warning("runtime %s reclaimed by Colab (404)", rt_id)
                    try:
                        await on_reclaimed(rt_id)
                    except Exception as e:  # noqa: BLE001
                        log.error("reclaim handler failed: %s", e)
                except Exception as e:  # noqa: BLE001
                    log.warning("refresh error for %s: %s", rt_id, e)

    def runtime_status(self) -> list[dict]:
        out = []
        for rt_id, h in self.handles.items():
            out.append(
                {
                    "runtime_id": rt_id,
                    "expires_in_s": int(h.expires_in_s),
                    "spec": self._specs.get(rt_id),
                }
            )
        return out


manager = RuntimeManager()
