"""Colab API v1beta client (colaboratory.googleapis.com).

7 methods: runtimes create (LRO)/get/list/delete, runtimespecs.list,
subscription get, operations get/wait. No `state` field on runtimes —
readiness = LRO Operation `done` + connectionInfo.
"""
import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

from . import config, gauth


class ColabError(Exception):
    def __init__(self, status: int, message: str, body: str = ""):
        self.status = status
        self.body = body
        super().__init__(message)


class RuntimeNotFound(ColabError):
    pass


@dataclass
class ConnectionHandle:
    runtime_id: str
    base_url: str
    token: str
    expire_time: datetime | None
    endpoint: str | None

    @property
    def expires_in_s(self) -> float:
        if not self.expire_time:
            return 3600.0
        return (self.expire_time - datetime.now(timezone.utc)).total_seconds()

    @property
    def ws_base(self) -> str:
        return self.base_url.replace("https://", "wss://", 1)


def _parse_time(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def spec_key(accelerator: str, shape: str) -> dict:
    accelerator = (accelerator or "NONE").upper()
    shape = (shape or "STANDARD").upper()
    if accelerator == "NONE":
        variant = "VARIANT_CPU"
    else:
        variant = "VARIANT_GPU"
    return {"variant": variant, "accelerator": accelerator, "shape": f"SHAPE_{shape}"}


class ColabClient:
    def __init__(self):
        self._client = httpx.AsyncClient(
            base_url=config.COLAB_API_BASE,
            timeout=httpx.Timeout(60.0, connect=15.0),
        )

    async def _headers(self) -> dict:
        tok = await gauth.access_token()
        if not tok:
            raise ColabError(0, "not connected: no Google credential")
        headers = {
            "Authorization": f"Bearer {tok}",
            "Accept": "application/json",
            "X-Goog-Api-Client": config.CLIENT_AGENT,
        }
        quota = gauth.quota_project()
        if quota:
            # The Colab API requires a quota project for ADC user credentials.
            headers["x-goog-user-project"] = quota
        return headers

    @staticmethod
    def _handle_from_runtime(rt: dict) -> ConnectionHandle:
        c = rt.get("connectionInfo") or {}
        name = rt.get("name") or ""
        runtime_id = name.removeprefix("runtimes/")
        return ConnectionHandle(
            runtime_id=runtime_id,
            base_url=c.get("url", ""),
            token=c.get("token", ""),
            expire_time=_parse_time(c.get("expireTime")),
            endpoint=c.get("endpoint"),
        )

    async def create_runtime(self, accelerator: str, shape: str,
                             timeout_s: float = 420.0) -> ConnectionHandle:
        """CreateRuntime (LRO) + poll until live. Returns live connection.

        Readiness: the LRO's metadata carries the runtime name immediately; we
        poll GetRuntime on that id until it returns connectionInfo. (The
        operations.get/:wait endpoints 404 at this host — verified 2026-11-18
        — so GetRuntime polling is the only working readiness signal.)
        """
        request_id = str(uuid.uuid4())
        body = {"runtimeSpec": spec_key(accelerator, shape)}
        r = await self._client.post(
            "/v1beta/runtimes",
            params={"requestId": request_id},
            json=body,
            headers=await self._headers(),
        )
        if r.status_code >= 400:
            raise ColabError(r.status_code, f"CreateRuntime failed: HTTP {r.status_code}", r.text)
        op = r.json()
        if op.get("done"):
            if op.get("error"):
                err = op["error"] or {}
                reason, msg = _parse_error_details(err)
                raise ColabError(
                    err.get("code", 9),
                    f"CreateRuntime failed: {msg}" + (f" (reason: {reason})" if reason else ""),
                )
            resp = op.get("response") or {}
            rt = resp.get("runtime") if "runtime" in resp else resp
            if rt and rt.get("connectionInfo"):
                return self._handle_from_runtime(rt)
        runtime_name = (op.get("metadata") or {}).get("runtime") or ""
        if not runtime_name:
            raise ColabError(0, "CreateRuntime returned no metadata.runtime (cannot poll)")
        runtime_id = runtime_name.removeprefix("runtimes/")
        deadline = asyncio.get_event_loop().time() + timeout_s
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(3.0)
            try:
                handle = await self.get_runtime(runtime_id)
                return handle
            except RuntimeNotFound:
                continue  # not assigned yet
            except ColabError as e:
                if e.status == 404:
                    continue
                raise
        raise ColabError(0, f"CreateRuntime LRO timed out after {timeout_s:.0f}s")

    async def get_runtime(self, runtime_id: str) -> ConnectionHandle:
        """GetRuntime — also the token refresh path (call before expiry-5min)."""
        r = await self._client.get(
            f"/v1beta/runtimes/{runtime_id}", headers=await self._headers()
        )
        if r.status_code == 404:
            raise RuntimeNotFound(404, f"runtime {runtime_id} is gone (404)")
        if r.status_code >= 400:
            raise ColabError(r.status_code, f"GetRuntime failed: HTTP {r.status_code}", r.text)
        return self._handle_from_runtime(r.json())

    async def list_runtimes(self) -> list[dict]:
        r = await self._client.get("/v1beta/runtimes", headers=await self._headers())
        if r.status_code >= 400:
            raise ColabError(r.status_code, f"ListRuntimes failed: HTTP {r.status_code}", r.text)
        return (r.json() or {}).get("runtimes") or []

    async def delete_runtime(self, runtime_id: str) -> None:
        r = await self._client.delete(
            f"/v1beta/runtimes/{runtime_id}",
            params={"allowMissing": "true"},
            headers=await self._headers(),
        )
        if r.status_code >= 400 and r.status_code != 404:
            raise ColabError(r.status_code, f"DeleteRuntime failed: HTTP {r.status_code}", r.text)

    async def list_runtime_specs(self) -> list[dict]:
        r = await self._client.get("/v1beta/runtimespecs", headers=await self._headers())
        if r.status_code >= 400:
            raise ColabError(r.status_code, f"ListRuntimeSpecs failed: HTTP {r.status_code}", r.text)
        return (r.json() or {}).get("runtimeSpecs") or []

    async def get_subscription(self) -> dict:
        r = await self._client.get("/v1beta/subscription", headers=await self._headers())
        if r.status_code >= 400:
            raise ColabError(r.status_code, f"GetSubscription failed: HTTP {r.status_code}", r.text)
        return r.json() or {}

    async def close(self):
        await self._client.aclose()


def _parse_error_details(err: dict) -> tuple[str | None, str]:
    """Extract (reason, message) from a google.rpc Status with ErrorInfo details."""
    msg = err.get("message", "")
    reason = None
    for d in err.get("details") or []:
        if isinstance(d, dict) and d.get("reason"):
            reason = d["reason"]
            break
    return reason, msg


def friendly_create_error(e: Exception) -> str:
    """Human-facing message for common CreateRuntime failures."""
    if isinstance(e, ColabError):
        text = f"{e} | body={e.body[:400]}" if e.body else str(e)
        low = text.lower()
        if "quota project" in low or "service_disabled" in low:
            return (
                "Colab API needs a quota project: reconnect with your Google "
                "Cloud project ID (the platform enables the Colab API on it)."
            )
        if "has not been used in project" in low or "it is disabled" in low:
            return (
                "Colab API is not enabled on the quota project yet — enable "
                "propagation can take a minute; retry, or run `gcloud services "
                "enable colaboratory.googleapis.com --project <id>`."
            )
        if "quota" in low:
            return "Colab refused the runtime: insufficient quota / compute units exhausted."
        if "stockout" in low or "unavailable" in low or "capacity" in low:
            return "Colab refused the runtime: accelerator unavailable (stockout). Try NONE/T4."
        if "too many" in low or "assignment" in low:
            return "Colab refused the runtime: too many active assignments for this account."
        if "denylist" in low or "banned" in low:
            return "Colab refused the runtime: account is restricted by Colab."
        if "401" in str(e.status) or "403" in str(e.status):
            return (
                "Colab API returned 401/403 — the credential may lack the "
                "colaboratory scope or the account/API may not be enabled."
            )
        return text
    return str(e)
