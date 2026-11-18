"""Jupyter kernel client over the Colab runtime proxy.

Standard Jupyter API blessed by the Runtime schema:
- POST {url}/api/sessions
- WS /api/kernels/{kernel_id}/channels (kernel message protocol)
- Contents API for file upload/download (base64, <50MB/request)
Auth: X-Colab-Runtime-Proxy-Token header (+ query param fallback for WS).
"""
import base64
import json
import uuid
from datetime import datetime, timezone
from typing import Awaitable, Callable

import httpx
from websockets.asyncio.client import connect as ws_connect

from .colab_api import ConnectionHandle

CLIENT_HEADERS = {
    "X-Colab-Runtime-Proxy-Token": "",  # filled per handle
    "X-Colab-Client-Agent": "colab-gpu-platform/1.0",
}


class JupyterError(Exception):
    pass


class ExecutionTimeout(JupyterError):
    pass


class ConnectionLost(JupyterError):
    pass


class ExecutionError(Exception):
    def __init__(self, ename: str, evalue: str, traceback: list[str]):
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback
        super().__init__(f"{ename}: {evalue}")


def _headers(handle: ConnectionHandle) -> dict:
    h = dict(CLIENT_HEADERS)
    h["X-Colab-Runtime-Proxy-Token"] = handle.token
    return h


class JupyterClient:
    def __init__(self, handle: ConnectionHandle):
        self.handle = handle
        self.base = handle.base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=self.base,
            headers=_headers(handle),
            timeout=httpx.Timeout(60.0, connect=15.0),
        )
        self.session_id: str | None = None
        self.kernel_id: str | None = None

    async def close(self):
        await self._http.aclose()

    # ---- sessions ----
    async def create_session(self, name: str) -> dict:
        body = {
            "name": name,
            "path": f"{name}.ipynb",
            "type": "notebook",
            "kernel": {"name": "python3"},
        }
        r = await self._http.post("/api/sessions", json=body)
        if r.status_code >= 400:
            raise JupyterError(f"POST /api/sessions failed: HTTP {r.status_code} {r.text[:300]}")
        data = r.json()
        self.session_id = data.get("id")
        self.kernel_id = (data.get("kernel") or {}).get("id")
        if not self.kernel_id:
            raise JupyterError("session created but no kernel id returned")
        return data

    async def delete_session(self, session_id: str | None = None) -> None:
        sid = session_id or self.session_id
        if not sid:
            return
        try:
            await self._http.delete(f"/api/sessions/{sid}")
        except Exception:  # noqa: BLE001
            pass

    # ---- contents ----
    async def upload_file(self, path: str, data: bytes) -> None:
        if len(data) > 45 * 1024 * 1024:
            raise JupyterError(f"file {path} exceeds the 45MB single-request cap")
        payload = {
            "type": "file",
            "format": "base64",
            "content": base64.b64encode(data).decode("ascii"),
        }
        from urllib.parse import quote
        r = await self._http.put(
            f"/api/contents/{quote(path.lstrip('/'))}", json=payload
        )
        if r.status_code >= 400:
            raise JupyterError(f"PUT {path} failed: HTTP {r.status_code} {r.text[:300]}")

    async def download_file(self, path: str) -> bytes:
        from urllib.parse import quote
        r = await self._http.get(
            f"/api/contents/{quote(path.lstrip('/'))}", params={"content": "1"}
        )
        if r.status_code == 404:
            raise FileNotFoundError(path)
        if r.status_code >= 400:
            raise JupyterError(f"GET {path} failed: HTTP {r.status_code} {r.text[:300]}")
        data = r.json()
        if data.get("format") == "base64":
            return base64.b64decode(data.get("content") or "")
        content = data.get("content")
        return content.encode("utf-8") if isinstance(content, str) else b""

    async def list_dir(self, path: str) -> list[dict]:
        from urllib.parse import quote
        r = await self._http.get(
            f"/api/contents/{quote(path.lstrip('/'))}", params={"content": "1"}
        )
        if r.status_code == 404:
            return []
        if r.status_code >= 400:
            raise JupyterError(f"list {path} failed: HTTP {r.status_code}")
        data = r.json()
        if data.get("type") == "directory":
            return data.get("content") or []
        return []

    # ---- execution (kernel message protocol over WS channels) ----
    async def execute(
        self,
        code: str,
        timeout_s: float,
        on_output: Callable[[str], Awaitable[None]] | None = None,
    ) -> str:
        """Execute code, stream outputs, return aggregate text. Raises ExecutionError."""
        if not self.kernel_id:
            raise JupyterError("no active kernel session")
        ws_url = (
            f"{self.handle.ws_base}/api/kernels/{self.kernel_id}/channels"
            f"?colab-runtime-proxy-token={self.handle.token}"
        )
        client_session = self.session_id or uuid.uuid4().hex
        msg_id = uuid.uuid4().hex
        req = {
            "header": {
                "msg_id": msg_id,
                "username": "colab-gpu-platform",
                "session": client_session,
                "msg_type": "execute_request",
                "version": "5.3",
                "date": datetime.now(timezone.utc).isoformat(),
            },
            "parent_header": {},
            "metadata": {},
            "content": {
                "code": code,
                "silent": False,
                "store_history": True,
                "user_expressions": {},
                "allow_stdin": False,
                "stop_on_error": True,
            },
            "buffers": [],
        }
        chunks: list[str] = []
        error: ExecutionError | None = None
        saw_reply = False
        import websockets.exceptions as _ws_exc

        async with ws_connect(
            ws_url,
            additional_headers=_headers(self.handle),
            max_size=64 * 1024 * 1024,
            open_timeout=30,
            ping_interval=25,
        ) as ws:
            await ws.send(json.dumps(req))
            deadline = asyncio_loop_time() + timeout_s
            while True:
                remaining = deadline - asyncio_loop_time()
                if remaining <= 0:
                    raise ExecutionTimeout(
                        f"execution timed out after {timeout_s:.0f}s (timeout_min cap)"
                    )
                try:
                    frame = await asyncio_wait_for(ws.recv(), timeout=min(remaining, 30))
                except TimeoutError:
                    continue
                except _ws_exc.ConnectionClosed as e:
                    raise ConnectionLost(f"kernel channel closed mid-run: {e}") from e
                msg = _deserialize(frame)
                if msg is None:
                    continue
                if msg.get("msg_type") == "colab_request":
                    continue  # Colab internal handshake; ignore
                parent = (msg.get("parent_header") or {}).get("msg_id")
                if parent and parent != msg_id:
                    continue
                mt = msg.get("msg_type")
                content = msg.get("content") or {}
                if mt == "stream":
                    text = f"[{content.get('name', 'stdout')}] {content.get('text', '')}"
                    chunks.append(text)
                    if on_output:
                        await on_output(text)
                elif mt in ("execute_result", "display_data"):
                    data = content.get("data") or {}
                    plain = data.get("text/plain")
                    if plain:
                        text = f"[result] {plain}"
                        chunks.append(text)
                        if on_output:
                            await on_output(text)
                    else:
                        mimes = [k for k in data.keys() if k != "text/plain"]
                        if mimes:
                            text = f"[rich output: {', '.join(mimes)}]"
                            chunks.append(text)
                            if on_output:
                                await on_output(text)
                elif mt == "error":
                    ename = content.get("ename", "Error")
                    evalue = content.get("evalue", "")
                    tb = content.get("traceback") or []
                    error = ExecutionError(ename, evalue, tb)
                    text = f"[error] {ename}: {evalue}"
                    chunks.append(text)
                    if on_output:
                        await on_output(text)
                elif mt == "execute_reply":
                    saw_reply = True
                    status = content.get("status")
                    if status == "error" and error is None:
                        error = ExecutionError(
                            (content.get("ename") or "Error"),
                            (content.get("evalue") or ""),
                            content.get("traceback") or [],
                        )
                    break
                elif mt == "status":
                    if content.get("execution_state") == "dead":
                        raise JupyterError("kernel died during execution")
        if error:
            raise error
        if not saw_reply:
            raise JupyterError("WS closed before execute_reply")
        return "\n".join(chunks)


def asyncio_loop_time() -> float:
    import asyncio
    return asyncio.get_event_loop().time()


async def asyncio_wait_for(aw, timeout: float):
    import asyncio
    return await asyncio.wait_for(aw, timeout=timeout)


def _deserialize(frame) -> dict | None:
    """Handle both Jupyter WS wire styles (single JSON object or part list)."""
    try:
        if isinstance(frame, (bytes, bytearray)):
            data = json.loads(frame.decode("utf-8", "replace"))
        else:
            data = json.loads(frame)
    except (ValueError, UnicodeDecodeError):
        return None
    if isinstance(data, dict):
        return {
            "msg_type": (data.get("header") or {}).get("msg_type") or data.get("msg_type"),
            "parent_header": data.get("parent_header") or {},
            "content": data.get("content") or {},
        }
    if isinstance(data, list) and len(data) >= 5:
        # wire format: [b'<MSG>', header, parent, metadata, content, buffers...]
        def _p(x):
            if isinstance(x, (bytes, bytearray)):
                return json.loads(x.decode("utf-8", "replace"))
            if isinstance(x, str):
                return json.loads(x)
            return x or {}
        try:
            header = _p(data[1])
            parent = _p(data[2])
            content = _p(data[4])
            return {
                "msg_type": header.get("msg_type"),
                "parent_header": parent,
                "content": content,
            }
        except (ValueError, IndexError):
            return None
    return None
