"""Jupyter kernel client over the Colab runtime proxy.

Standard Jupyter API blessed by the Runtime schema:
- POST {url}/api/sessions
- WS /api/kernels/{kernel_id}/channels (kernel message protocol)
- Contents API for file upload/download (base64, <50MB/request)

Auth (verified live against Colab runtimes):
- HTTP: X-Colab-Runtime-Proxy-Token header.
- WS: query params session_id + token + colab-runtime-proxy-token AND the
  X-Colab-Runtime-Proxy-Token header. The kernel channel socket MUST be kept
  alive with client-side pings (the Colab proxy sends none and drops idle
  sockets).

WS stack: `websocket-client` (WebSocketApp) in a worker thread — the same
library colab-cli uses via jupyter_kernel_client. The asyncio `websockets`
library gets its sockets silently reset by the Colab proxy (verified
2026-11-18: identical URL/headers — websocket-client round-trips, websockets
gets no frames then a frameless close).
"""
import asyncio
import base64
import json
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Awaitable, Callable
from urllib.parse import quote, urlencode

import httpx
import websocket as ws_client

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
        # The kernel can take a while to reach idle on a cold runtime; report
        # but do not block here (the WS buffers shell messages anyway).
        return data

    async def wait_kernel_idle(self, timeout_s: float = 180.0) -> str:
        """Poll GET /api/kernels/{id} until execution_state != 'starting'."""
        deadline = time.monotonic() + timeout_s
        state = "unknown"
        while time.monotonic() < deadline:
            try:
                r = await self._http.get(f"/api/kernels/{self.kernel_id}")
                if r.status_code == 200:
                    state = (r.json() or {}).get("execution_state") or state
                    if state != "starting":
                        return state
            except Exception:  # noqa: BLE001 — transient proxy errors
                pass
            await asyncio.sleep(3)
        return state

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
        r = await self._http.put(
            f"/api/contents/{quote(path.lstrip('/'))}", json=payload
        )
        if r.status_code >= 400:
            raise JupyterError(f"PUT {path} failed: HTTP {r.status_code} {r.text[:300]}")

    async def download_file(self, path: str) -> bytes:
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
        client_session = self.session_id or uuid.uuid4().hex
        ws_url = (
            f"{self.handle.ws_base}/api/kernels/{self.kernel_id}/channels?"
            + urlencode(
                {
                    "session_id": client_session,
                    "token": self.handle.token,
                    "colab-runtime-proxy-token": self.handle.token,
                }
            )
        )
        loop = asyncio.get_running_loop()
        out_q: asyncio.Queue = asyncio.Queue()
        result: dict = {"reply": None, "error": None, "lost": None, "chunks": []}

        def _emit_threadsafe(line: str):
            if not line:
                return
            loop.call_soon_threadsafe(out_q.put_nowait, line)

        def _thread_main():
            frames: queue.Queue = queue.Queue()
            opened = threading.Event()

            def on_open(ws):
                opened.set()

            def on_message(ws, frame):
                frames.put(("msg", frame))

            def on_error(ws, err):
                frames.put(("err", str(err)))

            def on_close(ws, code, msg):
                frames.put(("close", f"{code} {msg}"))

            app = ws_client.WebSocketApp(
                ws_url,
                header=_headers(self.handle),
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            threading.Thread(
                target=lambda: app.run_forever(ping_interval=20, ping_timeout=10),
                daemon=True,
            ).start()
            try:
                if not opened.wait(30):
                    result["lost"] = "kernel channel WS did not open within 30s"
                    return
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
                app.send(json.dumps(req))
                deadline = time.monotonic() + timeout_s
                while True:
                    if time.monotonic() > deadline:
                        result["error"] = ExecutionTimeout(
                            f"execution timed out after {timeout_s:.0f}s (timeout_min cap)"
                        )
                        return
                    try:
                        kind, frame = frames.get(timeout=2.0)
                    except queue.Empty:
                        continue
                    if kind != "msg":
                        if result["reply"] is not None:
                            return
                        result["lost"] = f"kernel channel closed: {frame}"
                        return
                    msg = _deserialize(frame)
                    if msg is None:
                        continue
                    if msg.get("msg_type") == "colab_request":
                        continue  # Colab-internal auth handshake; ignore
                    parent = (msg.get("parent_header") or {}).get("msg_id")
                    if parent and parent != msg_id:
                        continue
                    mt = msg.get("msg_type")
                    content = msg.get("content") or {}
                    if mt == "stream":
                        line = f"[{content.get('name', 'stdout')}] {content.get('text', '')}"
                        result["chunks"].append(line)
                        _emit_threadsafe(line)
                    elif mt in ("execute_result", "display_data"):
                        data = content.get("data") or {}
                        plain = data.get("text/plain")
                        line = (
                            f"[result] {plain}"
                            if plain
                            else "[rich output: " + ", ".join(k for k in data.keys() if k != "text/plain") + "]"
                        )
                        result["chunks"].append(line)
                        _emit_threadsafe(line)
                    elif mt == "error":
                        ename = content.get("ename", "Error")
                        evalue = content.get("evalue", "")
                        line = f"[error] {ename}: {evalue}"
                        result["chunks"].append(line)
                        _emit_threadsafe(line)
                        result["error"] = ExecutionError(
                            ename, evalue, content.get("traceback") or []
                        )
                        return
                    elif mt == "execute_reply":
                        status = content.get("status")
                        if status == "error" and result["error"] is None:
                            result["error"] = ExecutionError(
                                content.get("ename") or "Error",
                                content.get("evalue") or "",
                                content.get("traceback") or [],
                            )
                        result["reply"] = content
                        return
            finally:
                try:
                    app.close()
                except Exception:  # noqa: BLE001
                    pass

        thread = threading.Thread(target=_thread_main, daemon=True)
        thread.start()
        # Drain output while the worker runs; stream to subscribers.
        while True:
            drained = False
            while True:
                try:
                    line = out_q.get_nowait()
                    drained = True
                    if on_output:
                        await on_output(line)
                except asyncio.QueueEmpty:
                    break
            if not thread.is_alive() and not drained and out_q.empty():
                break
            await asyncio.wait_for(asyncio.sleep(0.2), timeout=1.0)
        if result["error"]:
            raise result["error"]
        if result["lost"]:
            raise ConnectionLost(result["lost"])
        reply = result["reply"]
        if reply is None:
            raise JupyterError("kernel channel closed before execute_reply")
        return "\n".join(result["chunks"])


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
