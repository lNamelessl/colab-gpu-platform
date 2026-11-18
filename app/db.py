"""SQLite persistence (jobs, runs, artifacts, settings) on the /data volume."""
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from . import config

_SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  code TEXT NOT NULL DEFAULT '',
  accelerator TEXT NOT NULL DEFAULT 'NONE',
  shape TEXT NOT NULL DEFAULT 'STANDARD',
  timeout_min INTEGER NOT NULL DEFAULT 30,
  schedule TEXT,
  notebook INTEGER NOT NULL DEFAULT 0,
  webhook_url TEXT,
  checkpoint_command TEXT,
  created_at TEXT NOT NULL,
  run_count INTEGER NOT NULL DEFAULT 0,
  last_fire TEXT
);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'QUEUED',
  trigger TEXT NOT NULL DEFAULT 'manual',
  runtime_id TEXT,
  runtime_spec TEXT,
  queued_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  log TEXT NOT NULL DEFAULT '',
  error TEXT,
  restored_checkpoint INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_runs_job ON runs(job_id, queued_at);
CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  path TEXT NOT NULL,
  size INTEGER NOT NULL DEFAULT 0,
  stored_path TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints (
  job_id TEXT PRIMARY KEY,
  manifest TEXT NOT NULL,
  updated_at TEXT
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id() -> str:
    return uuid.uuid4().hex


class DB:
    def __init__(self, path: Path):
        self.path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def execute(self, sql: str, params=()) -> None:
        assert self._conn
        await self._conn.execute(sql, params)
        await self._conn.commit()

    async def query(self, sql: str, params=()) -> list[dict]:
        assert self._conn
        cur = await self._conn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def one(self, sql: str, params=()) -> dict | None:
        rows = await self.query(sql, params)
        return rows[0] if rows else None

    # ---- settings ----
    async def get_setting(self, key: str) -> str | None:
        row = await self.one("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row else None

    async def set_setting(self, key: str, value: str) -> None:
        await self.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    # ---- jobs ----
    async def create_job(self, name, code, accelerator="NONE", shape="STANDARD",
                         timeout_min=30, schedule=None, notebook=False,
                         webhook_url=None, checkpoint_command=None) -> dict:
        job = {
            "id": new_id(),
            "name": name,
            "code": code,
            "accelerator": accelerator,
            "shape": shape,
            "timeout_min": timeout_min,
            "schedule": schedule,
            "notebook": 1 if notebook else 0,
            "webhook_url": webhook_url,
            "checkpoint_command": checkpoint_command,
            "created_at": now(),
            "run_count": 0,
            "last_fire": None,
        }
        await self.execute(
            "INSERT INTO jobs(id,name,code,accelerator,shape,timeout_min,schedule,"
            "notebook,webhook_url,checkpoint_command,created_at,run_count,last_fire) "
            "VALUES(:id,:name,:code,:accelerator,:shape,:timeout_min,:schedule,"
            ":notebook,:webhook_url,:checkpoint_command,:created_at,:run_count,:last_fire)",
            job,
        )
        return job

    async def list_jobs(self) -> list[dict]:
        return await self.query("SELECT * FROM jobs ORDER BY created_at DESC")

    async def get_job(self, job_id: str) -> dict | None:
        return await self.one("SELECT * FROM jobs WHERE id=?", (job_id,))

    async def delete_job(self, job_id: str) -> None:
        await self.execute("DELETE FROM jobs WHERE id=?", (job_id,))
        await self.execute("DELETE FROM runs WHERE job_id=?", (job_id,))
        await self.execute("DELETE FROM artifacts WHERE job_id=?", (job_id,))
        await self.execute("DELETE FROM checkpoints WHERE job_id=?", (job_id,))

    # ---- runs ----
    async def create_run(self, job_id: str, attempt: int = 1, trigger: str = "manual") -> dict:
        run = {
            "id": new_id(),
            "job_id": job_id,
            "attempt": attempt,
            "status": "QUEUED",
            "trigger": trigger,
            "runtime_id": None,
            "runtime_spec": None,
            "queued_at": now(),
            "started_at": None,
            "finished_at": None,
            "log": "",
            "error": None,
            "restored_checkpoint": 0,
        }
        await self.execute(
            "INSERT INTO runs(id,job_id,attempt,status,trigger,runtime_id,runtime_spec,"
            "queued_at,started_at,finished_at,log,error,restored_checkpoint) "
            "VALUES(:id,:job_id,:attempt,:status,:trigger,:runtime_id,:runtime_spec,"
            ":queued_at,:started_at,:finished_at,:log,:error,:restored_checkpoint)",
            run,
        )
        await self.execute(
            "UPDATE jobs SET run_count=run_count+1 WHERE id=?", (job_id,)
        )
        return run

    async def update_run(self, run_id: str, **fields) -> None:
        if not fields:
            return
        cols = ",".join(f"{k}=?" for k in fields)
        await self.execute(f"UPDATE runs SET {cols} WHERE id=?", (*fields.values(), run_id))

    async def append_log(self, run_id: str, line: str, cap: int | None = None) -> None:
        cap = cap or config.LOG_CAP_BYTES
        line = (line or "").rstrip("\n")
        if not line:
            return
        await self.execute(
            "UPDATE runs SET log = substr(log || ?, max(length(log) - ?, 0)) WHERE id=?",
            (line + "\n", cap, run_id),
        )

    async def get_run(self, run_id: str) -> dict | None:
        return await self.one("SELECT * FROM runs WHERE id=?", (run_id,))

    async def latest_run(self, job_id: str) -> dict | None:
        return await self.one(
            "SELECT * FROM runs WHERE job_id=? ORDER BY queued_at DESC, attempt DESC LIMIT 1",
            (job_id,),
        )

    async def list_runs(self, job_id: str) -> list[dict]:
        return await self.query(
            "SELECT * FROM runs WHERE job_id=? ORDER BY queued_at DESC, attempt DESC LIMIT 100",
            (job_id,),
        )

    async def queued_runs(self) -> list[dict]:
        return await self.query("SELECT * FROM runs WHERE status='QUEUED' ORDER BY queued_at ASC")

    async def active_runs(self) -> list[dict]:
        return await self.query(
            "SELECT * FROM runs WHERE status IN ('PROVISIONING','RUNNING') ORDER BY queued_at ASC"
        )

    # ---- artifacts ----
    async def add_artifact(self, job_id, run_id, path, size, stored_path) -> dict:
        a = {
            "id": new_id(),
            "job_id": job_id,
            "run_id": run_id,
            "path": path,
            "size": size,
            "stored_path": stored_path,
            "created_at": now(),
        }
        await self.execute(
            "INSERT INTO artifacts(id,job_id,run_id,path,size,stored_path,created_at) "
            "VALUES(:id,:job_id,:run_id,:path,:size,:stored_path,:created_at)",
            a,
        )
        return a

    async def list_artifacts(self, job_id: str | None = None) -> list[dict]:
        if job_id:
            return await self.query(
                "SELECT * FROM artifacts WHERE job_id=? ORDER BY created_at DESC", (job_id,)
            )
        return await self.query("SELECT * FROM artifacts ORDER BY created_at DESC LIMIT 500")

    async def get_artifact(self, artifact_id: str) -> dict | None:
        return await self.one("SELECT * FROM artifacts WHERE id=?", (artifact_id,))

    # ---- checkpoints ----
    async def save_checkpoint_manifest(self, job_id: str, manifest: dict) -> None:
        await self.execute(
            "INSERT INTO checkpoints(job_id,manifest,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(job_id) DO UPDATE SET manifest=excluded.manifest, updated_at=excluded.updated_at",
            (job_id, json.dumps(manifest), now()),
        )

    async def get_checkpoint_manifest(self, job_id: str) -> dict | None:
        row = await self.one("SELECT * FROM checkpoints WHERE job_id=?", (job_id,))
        if not row:
            return None
        try:
            return json.loads(row["manifest"])
        except (ValueError, TypeError):
            return None


db = DB(config.DB_PATH)
