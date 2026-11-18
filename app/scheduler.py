"""Scheduled jobs (croniter) — in-process scheduler tick."""
import asyncio
import logging
from datetime import datetime, timezone

from croniter import croniter

from .db import db
from .worker import worker

log = logging.getLogger("scheduler")

TICK_S = 30


def is_valid_schedule(expr: str) -> bool:
    try:
        croniter(expr)
        return True
    except (ValueError, TypeError):
        return False


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        d = datetime.fromisoformat(ts)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


async def scheduler_loop():
    while True:
        try:
            now = datetime.now(timezone.utc)
            jobs = await db.list_jobs()
            for job in jobs:
                expr = job.get("schedule")
                if not expr:
                    continue
                base = _parse(job.get("last_fire")) or _parse(job.get("created_at"))
                if base is None:
                    base = now
                try:
                    next_fire = croniter(expr, base).get_next(datetime)
                except (ValueError, TypeError):
                    continue
                if next_fire.tzinfo is None:
                    next_fire = next_fire.replace(tzinfo=timezone.utc)
                if next_fire > now:
                    continue
                # due (or catch-up after downtime): fire once
                latest = await db.latest_run(job["id"])
                if latest and latest["status"] in ("QUEUED", "PROVISIONING", "RUNNING"):
                    log.info("schedule skipped: job %s already has an active run", job["id"])
                    await db.execute(
                        "UPDATE jobs SET last_fire=? WHERE id=?", (db.now(), job["id"])
                    )
                    continue
                await worker.enqueue(job["id"], attempt=job["run_count"] + 1, trigger="schedule")
                await db.execute(
                    "UPDATE jobs SET last_fire=? WHERE id=?", (db.now(), job["id"])
                )
                log.info("scheduled fire: job %s (%s)", job["id"], job["name"])
        except Exception as e:  # noqa: BLE001
            log.error("scheduler tick error: %s", e)
        await asyncio.sleep(TICK_S)
