"""Webhook notifications on COMPLETE / FAILED / RECLAIMED."""
import logging

import httpx

from . import config

log = logging.getLogger("webhooks")


async def fire_event(event: str, job: dict, run: dict, error: str | None) -> None:
    url = (job.get("webhook_url") or config.NOTIFY_WEBHOOK_URL or "").strip()
    if not url:
        return
    payload = {
        "event": event,
        "source": "colab-gpu-platform",
        "job_id": job["id"],
        "job_name": job["name"],
        "run_id": run["id"],
        "attempt": run["attempt"],
        "status": event,
        "error": error,
        "finished_at": run.get("finished_at"),
        # Discord-friendly + generic fields
        "content": f"colab-gpu-platform: job '{job['name']}' -> {event}"
        + (f" ({error[:200]})" if error else ""),
        "text": f"colab-gpu-platform: job '{job['name']}' -> {event}"
        + (f" ({error[:200]})" if error else ""),
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json=payload)
            log.info("webhook %s for job %s: HTTP %s", event, job["id"], r.status_code)
    except Exception as e:  # noqa: BLE001
        log.warning("webhook delivery failed for job %s: %s", job["id"], e)
