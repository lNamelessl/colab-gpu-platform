"""Colab GPU Platform — FastAPI app entrypoint."""
import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import config, gauth
from .db import db
from .routes import router
from .scheduler import scheduler_loop
from .worker import worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
log = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    # Seed credential from env (template deploys: GOOGLE_CREDENTIALS_JSON).
    if config.GOOGLE_CREDENTIALS_JSON:
        try:
            raw = json.loads(config.GOOGLE_CREDENTIALS_JSON)
            summary = gauth.store_credential(raw)
            log.info(
                "credential loaded from GOOGLE_CREDENTIALS_JSON (client ...%s)",
                summary.get("client_id_tail"),
            )
        except (ValueError, gauth.CredentialError) as e:
            log.warning(
                "!!!!!!!! GOOGLE_CREDENTIALS_JSON INVALID — RUNNING KEYLESS !!!!!!!!\n"
                "  reason: %s\n"
                "  the platform stays healthy; paste a valid credential at /connect",
                e,
            )
    elif not gauth.connected():
        log.info(
            "no Google credential yet — keyless mode: dashboard works, Google ops "
            "return 503. Complete consent at /connect"
        )
    from .runtime_manager import manager

    async def on_reclaimed(runtime_id: str):
        # find the active run pinned to this runtime and mark it RECLAIMED
        active = await db.active_runs()
        for run in active:
            if run.get("runtime_id") == runtime_id:
                await worker._handle_reclaim(
                    await db.get_job(run["job_id"]),
                    run,
                    type("H", (), {"runtime_id": runtime_id})(),
                    "runtime reclaimed by Colab (404 on token refresh)",
                )
                return

    tasks = [
        asyncio.create_task(worker.run_loop()),
        asyncio.create_task(scheduler_loop()),
        asyncio.create_task(manager.refresher_loop(on_reclaimed)),
    ]
    log.info(
        "colab-gpu-platform up: port=%s reuse=%s max_concurrent=%s",
        config.PORT, config.RUNTIME_REUSE, config.MAX_CONCURRENT_JOBS,
    )
    yield
    for t in tasks:
        t.cancel()
    await db.close()


app = FastAPI(title="Colab GPU Platform", version="1.0.0", lifespan=lifespan)
app.include_router(router)
