# Colab GPU Platform

Headless GPU jobs on your own Colab account. Connect Google once, then submit jobs
(code paste or notebook) from any device: jobs run server-side on Colab runtimes and
survive disconnects, browser closure, and runtime reclamation via checkpoint/resume.

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/new/template/colab-gpu-platform)

## What it does

- **Connect once** — one-time Google consent via the `/connect` page: open the
  consent URL in the browser where you are signed in to Google, approve it, then
  copy the `code=4/0A...` value from the browser's **address bar** (after approving,
  the browser lands on a "This site can't be reached" page — that is expected: the
  redirect goes to `http://localhost:8080` and nothing is listening) and paste it
  into `/connect`. Alternatively paste a gcloud ADC credential JSON
  (`~/.config/gcloud/application_default_credentials.json`). The refresh token is
  stored on your volume and refreshed server-side forever.
- **Submit GPU jobs** — `POST /api/jobs` or the dashboard: paste Python code or an
  exported `.ipynb` (notebook mode), pick an accelerator (NONE/T4/L4/A100/H100) and
  shape (STANDARD/HIGHMEM), set a timeout. Jobs run on Colab runtimes via the
  official Colab API v1beta + standard Jupyter protocol.
- **Server-side execution** — the platform holds the kernel WebSocket, not your
  browser. Close the tab; output keeps streaming to the run log. Reattach anytime
  via the dashboard or `WS /api/jobs/{id}/stream`.
- **Checkpoint / resume** — long jobs call `platform_checkpoint()` to snapshot
  `CHECKPOINT_PATH` to the platform volume. If Colab reclaims the runtime mid-run
  (it deletes idle runtimes by design), the run is marked RECLAIMED, a new attempt
  is auto-enqueued, and the checkpoint is re-uploaded to the fresh runtime before
  your code re-runs.
- **Scheduled jobs** — cron schedule per job (in-process scheduler, 30 s tick).
- **Notebook mode** — paste `.ipynb` JSON; code cells execute in order as one job.
- **Artifacts** — files written to `/content/artifacts` on the runtime are fetched
  to the platform volume at job completion; browsable/downloadable from the UI.
- **Webhooks** — `NOTIFY_WEBHOOK_URL` (or per-job webhook) fires a JSON POST on
  COMPLETE / FAILED / RECLAIMED (Discord-compatible `content` field included).

## Honest limits (read before deploying)

| Limit | Reality |
|---|---|
| Free tier lifetime | Runtimes are capped (~12 h) and **idle runtimes are actively deleted by Colab** — that is Colab's design, not a bug. Checkpoint/resume is the reliability mechanism. |
| No synthetic keep-alive | The platform never pings the runtime to keep it alive. The only periodic call is a token refresh (`GetRuntime` at expiry − 5 min) — a metadata read, identical to what Google's own VS Code client does. |
| Quota | Colab free tier has usage limits and may refuse runtimes (quota/stockout); errors surface verbatim. Pro/Pro+ raise limits and unlock A100/H100. |
| ToS | Batch compute via the official Colab API on your own account is sanctioned use. **Hosting live web services from runtimes is prohibited** — this platform only orchestrates job executions. |
| API beta | The Colab API is v1beta and (as of the vendored spec) allowlist-gated per project; if your account gets 401/403, the platform surfaces it at connect time. |
| Files | Jupyter Contents API requests cap under 50 MB; artifacts above `ARTIFACT_MAX_FILE_MB` (45 MB default) are skipped. |

## Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `GOOGLE_CREDENTIALS_JSON` | **yes (deploy-form prompt)** | The one prompt. Paste a refresh-token credential JSON (gcloud ADC file contents, or the authorization-code exchange from `/connect`). Invalid JSON degrades to keyless mode with a loud log banner — fix it at `/connect`. |
| `APP_ACCESS_PASSWORD` | no (auto-generated per deploy) | Gates mutations (jobs, connect, delete). Reads stay open (job ids are unguessable). Send as `X-Platform-Password` header or `?password=`. |
| `NOTIFY_WEBHOOK_URL` | no | Discord-style JSON POST on COMPLETE/FAILED/RECLAIMED. |
| `RUNTIME_REUSE` | no (default `false`) | Reuse one runtime across consecutive jobs instead of delete-on-completion. Idle reused runtimes are deleted after `RUNTIME_REUSE_TTL_S` (300 s default). |
| `MAX_CONCURRENT_JOBS` | no (default `1`) | Parallel job slots (each needs a Colab runtime assignment). |
| `DEFAULT_TIMEOUT_MIN` / `MAX_TIMEOUT_MIN` | no (30 / 720) | Job timeout bounds. |

## Job-code conventions (injected env + helpers)

Every run executes with:

```python
PLATFORM_URL   # this platform's public URL
JOB_ID, RUN_ID, JOB_NAME, ATTEMPT, ACCELERATOR, TIMEOUT_MIN
CHECKPOINT_PATH   # /content/checkpoints/<job_id> — created for you
ARTIFACT_DIR      # /content/artifacts — created for you; files here are harvested
platform_checkpoint()  # defined by the prelude — snapshot CHECKPOINT_PATH to the platform volume
```

Checkpoint pattern for long jobs:

```python
import os, time
CKPT = os.environ["CHECKPOINT_PATH"]
step = 0
if os.path.exists(os.path.join(CKPT, "state.json")):
    import json
    step = json.load(open(os.path.join(CKPT, "state.json")))["step"]  # restored by the platform
while step < 100:
    step += 1
    time.sleep(6)  # ... real work ...
    json.dump({"step": step}, open(os.path.join(CKPT, "state.json"), "w"))
    platform_checkpoint()  # platform-side copy survives even a full runtime deletion
```

If the runtime is reclaimed mid-run: run → RECLAIMED → webhook fires → new attempt
auto-enqueued → checkpoint files re-uploaded to the fresh runtime → your code reads
`state.json` and resumes at `step`. Cross-runtime resume that skips the platform
(restore straight from Drive/GCS in your own code) also works — write both patterns.

## API

```
GET  /health                        liveness (no Google call)
GET  /api/status                    internals: runtimes, worker, hints
GET  /api/specs                     Colab runtime specs + eligibility (connected)
GET  /api/subscription              Colab tier badge (connected)
POST /api/jobs                      {name, code | notebook_json, accelerator, shape,
                                    timeout_min, schedule, webhook_url} → 201 + run
GET  /api/jobs                      list with latest run
GET  /api/jobs/{id}                 job + run history
POST /api/jobs/{id}/run             re-run now (202)
DELETE /api/jobs/{id}               delete job + history
GET  /api/jobs/{id}/log?run_id=     full log as text
WS   /api/jobs/{id}/stream          live log stream (backlog first)
GET  /api/runs/{run_id}             run detail
GET  /api/artifacts[?job_id=]       artifact index
GET  /api/artifacts/{id}/download   artifact bytes
POST /api/connect                   {code} or {credential_json} — connect Google
POST /api/disconnect                forget credential
```

Mutations require `X-Platform-Password` when `APP_ACCESS_PASSWORD` is set (it is,
auto-generated, on template deploys — find it in the Variables tab).

## Local development

```
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --port 8000
```

Keyless boot is fully healthy: the dashboard renders, `/health` returns 200, and
Google-dependent calls return a 503-shaped `google_credential_missing` until you
complete consent at `/connect`.

## Stack

Python 3.12 · FastAPI · google-auth (server-side refresh) · httpx (Colab REST +
Jupyter Contents API) · websockets (kernel channels) · aiosqlite (SQLite on the
volume) · croniter (schedules) · Jinja2 dashboard.

MIT license. Not affiliated with Google; Colab is a trademark of Google LLC.
