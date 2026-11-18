# Colab GPU Platform — headless GPU jobs that survive disconnects

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/colab-gpu-platform)

Submit GPU jobs to your own Colab account from any device — paste code or a notebook,
pick an accelerator, close the tab. Jobs run server-side on Colab runtimes (official
Colab API v1beta + standard Jupyter protocol), output streams into a run log you can
reattach to anytime, and a checkpoint/resume layer handles Colab's runtime reclamation
instead of pretending it does not exist.

**One deploy prompt**: `GOOGLE_CREDENTIALS_JSON` — a refresh-token credential for your
Google account. Everything else (password, webhook, concurrency) is optional and can be
added post-deploy.

## What you get

- **Dashboard** (Jinja2): connect flow, job list, job detail with live log stream,
  artifact browser, Colab tier badge.
- **Job API**: `POST /api/jobs` (code paste or `.ipynb` notebook mode, accelerator
  NONE/T4/L4/A100/H100, shape STANDARD/HIGHMEM, timeout, cron schedule, per-job
  webhook), run history, `WS /api/jobs/{id}/stream` for live output.
- **Runtime lifecycle**: CreateRuntime (LRO) → token refresh at expiry−5 min →
  Jupyter session execute → DeleteRuntime on completion (or reuse with
  `RUNTIME_REUSE=true`; idle reused runtimes are reaped after 5 min).
- **Checkpoint/resume**: jobs call `platform_checkpoint()` to snapshot
  `CHECKPOINT_PATH` to the platform volume; on reclaim, the run is marked RECLAIMED,
  a webhook fires, and a new attempt auto-resumes with the checkpoint re-uploaded
  before your code re-runs.
- **Scheduler**: per-job cron (`*/15 * * * *` etc.), 30 s tick, skips ticks when a
  run is already active.
- **Artifacts**: files written to `/content/artifacts` are harvested to the volume at
  completion and downloadable from the UI.
- **Notifications**: `NOTIFY_WEBHOOK_URL` (Discord-compatible JSON) on
  COMPLETE/FAILED/RECLAIMED.
- **Security**: `APP_ACCESS_PASSWORD` (auto-generated per deploy) gates mutations;
  reads stay open via unguessable job ids.

## Honest limits

- Free-tier Colab caps runtimes (~12 h) and **actively deletes idle runtimes** — that
  is Colab's design. This platform never sends synthetic keep-alive; checkpoint/resume
  is the reliability mechanism.
- Colab quota can refuse runtime creation (quota/stockout) — errors surface verbatim.
  Pro/Pro+ raise limits and unlock A100/H100 (eligibility comes from the API itself).
- The Colab API is v1beta and allowlist-gated for some accounts; a 401/403 surfaces
  immediately at connect time.
- Files move through the Jupyter Contents API: 45 MB max per artifact/checkpoint file.
- Orchestration via the official API on your own account is sanctioned use; hosting
  live web services from runtimes is prohibited and this platform does not do it.

## Deploy

1. Click the deploy button. The deploy form asks for **`GOOGLE_CREDENTIALS_JSON`**:
   on your own machine run
   `gcloud auth application-default login --scopes=openid,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/colaboratory`,
   then paste the contents of `~/.config/gcloud/application_default_credentials.json`
   (or, after deploy, use the `/connect` page: open the consent URL, paste the
   authorization code).
2. Railway provisions one service + a `/data` volume (credential, SQLite jobs/runs,
   checkpoints, artifacts) and a public domain with a `/health` healthcheck.
3. Open the domain → submit a job from the dashboard or `POST /api/jobs`.

# Deploy and Host

## About Hosting

One Railway service (Python 3.12 / FastAPI) with a persistent `/data` volume:

- **colab-gpu-platform** — the API + dashboard + worker. Healthcheck at `/health`
  (restart ON_FAILURE, max 10 retries). SQLite (jobs, runs, artifacts, checkpoints)
  and the Google credential JSON live on the volume, so a redeploy keeps all state.
- Public domain on the service; TCP is plain HTTPS (the platform talks to Colab
  server-side — nothing inbound except this dashboard/API).

Deploy-form variables:

- `GOOGLE_CREDENTIALS_JSON` (required) — refresh-token credential JSON for your
  Google account (gcloud ADC file contents, or paste the authorization code at
  `/connect` post-deploy).
- `APP_ACCESS_PASSWORD` — auto-generated per deploy (`${{secret(24)}}`), shown in
  the Variables tab; gates all mutations.

## Why Deploy

Colab gives you free/cheap GPUs but only through a browser tab that dies when you
close it. This template turns that into a real job platform: server-side execution,
a REST/WS API, cron schedules, checkpoints, and artifacts — with the runtime
lifecycle handled the way Colab actually behaves (idle reclamation is expected and
recovered, not fought with keep-alive hacks). Deploying the raw pieces yourself
means gluing OAuth refresh, LRO polling, the Jupyter kernel WebSocket protocol,
Contents-API chunking, and a reclaim state machine by hand — this template ships
all of it working, on one service and one volume.

## Common Use Cases

- Long training/eval sweeps that outlive a browser session (checkpoint every N steps).
- Nightly scheduled GPU jobs (`0 3 * * *`) — backtests, data pipelines, fine-tunes.
- Notebook mode: run an exported `.ipynb` headless on a schedule.
- Team job endpoint: submit jobs from scripts/CI via `POST /api/jobs`, watch via
  `WS /api/jobs/{id}/stream`, get pinged on Discord when they finish or get reclaimed.
- Artifact collection: models/metrics written to `/content/artifacts` land on the
  platform volume, downloadable from the dashboard.

## Dependencies for

### Deployment Dependencies

- A Google account with Colab access (free tier works; Pro/Pro+ raise limits and
  unlock A100/H100) and one-time consent via the `/connect` page or the
  `gcloud auth application-default login` command above. The credential refreshes
  server-side forever; nothing else is required.
- No external database, no other services: SQLite on the volume.
- Colab API v1beta is Google's beta API; usage is subject to Colab's quotas and
  Terms of Service (batch orchestration on your own account — no hosted web
  services from runtimes, no keep-alive synthetics, both by design here).
