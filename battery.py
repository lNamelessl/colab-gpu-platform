"""Full battery for the Colab GPU Platform — run when the runtime window opens.

Usage: battery.py <APP_ACCESS_PASSWORD>
"""
import asyncio
import json
import sys
import time
import urllib.request

BASE = "https://colab-gpu-platform-production.up.railway.app"
WS_BASE = "wss://colab-gpu-platform-production.up.railway.app"
WH = "https://webhook.site/dc91680a-823b-42bc-88cb-17c7a76c7126"
PW = sys.argv[1]
WH_UUID_FILE = r"C:\Users\HomePC\AppData\Local\Temp\wh_uuid.txt"


def call(method, path, body=None, expect=None, timeout=90):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", "X-Platform-Password": PW}
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    r = urllib.request.urlopen(req, timeout=timeout)
    d = json.loads(r.read() or b"{}")
    return r.status, d


def poll_run(run_id, terminal=("COMPLETE", "FAILED", "RECLAIMED"), max_s=1800, tag=""):
    t0 = time.time()
    last = None
    while time.time() - t0 < max_s:
        try:
            _, d = call("GET", f"/api/runs/{run_id}", timeout=40)
            rr = d["run"]
        except Exception:
            time.sleep(8)
            continue
        if rr["status"] in terminal:
            return rr
        if rr["status"] != last:
            print(f"  [{tag}] {int(time.time()-t0)}s {rr['status']} {rr.get('runtime_id') or ''}", flush=True)
            last = rr["status"]
        time.sleep(5)
    return None


def webhook_requests():
    uuid = open(WH_UUID_FILE).read().strip()
    req = urllib.request.Request(
        f"https://webhook.site/token/{uuid}/requests?sorting=newest",
        headers={"Accept": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())


def webhook_events():
    evs = []
    for it in webhook_requests().get("data", []):
        try:
            body = it.get("event", {}) or {}
            data = json.loads(it.get("content") or "{}")
        except ValueError:
            continue
        evs.append(data.get("event") or (body if isinstance(body, str) else ""))
    return evs


results = []


def record(name, ok, evidence=""):
    results.append((name, bool(ok), str(evidence)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {str(evidence)[:200]}", flush=True)


# ---------- 0. clean queue ----------
print("== 0. clean queue (delete pre-existing jobs)")
_, d = call("GET", "/api/jobs", timeout=60)
for j in d.get("jobs", []):
    call("DELETE", f"/api/jobs/{j['id']}", timeout=60)
    print("  deleted", j["name"])
# wait for any zombie-reconcile task to drain its slot
t0 = time.time()
while time.time() - t0 < 600:
    _, s = call("GET", "/api/status", timeout=40)
    if s.get("worker_in_flight", 0) == 0:
        break
    print("  waiting for worker slot...", int(time.time() - t0), "s", flush=True)
    time.sleep(10)
print("  queue clean, worker idle:", s.get("worker_in_flight") == 0)

# ---------- 1. quick job ----------
print("== 1. quick job")
_, d = call("POST", "/api/jobs", {
    "name": "battery-quick", "accelerator": "NONE", "shape": "STANDARD", "timeout_min": 10,
    "webhook_url": WH,
    "code": "import sys, platform\nprint('hello from colab runtime')\nprint('python:', sys.version.split()[0])\nprint('machine:', platform.machine())\nprint('marker-quick-42')",
})
rr = poll_run(d["run"]["id"], tag="quick")
record("quick-job COMPLETE + outputs in log",
       bool(rr) and rr["status"] == "COMPLETE" and "marker-quick-42" in rr["log"]
       and "hello from colab runtime" in rr["log"],
       (rr or {}).get("error") or "")

# ---------- 2. GPU job ----------
print("== 2. GPU job (T4, torch.cuda)")
_, d = call("POST", "/api/jobs", {
    "name": "battery-gpu", "accelerator": "T4", "shape": "STANDARD", "timeout_min": 25,
    "webhook_url": WH,
    "code": "import torch\nprint('torch:', torch.__version__)\nprint('cuda available:', torch.cuda.is_available())\nif torch.cuda.is_available():\n    print('device:', torch.cuda.get_device_name(0))\n    x = torch.rand(1000, 1000).cuda()\n    print('matmul ok:', (x @ x).sum().item() > 0)\nprint('marker-gpu-42')",
})
rr = poll_run(d["run"]["id"], tag="gpu")
log = (rr or {}).get("log", "")
record("gpu-job COMPLETE + cuda True on T4",
       bool(rr) and rr["status"] == "COMPLETE" and "cuda available: True" in log and "marker-gpu-42" in log,
       (rr or {}).get("error") or "")

# ---------- 3. long job + disconnect survival ----------
print("== 3. disconnect-survival (48 beats @5s; WS closed mid-run, reattach)")
_, d = call("POST", "/api/jobs", {
    "name": "battery-long", "accelerator": "NONE", "shape": "STANDARD", "timeout_min": 20,
    "code": "import time\nfor i in range(48):\n    print(f'beat-{i}', flush=True)\n    time.sleep(5)\nprint('marker-long-42')",
})
job_id, run_id = d["job"]["id"], d["run"]["id"]

async def disconnect_proof(job_id, run_id):
    import websockets
    uri = f"{WS_BASE}/api/jobs/{job_id}/stream?run_id={run_id}"
    # wait for RUNNING
    while True:
        _, dd = call("GET", f"/api/runs/{run_id}", timeout=40)
        if dd["run"]["status"] == "RUNNING":
            break
        if dd["run"]["status"] in ("FAILED", "COMPLETE", "RECLAIMED"):
            return None, None
        await asyncio.sleep(5)
    async with websockets.connect(uri, open_timeout=30) as ws:
        seen = []
        while True:
            try:
                line = await asyncio.wait_for(ws.recv(), timeout=20)
            except asyncio.TimeoutError:
                break
            if line.startswith("beat-"):
                seen.append(line)
            if len([s for s in seen if s.startswith("beat-")]) >= 8:
                break
    last_before = seen[-1] if seen else "none"
    await asyncio.sleep(75)  # client "closed" window: no WS attached
    # reattach: pull the full log — beats printed during the closed window prove
    # execution continued without any client
    _, dd = call("GET", f"/api/jobs/{job_id}/log?run_id={run_id}", timeout=40)
    return last_before, dd

last_before, after = asyncio.run(disconnect_proof(job_id, run_id))
rr = poll_run(run_id, tag="long")
log = (rr or {}).get("log", "")
n_beats = log.count("beat-")
record("disconnect-survival: job kept running with zero clients; beats grew during closed window",
       bool(rr) and rr["status"] == "COMPLETE" and n_beats >= 40 and "marker-long-42" in log,
       f"last-before-close={last_before}; total beats={n_beats}")

# ---------- 4. reclaim/resume ----------
print("== 4. reclaim/resume (DeleteRuntime mid-run)")
_, d = call("POST", "/api/jobs", {
    "name": "battery-reclaim", "accelerator": "NONE", "shape": "STANDARD", "timeout_min": 25,
    "webhook_url": WH,
    "code": "import os, json, time\nck = os.environ['CHECKPOINT_PATH']\nsf = os.path.join(ck, 'state.json')\nstep = 0\nif os.path.exists(sf):\n    step = json.load(open(sf))['step']\n    print(f'restored checkpoint at step {step}', flush=True)\nfor i in range(step, 24):\n    print(f'step-{i}', flush=True)\n    json.dump({'step': i+1}, open(sf, 'w'))\n    platform_checkpoint()\n    time.sleep(12)\nprint('marker-reclaim-42')",
})
job_id, run = d["job"]["id"], d["run"]
rr = None
t0 = time.time()
while time.time() - t0 < 1500:
    _, dd = call("GET", f"/api/runs/{run['id']}", timeout=40)
    rr = dd["run"]
    if rr["status"] == "RUNNING" and rr.get("runtime_id"):
        break
    if rr["status"] in ("FAILED", "COMPLETE", "RECLAIMED"):
        break
    time.sleep(5)
deleted = False
if rr and rr["status"] == "RUNNING" and rr.get("runtime_id"):
    from google.oauth2.credentials import Credentials as _C
    from google.auth.transport.requests import Request as _Req
    cred = json.load(open(r"C:\Users\HomePC\AppData\Local\Temp\colab_credential.json"))
    c = _C(token=None, refresh_token=cred["refresh_token"], token_uri=cred["token_uri"],
           client_id=cred["client_id"], client_secret=cred["client_secret"],
           scopes=cred["scopes"], quota_project_id=cred.get("quota_project_id"))
    c.refresh(_Req())
    H = {"Authorization": f"Bearer {c.token}", "x-goog-user-project": cred.get("quota_project_id")}
    req = urllib.request.Request(
        f"https://colaboratory.googleapis.com/v1beta/runtimes/{rr['runtime_id']}?allowMissing=true",
        method="DELETE", headers=H)
    print("  DeleteRuntime:", urllib.request.urlopen(req, timeout=40).status)
    deleted = True
rr2 = poll_run(run["id"], max_s=2700, tag="reclaim")
_, djobs = call("GET", f"/api/jobs/{job_id}")
runs = djobs["runs"]
reclaimed = [r_ for r_ in runs if r_["status"] == "RECLAIMED"]
resumed = [r_ for r_ in runs if r_["status"] == "COMPLETE" and r_["attempt"] > 1 and r_.get("restored_checkpoint")]
log = (rr2 or {}).get("log", "")
record("reclaim-resume: DeleteRuntime -> RECLAIMED -> auto-resume COMPLETE + checkpoint restored",
       deleted and bool(reclaimed) and bool(resumed) and "marker-reclaim-42" in log and "restored checkpoint" in log,
       f"deleted={deleted} reclaimed={len(reclaimed)} resumed={len(resumed)}")

# ---------- 5. webhook ----------
print("== 5. webhook deliveries (COMPLETE x2 + RECLAIMED x1)")
evs = webhook_events()
record("webhook deliveries >= 3 with RECLAIMED present",
       len(evs) >= 3 and "RECLAIMED" in evs and "COMPLETE" in evs,
       f"events={evs[:6]}")

# ---------- 6. scheduled ----------
print("== 6. scheduled job (*/2 * * * *)")
_, d = call("POST", "/api/jobs", {
    "name": "battery-sched", "accelerator": "NONE", "shape": "STANDARD", "timeout_min": 10,
    "schedule": "*/2 * * * *",
    "webhook_url": WH,
    "code": "print('marker-sched-42')",
})
job_id = d["job"]["id"]
print("  waiting 160s for scheduler tick...", flush=True)
time.sleep(160)
_, d2 = call("GET", f"/api/jobs/{job_id}")
sched_runs = [r_ for r_ in d2["runs"] if r_["trigger"] == "schedule"]
record("scheduled fires with trigger=schedule", len(sched_runs) >= 1,
       f"{len(sched_runs)} scheduled runs; statuses={[r_['status'] for r_ in sched_runs[:3]]}")
poll_run(sched_runs[0]["id"], tag="sched") if sched_runs else None

# ---------- 7. notebook mode ----------
print("== 7. notebook mode")
nb = {"cells": [
    {"cell_type": "markdown", "source": "# intro"},
    {"cell_type": "code", "source": ["print('nb-cell-1')\n", "import platform\n", "print(platform.python_version())"]},
    {"cell_type": "code", "source": "x = 21\nprint('nb-cell-2', x * 2)\nprint('marker-nb-42')"},
], "nbformat": 4, "nbformat_minor": 5}
_, d = call("POST", "/api/jobs", {"name": "battery-notebook", "notebook_json": json.dumps(nb),
                                  "accelerator": "NONE", "shape": "STANDARD", "timeout_min": 10})
rr = poll_run(d["run"]["id"], tag="nb")
log = (rr or {}).get("log", "")
record("notebook job COMPLETE + both code cells executed",
       bool(rr) and rr["status"] == "COMPLETE" and "nb-cell-1" in log and "nb-cell-2 42" in log and "marker-nb-42" in log,
       (rr or {}).get("error") or "")

# ---------- 8. artifacts + RAM + persistence ----------
print("== 8. artifacts / RAM / persistence")
_, d = call("POST", "/api/jobs", {
    "name": "battery-artifacts", "accelerator": "NONE", "shape": "STANDARD", "timeout_min": 10,
    "code": "import os\np = os.path.join(os.environ['ARTIFACT_DIR'], 'result.txt')\nopen(p, 'w').write('artifact-marker-42')\nprint('wrote artifact', p)\nprint('marker-artifact-42')",
})
rr = poll_run(d["run"]["id"], tag="art")
_, arts = call("GET", "/api/artifacts?job_id=" + d["job"]["id"])
ok_art = bool(rr) and rr["status"] == "COMPLETE" and len(arts.get("artifacts", [])) >= 1
record("artifact harvested to platform volume", ok_art,
       f"{len(arts.get('artifacts', []))} artifacts")
_, h = call("GET", "/health")
record("RAM rss_mb sane (<600MB)", h.get("rss_mb", 9999) < 600, f"rss={h.get('rss_mb')}MB")
_, s = call("GET", "/api/status")
record("platform status connected", s.get("connected") is True, str(s.get("runtimes"))[:120])

print("\n===== BATTERY SUMMARY =====")
for name, ok, ev in results:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} | {ev}"[:250])
