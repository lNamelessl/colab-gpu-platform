"""Configuration for the Colab GPU Platform."""
import os
from pathlib import Path

def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default

def _bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")

PORT = _int("PORT", 8000)
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
if not DATA_DIR.exists():
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    except OSError:
        DATA_DIR = Path(os.getcwd()) / "data"
        DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "platform.db"
CREDENTIAL_PATH = DATA_DIR / "credentials.json"

# Google-dependent operations degrade to a 503-shaped error when no
# credential is present (keyless boot stays healthy).
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "") or ""

# Optional mutation password. If set, every mutation (jobs, connect,
# checkpoints, artifacts delete) requires X-Platform-Password / ?password= /
# Bearer <pw>. Reads stay open (job ids are unguessable uuid4).
APP_ACCESS_PASSWORD = os.environ.get("APP_ACCESS_PASSWORD", "") or ""

# Optional global webhook (Discord-style POST) fired on COMPLETE/FAILED/RECLAIMED.
NOTIFY_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "") or ""

# Reuse one Colab runtime across consecutive jobs (per spec: default off,
# delete on completion).
RUNTIME_REUSE = _bool("RUNTIME_REUSE", False)
# How long a reused idle runtime is kept before it is deleted (seconds).
RUNTIME_REUSE_TTL_S = _int("RUNTIME_REUSE_TTL_S", 300)
MAX_CONCURRENT_JOBS = _int("MAX_CONCURRENT_JOBS", 1)

# Job limits
DEFAULT_TIMEOUT_MIN = _int("DEFAULT_TIMEOUT_MIN", 30)
MAX_TIMEOUT_MIN = _int("MAX_TIMEOUT_MIN", 720)  # free tier caps at 12h anyway
LOG_CAP_BYTES = _int("LOG_CAP_BYTES", 1_000_000)

# Artifact / checkpoint storage caps on the platform volume
ARTIFACT_TOTAL_CAP_MB = _int("ARTIFACT_TOTAL_CAP_MB", 200)
ARTIFACT_MAX_FILE_MB = _int("ARTIFACT_MAX_FILE_MB", 45)  # Jupyter Contents API <50MB/request
CHECKPOINT_MAX_TOTAL_MB = _int("CHECKPOINT_MAX_TOTAL_MB", 100)

# Paths on the Colab runtime filesystem (/content persists across sessions)
RUNTIME_JOB_DIR = "/content/colab-gpu-platform"
RUNTIME_ARTIFACT_DIR = "/content/artifacts"
RUNTIME_CHECKPOINT_DIR = "/content/checkpoints"

# Public base URL of this platform (for job-code callback conventions).
PUBLIC_BASE_URL = (os.environ.get("PUBLIC_BASE_URL", "") or "").rstrip("/")

# OAuth client used for the copy-paste consent flow. This is Google's own
# cloud-SDK OAuth client (the exact config colab-cli ships and gcloud uses),
# with the registered remote copy-paste redirect page. Reusing another client
# id with this redirect yields redirect_uri_mismatch.
OAUTH_CLIENT = {
    "client_id": "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com",
    "client_secret": "d-FL95Q19q7MQmFpd7hHD0Ty",
    "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "redirect_uri": "https://sdk.cloud.google.com/applicationdefaultauthcode.html",
}
OAUTH_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/colaboratory",
    "https://www.googleapis.com/auth/drive.file",
]

COLAB_API_BASE = "https://colaboratory.googleapis.com"
CLIENT_AGENT = "colab-gpu-platform/1.0"

def consent_url(state: str = "") -> str:
    """The one-time consent URL the user opens in their own browser."""
    from urllib.parse import urlencode
    params = {
        "client_id": OAUTH_CLIENT["client_id"],
        "redirect_uri": OAUTH_CLIENT["redirect_uri"],
        "response_type": "code",
        "scope": " ".join(OAUTH_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "token_usage": "remote",
    }
    if state:
        params["state"] = state
    return OAUTH_CLIENT["auth_uri"] + "?" + urlencode(params)
