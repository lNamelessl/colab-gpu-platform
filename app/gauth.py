"""Google OAuth credential handling (BYO refresh token).

Accepted credential shapes (stored JSON):
- gcloud ADC form: {"refresh_token": "...", "client_id": "...", "client_secret": "...",
  "type": "authorized_user", ...}  (the file gcloud writes to
  ~/.config/gcloud/application_default_credentials.json)
- minimal form: {"refresh_token", "client_id", "client_secret",
  "token_uri": "https://oauth2.googleapis.com/token"}

The credential is refreshed server-side forever via google-auth; access tokens
are short-lived and never persisted.
"""
import json
import logging
import threading
import time
from pathlib import Path

import httpx
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from . import config

log = logging.getLogger("gauth")

_lock = threading.Lock()
_cached_creds: Credentials | None = None
_cached_scopes: list[str] | None = None
_identity: dict | None = None  # {"email": ..., "client_id_tail": ...}


class CredentialError(Exception):
    pass


def _normalize(raw: dict) -> dict:
    if not isinstance(raw, dict):
        raise CredentialError("credential JSON must be an object")
    if "refresh_token" not in raw:
        raise CredentialError(
            "credential JSON must contain refresh_token (run `gcloud auth "
            "application-default login` locally and paste that JSON, or paste "
            "the authorization code from the consent page)"
        )
    out = {
        "refresh_token": raw["refresh_token"],
        "client_id": raw.get("client_id") or config.OAUTH_CLIENT["client_id"],
        "client_secret": raw.get("client_secret") or config.OAUTH_CLIENT["client_secret"],
        "token_uri": raw.get("token_uri") or "https://oauth2.googleapis.com/token",
        "scopes": raw.get("scopes") or config.OAUTH_SCOPES,
        "quota_project_id": raw.get("quota_project_id")
        or config.QUOTA_PROJECT_ID
        or "",
    }
    return out


def _make_credentials(norm: dict, access_token: str | None = None) -> Credentials:
    return Credentials(
        token=access_token,
        refresh_token=norm["refresh_token"],
        token_uri=norm["token_uri"],
        client_id=norm["client_id"],
        client_secret=norm["client_secret"],
        scopes=norm["scopes"],
        quota_project_id=norm.get("quota_project_id") or None,
    )


def store_credential(raw: dict) -> dict:
    """Validate + persist a credential JSON. Returns masked summary."""
    global _cached_creds, _identity
    norm = _normalize(raw)
    # Prove the refresh token actually works before storing anything.
    creds = _make_credentials(norm)
    creds.refresh(Request())
    if not creds.token:
        raise CredentialError("refresh failed: no access token returned")
    config.CREDENTIAL_PATH.write_text(json.dumps(norm), encoding="utf-8")
    with _lock:
        _cached_creds = creds
        _identity = None
    return {
        "ok": True,
        "client_id_tail": norm["client_id"][-12:],
        "scopes": norm["scopes"],
        "quota_project_id": norm.get("quota_project_id") or None,
    }


def load_credential() -> Credentials | None:
    """Return valid (refreshed if needed) credentials, or None if keyless."""
    global _cached_creds
    # Env var wins (template deploys set GOOGLE_CREDENTIALS_JSON).
    if config.GOOGLE_CREDENTIALS_JSON and not config.CREDENTIAL_PATH.exists():
        try:
            raw = json.loads(config.GOOGLE_CREDENTIALS_JSON)
            store_credential(raw)
        except (ValueError, CredentialError) as e:
            log.warning("GOOGLE_CREDENTIALS_JSON invalid, running keyless: %s", e)
    if not config.CREDENTIAL_PATH.exists():
        return None
    with _lock:
        if _cached_creds is not None:
            if _cached_creds.valid:
                return _cached_creds
            try:
                _cached_creds.refresh(Request())
                return _cached_creds
            except Exception as e:  # noqa: BLE001
                log.warning("token refresh failed: %s", e)
                _cached_creds = None
    try:
        raw = json.loads(config.CREDENTIAL_PATH.read_text(encoding="utf-8"))
        norm = _normalize(raw)
    except (ValueError, CredentialError, OSError) as e:
        log.warning("stored credential unreadable: %s", e)
        return None
    creds = _make_credentials(norm)
    try:
        creds.refresh(Request())
    except Exception as e:  # noqa: BLE001
        log.warning("credential refresh failed: %s", e)
        return None
    with _lock:
        _cached_creds = creds
    return creds


def connected() -> bool:
    return config.CREDENTIAL_PATH.exists() or bool(
        config.GOOGLE_CREDENTIALS_JSON
    )


async def access_token() -> str | None:
    creds = load_credential()
    return creds.token if creds else None


async def fetch_identity() -> dict | None:
    """Best-effort account email for the dashboard badge."""
    global _identity
    if _identity is not None:
        return _identity
    tok = await access_token()
    if not tok:
        return None
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://www.googleapis.com/oauth2/v3/userinfo",
                headers={"Authorization": f"Bearer {tok}"},
            )
            if r.status_code == 200:
                data = r.json()
                _identity = {"email": data.get("email"), "sub_tail": (data.get("sub") or "")[-8:]}
                return _identity
    except Exception as e:  # noqa: BLE001
        log.debug("identity fetch failed: %s", e)
    return None


def disconnect() -> None:
    global _cached_creds, _identity
    with _lock:
        _cached_creds = None
        _identity = None
    if config.CREDENTIAL_PATH.exists():
        config.CREDENTIAL_PATH.unlink()


def exchange_code(code: str) -> dict:
    """Exchange an authorization code (copy-paste consent flow) for tokens."""
    data = {
        "code": code,
        "client_id": config.OAUTH_CLIENT["client_id"],
        "client_secret": config.OAUTH_CLIENT["client_secret"],
        "redirect_uri": config.OAUTH_CLIENT["redirect_uri"],
        "grant_type": "authorization_code",
    }
    with httpx.Client(timeout=20) as client:
        r = client.post(config.OAUTH_CLIENT["token_uri"], data=data)
        if r.status_code != 200:
            raise CredentialError(f"token exchange failed: HTTP {r.status_code} {r.text[:300]}")
        tok = r.json()
    if "refresh_token" not in tok:
        raise CredentialError(
            "no refresh_token in exchange response (re-run consent with prompt=consent)"
        )
    return {
        "refresh_token": tok["refresh_token"],
        "access_token": tok.get("access_token"),
        "client_id": config.OAUTH_CLIENT["client_id"],
        "client_secret": config.OAUTH_CLIENT["client_secret"],
        "token_uri": config.OAUTH_CLIENT["token_uri"],
    }
