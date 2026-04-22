from __future__ import annotations

import secrets


PLATFORM_ADMIN_ROLE = "platform_superadmin"
TENANT_ADMIN_ROLE = "tenant_admin"
OPERATIONAL_ROLES = {"panadero", "cajero", "mesero"}
VALID_ROLES = {PLATFORM_ADMIN_ROLE, TENANT_ADMIN_ROLE, *OPERATIONAL_ROLES}
ADMIN_ROLES = {PLATFORM_ADMIN_ROLE, TENANT_ADMIN_ROLE, "panadero"}
CSRF_SESSION_KEY = "_csrf_token"


def is_valid_role(role: str) -> bool:
    return str(role or "").strip().lower() in VALID_ROLES


def is_admin_role(role: str) -> bool:
    return str(role or "").strip().lower() in ADMIN_ROLES


def normalize_role(role: str, fallback: str = "cajero") -> str:
    candidate = str(role or "").strip().lower()
    return candidate if candidate in VALID_ROLES else fallback


def ensure_csrf_token(session) -> str:
    token = str(session.get(CSRF_SESSION_KEY, "") or "").strip()
    if not token:
        token = secrets.token_urlsafe(32)
        session[CSRF_SESSION_KEY] = token
    return token
