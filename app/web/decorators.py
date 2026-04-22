"""
Decoradores de autenticación y autorización para blueprints.
Independientes del objeto `app` — usan current_app, g, session de Flask.
"""
from functools import wraps
import os
from datetime import datetime

from flask import g, session, redirect, url_for, flash

from app.responses import json_error, wants_json_response
from app.security import ADMIN_ROLES, PLATFORM_ADMIN_ROLE
from app.web.utils import _rol_usuario_actual, _usuario_actual

_SESSION_HOURS = int(os.environ.get("SESSION_LIFETIME_HOURS", "8"))


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "usuario" not in session:
            if wants_json_response():
                return json_error("No autenticado", 401, code="auth_required")
            return redirect(url_for("auth.login"))
        login_ts = session.get("_login_ts")
        if login_ts:
            age = datetime.now().timestamp() - float(login_ts)
            if age > _SESSION_HOURS * 3600:
                session.clear()
                if wants_json_response():
                    return json_error("Sesion expirada", 401, code="session_expired")
                flash("Tu sesion expiró. Inicia sesion de nuevo.", "info")
                return redirect(url_for("auth.login"))
        last_activity_ts = session.get("_last_activity_ts")
        if last_activity_ts:
            idle = datetime.now().timestamp() - float(last_activity_ts)
            if idle > _SESSION_HOURS * 3600:
                session.clear()
                if wants_json_response():
                    return json_error("Sesion expirada por inactividad", 401, code="session_idle_timeout")
                flash("Tu sesion expiró por inactividad. Inicia sesion de nuevo.", "info")
                return redirect(url_for("auth.login"))

        # ── Invalidación server-side: verificar session_version ──────────────
        usuario = _usuario_actual()
        usuario_id = usuario.get("id")
        if usuario_id:
            try:
                from data.database import obtener_session_version_usuario
                sv_sesion = int(usuario.get("session_version") or 0)
                sv_db = obtener_session_version_usuario(int(usuario_id))
                if sv_db >= 0 and sv_db != sv_sesion:
                    session.clear()
                    if wants_json_response():
                        return json_error("Sesion revocada", 401, code="session_revoked")
                    flash("Tu sesion fue revocada. Inicia sesion de nuevo.", "warning")
                    return redirect(url_for("auth.login"))
            except Exception:
                pass  # Si la DB falla, no bloquear por esto

        session["_last_activity_ts"] = datetime.now().timestamp()
        return f(*args, **kwargs)
    return decorated


def tenant_scope_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # platform_superadmin opera sin tenant fijo — bypasa la restricción
        if _rol_usuario_actual() == PLATFORM_ADMIN_ROLE:
            return f(*args, **kwargs)
        tenant_context = getattr(g, "tenant_context", None)
        if tenant_context is None or not tenant_context.available:
            if wants_json_response():
                return json_error("Contexto de panaderia no disponible", 403, code="tenant_scope_missing")
            return redirect(url_for("auth.index"))
        return f(*args, **kwargs)
    return decorated


def sede_scope_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # platform_superadmin opera sin sede fija — bypasa la restricción
        if _rol_usuario_actual() == PLATFORM_ADMIN_ROLE:
            return f(*args, **kwargs)
        sede_context = getattr(g, "sede_context", None)
        if sede_context is None or not sede_context.available:
            if wants_json_response():
                return json_error("Contexto de sede no disponible", 403, code="sede_scope_missing")
            return redirect(url_for("auth.index"))
        return f(*args, **kwargs)
    return decorated


def roles_required(*roles: str):
    valid_roles = {str(role or "").strip().lower() for role in roles if str(role or "").strip()}

    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if _rol_usuario_actual().lower() not in valid_roles:
                if wants_json_response():
                    return json_error("Sin permiso", 403, code="forbidden")
                flash("No tienes permiso para entrar a esta seccion.", "error")
                return redirect(url_for("auth.index"))
            return f(*args, **kwargs)
        return decorated
    return decorator


def admin_required(f):
    return roles_required(*sorted(ADMIN_ROLES))(f)
