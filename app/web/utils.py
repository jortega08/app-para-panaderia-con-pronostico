"""
Utilidades compartidas para blueprints de la app de panadería.
Funciones de sesión, request, iconos y helpers varios usados por múltiples blueprints.
"""
import os
import re
import unicodedata
from datetime import datetime, timedelta

from flask import flash, g, redirect, request, session, url_for

from app.context import BrandContext
from app.responses import json_error, wants_json_response
from app.security import ensure_csrf_token, is_admin_role
from data.database import (
    obtener_adicionales,
    obtener_branding_panaderia,
    obtener_categoria_producto_nombre,
    obtener_panaderia_principal,
    obtener_sede_principal,
    obtener_session_version_usuario,
)

# ── Rate limiting config ──────────────────────────────────────────────────────
_MAX_ATTEMPTS = int(os.environ.get("MAX_LOGIN_ATTEMPTS", "5"))
_LOCKOUT_MINUTES = int(os.environ.get("LOGIN_LOCKOUT_MINUTES", "5"))

# ── Iconos y colores por categoría ────────────────────────────────────────────
ICONOS_CATEGORIA = {
    "Panaderia": "bakery_dining",
    "Bebidas Calientes": "local_cafe",
    "Bebidas Frias": "local_bar",
    "Bebidas Frías": "local_bar",
    "Desayunos": "breakfast_dining",
    "Almuerzos": "lunch_dining",
    "Acompañamientos": "bakery_dining",
    "Cacerola de Huevos": "egg",
    "Caldos": "soup_kitchen",
    "Changua": "breakfast_dining",
    "Clásicos de Queso": "bakery_dining",
    "De la Casa": "restaurant_menu",
    "Dulcería": "cookie",
    "Galletas": "cookie",
    "Hojaldre": "bakery_dining",
    "Huevos Florentinos": "egg_alt",
    "Huevos Rancheros": "egg_alt",
    "Huevos Richs": "egg_alt",
    "Omelettes": "egg_alt",
    "Pastelería Casera": "cake",
    "Sándwiches": "lunch_dining",
    "Sándwiches - Croissant": "breakfast_dining",
    "Sándwiches - Pan Saludable": "sandwich",
    "Típico": "skillet",
}

MENUS_PREFERIDOS = ["Desayunos", "Tardes"]

COLORES_PROD = {
    "Pan Frances": "#E8B44D",
    "Pan Dulce": "#E07A5F",
    "Croissant": "#81B29A",
    "Integral": "#9B8EA0",
}


def icono_categoria(categoria: str) -> str:
    return ICONOS_CATEGORIA.get(categoria, "restaurant")


def icono(nombre: str, categoria=None) -> str:
    categoria_real = categoria or obtener_categoria_producto_nombre(nombre)
    return icono_categoria(categoria_real)


def color_prod(nombre: str) -> str:
    return COLORES_PROD.get(nombre, "#B0BEC5")


# ── Helpers de sesión ─────────────────────────────────────────────────────────

def _usuario_actual() -> dict:
    return session.get("usuario", {}) if "usuario" in session else {}


def _nombre_usuario_actual() -> str:
    return str(_usuario_actual().get("nombre", "") or "").strip()


def _rol_usuario_actual() -> str:
    return str(_usuario_actual().get("rol", "") or "").strip()


def _usuario_actual_id() -> int | None:
    try:
        value = _usuario_actual().get("id")
        return int(value) if value not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        return None


def get_current_panaderia_id() -> int | None:
    tenant_context = getattr(g, "tenant_context", None)
    tenant_id = getattr(tenant_context, "id", None)
    if tenant_id not in (None, "", 0, "0"):
        try:
            return int(tenant_id)
        except (TypeError, ValueError):
            pass
    try:
        value = _usuario_actual().get("panaderia_id")
        if value not in (None, "", 0, "0"):
            return int(value)
    except (TypeError, ValueError):
        pass
    try:
        principal = obtener_panaderia_principal()
        if principal and principal.get("id") not in (None, "", 0, "0"):
            return int(principal["id"])
    except Exception:
        pass
    return None


def _panaderia_actual_id() -> int | None:
    return get_current_panaderia_id()


def _sede_actual_id() -> int | None:
    sede_context = getattr(g, "sede_context", None)
    sede_id = getattr(sede_context, "id", None)
    if sede_id not in (None, "", 0, "0"):
        try:
            return int(sede_id)
        except (TypeError, ValueError):
            pass
    try:
        value = _usuario_actual().get("sede_id")
        return int(value) if value not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        pass
    try:
        panaderia_id = get_current_panaderia_id()
        sede = obtener_sede_principal(panaderia_id)
        if sede and sede.get("id") not in (None, "", 0, "0"):
            return int(sede["id"])
    except Exception:
        pass
    return None


def _rol_es_admin() -> bool:
    return is_admin_role(_rol_usuario_actual())


# ── Helpers de request ────────────────────────────────────────────────────────

def _client_ip() -> str:
    forwarded = str(request.headers.get("X-Forwarded-For", "") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    return str(request.remote_addr or "unknown").strip() or "unknown"


def _csrf_invalid_response():
    if wants_json_response():
        return json_error("Token CSRF invalido o ausente", 403, code="csrf_invalid")
    flash("Tu sesion de seguridad cambio. Intenta de nuevo.", "error")
    return redirect(url_for("auth.login"))


def _current_csrf_token() -> str:
    return ensure_csrf_token(session)


# ── Sesión auth ───────────────────────────────────────────────────────────────

def _registrar_sesion(usuario: dict) -> None:
    """Inicializa la sesión tras autenticación exitosa."""
    session.clear()
    session.permanent = True
    # Guardar session_version actual para detectar invalidaciones server-side
    usuario_id = usuario.get("id")
    sv = int(usuario.get("session_version") or 0)
    if usuario_id and sv == 0:
        sv = obtener_session_version_usuario(int(usuario_id))
    usuario_con_version = dict(usuario)
    usuario_con_version["session_version"] = sv
    session["usuario"] = usuario_con_version
    session["_login_ts"] = datetime.now().timestamp()
    session["_last_activity_ts"] = datetime.now().timestamp()
    ensure_csrf_token(session)


# ── Permisos de negocio ───────────────────────────────────────────────────────

def _usuario_puede_registrar_produccion() -> bool:
    return _rol_usuario_actual() in ("panadero", "cajero")


def _pedido_visible_para_usuario(pedido: dict | None) -> bool:
    if not pedido:
        return False
    if _rol_usuario_actual() != "mesero":
        return True
    return str(pedido.get("mesero", "") or "").strip() == _nombre_usuario_actual()


# ── Utilidades de texto / fecha ───────────────────────────────────────────────

def _normalizar_texto(texto: str) -> str:
    base = unicodedata.normalize("NFKD", str(texto or ""))
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.strip().lower()
    return re.sub(r"[^a-z0-9]+", " ", base).strip()


def _iso_datetime(fecha: str | None, hora: str | None = None) -> str:
    fecha_txt = str(fecha or "").strip()
    hora_txt = str(hora or "").strip()
    if fecha_txt:
        if "T" in fecha_txt:
            return fecha_txt
        if " " in fecha_txt:
            return fecha_txt.replace(" ", "T")
        if hora_txt:
            return f"{fecha_txt}T{hora_txt}"
        return f"{fecha_txt}T00:00:00"
    return datetime.now().isoformat(timespec="seconds")


def _crear_notificacion(
    notif_id: str,
    title: str,
    description: str,
    notif_type: str,
    when_iso: str,
    sound: str = "",
) -> dict:
    return {
        "id": notif_id,
        "title": title,
        "description": description,
        "type": notif_type,
        "time": when_iso,
        "sound": sound,
        "source": "server",
    }


def _parse_fecha_iso(fecha: str | None = None) -> str:
    fecha_str = str(fecha or "").strip() or datetime.now().strftime("%Y-%m-%d")
    datetime.strptime(fecha_str, "%Y-%m-%d")
    return fecha_str


def _resolver_filtro_historial(default_days: int = 30) -> dict:
    hoy = datetime.now().date()
    hoy_str = hoy.strftime("%Y-%m-%d")

    try:
        dias = int(request.args.get("dias", default_days))
    except (TypeError, ValueError):
        dias = default_days
    dias = max(1, min(dias, 365))

    fecha_inicio_raw = str(request.args.get("fecha_inicio", "") or "").strip()
    fecha_fin_raw = str(request.args.get("fecha_fin", "") or "").strip()
    filtro_personalizado = bool(fecha_inicio_raw or fecha_fin_raw)

    try:
        fecha_inicio = _parse_fecha_iso(fecha_inicio_raw) if fecha_inicio_raw else None
    except ValueError:
        fecha_inicio = None
    try:
        fecha_fin = _parse_fecha_iso(fecha_fin_raw) if fecha_fin_raw else None
    except ValueError:
        fecha_fin = None

    if fecha_inicio and fecha_inicio > hoy_str:
        fecha_inicio = hoy_str
    if fecha_fin and fecha_fin > hoy_str:
        fecha_fin = hoy_str

    if fecha_fin and not fecha_inicio:
        fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
        fecha_inicio = (fecha_fin_dt - timedelta(days=dias - 1)).strftime("%Y-%m-%d")
    elif fecha_inicio and not fecha_fin:
        fecha_fin = hoy_str

    if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
        fecha_inicio, fecha_fin = fecha_fin, fecha_inicio

    if not fecha_inicio or not fecha_fin:
        fecha_fin = hoy_str
        fecha_inicio = (hoy - timedelta(days=dias - 1)).strftime("%Y-%m-%d")

    dias = max(
        (datetime.strptime(fecha_fin, "%Y-%m-%d").date() -
         datetime.strptime(fecha_inicio, "%Y-%m-%d").date()).days + 1,
        1,
    )

    return {
        "dias": dias,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "hoy_str": hoy_str,
        "filtro_personalizado": filtro_personalizado,
    }


# ── Brand context ─────────────────────────────────────────────────────────────

def _build_brand_context(tenant_id: int | None = None) -> BrandContext:
    branding = obtener_branding_panaderia(tenant_id)
    return BrandContext(
        panaderia_id=branding.get("panaderia_id"),
        brand_name=str(branding.get("brand_name", "RICHS") or "RICHS"),
        legal_name=str(branding.get("legal_name", "") or ""),
        tagline=str(branding.get("tagline", "Panaderia artesanal") or "Panaderia artesanal"),
        support_label=str(branding.get("support_label", "Delicias que nutren") or "Delicias que nutren"),
        logo_path=str(branding.get("logo_path", "brand/richs-logo.svg") or "brand/richs-logo.svg"),
        favicon_path=str(branding.get("favicon_path", "brand/richs-logo.svg") or "brand/richs-logo.svg"),
        primary_color=str(branding.get("primary_color", "#8b5513") or "#8b5513"),
        secondary_color=str(branding.get("secondary_color", "#d4722a") or "#d4722a"),
        accent_color=str(branding.get("accent_color", "#e0a142") or "#e0a142"),
    )


# ── DB wrappers ───────────────────────────────────────────────────────────────

def _obtener_adicionales_operativos() -> list:
    return list(obtener_adicionales())
