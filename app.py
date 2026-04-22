import csv
import base64
import io
import json
import os
import re
import secrets
import smtplib
import socket
import time
import unicodedata
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from email.message import EmailMessage
from io import BytesIO
from xml.etree import ElementTree as ET

# Cargar variables locales solo en desarrollo.
try:
    from dotenv import load_dotenv
    if not any(os.environ.get(key) for key in ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "RAILWAY_SERVICE_ID")):
        load_dotenv()
except ImportError:
    pass

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, flash, Response, g, abort,
)
from werkzeug.middleware.proxy_fix import ProxyFix
from data.db_adapter import get_database_info

from data.database import (
    inicializar_base_de_datos,
    get_connection,
    registrar_audit,
    obtener_audit_log,
    obtener_top_productos_dia,
    obtener_alertas_stock_productos,
    actualizar_stock_minimo_producto,
    guardar_ajuste_pronostico,
    obtener_ajuste_pronostico,
    obtener_historial_ajustes,
    registrar_merma,
    obtener_mermas_dia,
    obtener_resumen_mermas,
    obtener_factor_dia_especial,
    obtener_dias_especiales,
    guardar_dia_especial,
    obtener_resumen_cierre_diario,
    exportar_ventas_csv,
    exportar_inventario_csv,
    exportar_productos_sistema,
    guardar_registro,
    descartar_stock_produccion,
    obtener_registros,
    obtener_productos,
    obtener_productos_panaderia,
    obtener_productos_con_precio,
    obtener_producto_por_id,
    generar_surtido_por_valor,
    obtener_categorias_producto,
    obtener_categoria_producto_nombre,
    agregar_producto,
    guardar_catalogo_productos,
    agregar_categoria_producto,
    actualizar_categoria_producto,
    actualizar_producto_completo,
    actualizar_producto_adicional,
    eliminar_producto_por_id,
    guardar_catalogo_insumos,
    actualizar_precio,
    verificar_pin,
    verificar_password,
    cambiar_password_usuario,
    set_query_context,
    obtener_usuarios,
    agregar_usuario,
    actualizar_usuario,
    resetear_pin_usuario,
    eliminar_usuario,
    set_usuario_activo,
    obtener_panaderia_principal,
    obtener_sede_principal,
    obtener_branding_panaderia,
    obtener_estado_login_attempts,
    limpiar_login_attempts,
    registrar_login_attempts_fallido,
    registrar_venta,
    registrar_venta_lote,
    obtener_ventas_dia,
    obtener_resumen_ventas_dia,
    obtener_resumen_ventas_por_responsable,
    obtener_total_ventas_dia,
    obtener_vendido_dia_producto,
    obtener_ventas_rango,
    obtener_totales_ventas_rango,
    obtener_serie_ventas_diarias,
    obtener_resumen_productos_rango,
    obtener_resumen_medios_pago_rango,
    obtener_serie_medios_pago_diaria_rango,
    obtener_serie_ventas_horaria_rango,
    obtener_resumen_por_dia_semana,
    obtener_arqueo_caja_activo,
    abrir_arqueo_caja,
    cerrar_arqueo_caja,
    obtener_historial_arqueos,
    obtener_arqueos_rango,
    obtener_movimientos_caja,
    obtener_movimientos_caja_rango,
    obtener_resumen_caja_dia,
    registrar_movimiento_caja,
    obtener_codigo_verificacion_caja,
    guardar_codigo_verificacion_caja,
    obtener_mesas,
    obtener_mesa,
    agregar_mesa,
    actualizar_mesa,
    activar_mesa,
    desactivar_mesa,
    eliminar_mesa,
    crear_pedido,
    actualizar_pedido,
    obtener_pedidos,
    obtener_pedidos_con_detalle,
    obtener_pedidos_con_detalle_paginados,
    obtener_pedido,
    crear_comanda_desde_pedido,
    obtener_comanda,
    obtener_comandas_por_pedido,
    obtener_items_comanda,
    marcar_comanda_impresa,
    marcar_comanda_reimpresa,
    build_documento_payload_desde_encargo,
    build_documento_payload_desde_pedido,
    build_documento_payload_desde_venta,
    crear_documento_emitido,
    generar_consecutivo_documento,
    marcar_documento_impreso,
    marcar_documento_reimpreso,
    obtener_documento_emitido,
    obtener_documentos_por_origen,
    obtener_documentos_recientes,
    obtener_documentos_recientes_paginados,
    obtener_envios_documento,
    obtener_trazabilidad_pedido,
    obtener_pedido_activo_mesa,
    obtener_pedido_activo_mesa_mesero,
    cambiar_estado_pedido,
    dividir_pedido_y_cobrar,
    pagar_pedido,
    validar_items_contra_produccion_panaderia,
    validar_stock_pedido,
    obtener_stock_disponible_hoy,
    obtener_stock_operativo_detalle,
    obtener_resumen_mesas,
    obtener_adicionales,
    agregar_adicional,
    actualizar_adicional_detalle,
    actualizar_adicional,
    eliminar_adicional,
    guardar_configuracion_adicional,
    crear_encargo,
    obtener_encargos,
    obtener_encargo,
    actualizar_estado_encargo,
    eliminar_encargo,
    unificar_pedidos,
    obtener_insumos,
    agregar_insumo,
    actualizar_stock,
    eliminar_insumo,
    obtener_insumos_bajo_stock,
    obtener_receta,
    guardar_receta,
    obtener_consumo_diario,
    obtener_estadisticas_pedidos,
    _consumo_producto,
    es_categoria_panaderia,
    crear_cliente,
    obtener_clientes,
    obtener_cliente,
    actualizar_cliente,
    obtener_historial_cliente,
    crear_encargo_v2,
    actualizar_encargo,
    actualizar_estado_encargo_v2,
    registrar_pago_encargo,
    obtener_pagos_encargo,
    obtener_encargos_v2,
    obtener_encargo_v2,
    obtener_cuenta_por_cobrar,
    obtener_cuentas_por_cobrar,
    obtener_cuentas_por_cobrar_paginadas,
    obtener_resumen_cartera,
    registrar_abono_cuenta,
    crear_venta_header,
    actualizar_items_venta,
    actualizar_comprador_venta,
    actualizar_cliente_venta,
    actualizar_cliente_pedido,
    registrar_pago_venta,
    cerrar_venta,
    suspender_venta,
    reanudar_venta,
    anular_venta,
    obtener_ventas_suspendidas,
    obtener_venta_header,
    registrar_envio_documento,
    obtener_session_version_usuario,
)
from backup import (
    crear_backup,
    listar_backups,
    restaurar_backup,
    eliminar_backup,
    limpiar_backups_antiguos,
    obtener_info_backup,
)
from logic.pronostico import (
    calcular_pronostico,
    calcular_eficiencia,
    analizar_tendencia,
    obtener_historial_pronostico,
    obtener_resumen_pronostico_por_dia_semana,
    calcular_backtesting,
    obtener_encargos_confirmados_para_fecha,
    generar_lectura_operativa,
    TIPO_DIA,
)
from app.context import SedeContext, SubscriptionContext, TenantContext, TerminalContext
from app.tenant_service import TenantService, TenantSuspendedError, SubscriptionExpiredError
from app.logging_utils import configure_app_logging, generate_request_id
from app.responses import json_error, wants_json_response
from app.security import CSRF_SESSION_KEY, PLATFORM_ADMIN_ROLE, VALID_ROLES
from app.web.auth import auth_bp
from app.web.decorators import (
    admin_required,
    login_required,
    roles_required,
    sede_scope_required,
    tenant_scope_required,
)
from app.web.utils import (
    COLORES_PROD,
    ICONOS_CATEGORIA,
    MENUS_PREFERIDOS,
    _MAX_ATTEMPTS,
    _LOCKOUT_MINUTES,
    _build_brand_context,
    _client_ip,
    _crear_notificacion,
    _csrf_invalid_response,
    _current_csrf_token,
    _iso_datetime,
    _nombre_usuario_actual,
    _normalizar_texto,
    _obtener_adicionales_operativos,
    _parse_fecha_iso,
    _panaderia_actual_id,
    _pedido_visible_para_usuario,
    _registrar_sesion,
    _resolver_filtro_historial,
    _rol_es_admin,
    _rol_usuario_actual,
    _sede_actual_id,
    _usuario_actual,
    _usuario_actual_id,
    _usuario_puede_registrar_produccion,
    color_prod,
    icono,
    icono_categoria,
)

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
configure_app_logging(app)

# ── Seguridad: secret key desde variable de entorno ──────────────────────────
_secret_key = os.environ.get("FLASK_SECRET_KEY", "").strip()
if not _secret_key or _secret_key == "cambia-esto-por-una-clave-aleatoria-segura":
    import warnings
    warnings.warn(
        "ADVERTENCIA DE SEGURIDAD: FLASK_SECRET_KEY no configurada. "
        "Usando clave temporal generada. Configura FLASK_SECRET_KEY en producción.",
        stacklevel=1,
    )
    _secret_key = secrets.token_hex(32)
app.secret_key = _secret_key

# ── Configuración de sesión ────────────────────────────────────────────────────
_SESSION_HOURS = int(os.environ.get("SESSION_LIFETIME_HOURS", "8"))
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=_SESSION_HOURS)
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on", "si", "sí"}


_session_cookie_secure = _env_bool(
    "SESSION_COOKIE_SECURE",
    default=(
        _env_bool("FORCE_HTTPS")
        or _env_bool("COOKIE_SECURE")
        or str(os.environ.get("PREFERRED_URL_SCHEME", "") or "").strip().lower() == "https"
        or bool(os.environ.get("RAILWAY_ENVIRONMENT"))
    ),
)
app.config['SESSION_COOKIE_SECURE'] = _session_cookie_secure
app.logger.info(
    "SESSION_COOKIE_SECURE configurado en %s (override=%s, preferred_scheme=%s, railway=%s)",
    app.config["SESSION_COOKIE_SECURE"],
    os.environ.get("SESSION_COOKIE_SECURE"),
    os.environ.get("PREFERRED_URL_SCHEME"),
    bool(os.environ.get("RAILWAY_ENVIRONMENT")),
)


def _safe_display_number(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_pagination_args(default_size: int = 50, max_size: int = 100) -> dict:
    try:
        page = int(request.args.get("page", 1) or 1)
    except (TypeError, ValueError):
        page = 1
    try:
        size = int(request.args.get("size", default_size) or default_size)
    except (TypeError, ValueError):
        size = default_size
    return {
        "page": max(page, 1),
        "size": max(1, min(size, max_size)),
    }


def _build_pagination_links(endpoint: str, pagination: dict, **params) -> dict:
    page = int((pagination or {}).get("page", 1) or 1)
    size = int((pagination or {}).get("size", 50) or 50)
    prev_page = pagination.get("prev_page") if pagination else None
    next_page = pagination.get("next_page") if pagination else None
    return {
        "prev": url_for(endpoint, **params, page=prev_page, size=size) if prev_page else None,
        "next": url_for(endpoint, **params, page=next_page, size=size) if next_page else None,
        "current": url_for(endpoint, **params, page=page, size=size),
    }


def _log_event(evento: str, level: str = "info", **fields) -> None:
    payload = " ".join(
        f"{key}={json.dumps(value, ensure_ascii=True)}"
        for key, value in fields.items()
        if value not in (None, "", [], {})
    )
    getattr(app.logger, level, app.logger.info)(f"{evento} {payload}".strip())


def _log_exception(evento: str, exc: Exception, **fields) -> None:
    payload = " ".join(
        f"{key}={json.dumps(value, ensure_ascii=True)}"
        for key, value in fields.items()
        if value not in (None, "", [], {})
    )
    app.logger.error(
        f"{evento} {payload}".strip(),
        exc_info=(type(exc), exc, exc.__traceback__),
    )


def _format_display_number(value, decimals=0):
    number = _safe_display_number(value)
    pattern = f"{{:,.{max(0, int(decimals))}f}}"
    return pattern.format(number)


@app.template_filter("display_integer")
def display_integer_filter(value):
    return _format_display_number(value, 0)


@app.template_filter("display_decimal1")
def display_decimal1_filter(value):
    return _format_display_number(value, 1)

def _resolver_contextos_request() -> tuple:
    session_user = _usuario_actual()
    session_panaderia_id = session_user.get("panaderia_id")
    session_sede_id = session_user.get("sede_id")
    rol = str(session_user.get("rol", "") or "")
    tenant_context = TenantService.resolve_current_panaderia()

    # La instalación opera con una sola panadería. Si una sesión antigua apunta
    # a otro tenant, se invalida para evitar seguir usando resolución SaaS.
    if (
        session_panaderia_id
        and rol != PLATFORM_ADMIN_ROLE
        and int(session_panaderia_id) != int(tenant_context.id)
    ):
        session.clear()
        session_user = {}
        session_sede_id = None
        rol = ""

    tenant_context = TenantContext(
        id=tenant_context.id,
        slug=tenant_context.slug,
        nombre=tenant_context.nombre,
        activa=tenant_context.activa,
        is_platform=rol == "platform_superadmin",
        estado_operativo=tenant_context.estado_operativo,
    )

    # Resolver sede: primero desde la sesión del usuario, luego la principal
    sede_context: SedeContext | None = None
    if session_sede_id:
        sede_context = TenantService.resolve_sede_by_id(int(session_sede_id))
        if sede_context is not None and int(sede_context.panaderia_id or 0) != int(tenant_context.id):
            sede_context = None
    if sede_context is None:
        sede_context = TenantService.resolve_sede_default(tenant_context.id)

    subscription_context = TenantService.get_subscription(tenant_context.id)

    # Resolver terminal desde sesión (login operativo la guarda en session)
    terminal_context: TerminalContext | None = None
    session_terminal_id = session_user.get("terminal_id")
    if session_terminal_id:
        terminal_context = TenantService.resolve_terminal_by_id(int(session_terminal_id))

    return tenant_context, sede_context, TenantService.get_branding(tenant_context.id), subscription_context, terminal_context




def _obtener_ventas_pos_operaciones(fecha: str | None = None, limite: int = 12) -> list[dict]:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    limite = max(1, min(int(limite or 12), 40))

    with get_connection() as conn:
        grupos = [
            dict(row) for row in conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id) AS grupo_id,
                    MAX(fecha) AS fecha,
                    MAX(hora) AS hora,
                    MAX(registrado_por) AS cajero,
                    MAX(metodo_pago) AS metodo_pago,
                    MAX(monto_recibido) AS monto_recibido,
                    MAX(cambio) AS cambio,
                    COALESCE(SUM(cantidad), 0) AS unidades,
                    COALESCE(SUM(total), 0.0) AS total
                FROM ventas
                WHERE fecha = ?
                  AND COALESCE(NULLIF(referencia_tipo, ''), 'pos') != 'pedido'
                GROUP BY COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)
                ORDER BY MAX(hora) DESC, MAX(id) DESC
                LIMIT ?
                """,
                (fecha, limite),
            ).fetchall()
        ]

        if not grupos:
            return []

        grupo_ids = [row["grupo_id"] for row in grupos]
        placeholders = ",".join("?" * len(grupo_ids))
        items_rows = conn.execute(
            f"""
            SELECT
                COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id) AS grupo_id,
                producto,
                cantidad,
                total
            FROM ventas
            WHERE fecha = ?
              AND COALESCE(NULLIF(referencia_tipo, ''), 'pos') != 'pedido'
              AND COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id) IN ({placeholders})
            ORDER BY hora DESC, id ASC
            """,
            [fecha, *grupo_ids],
        ).fetchall()

    items_por_grupo: dict[str, list[dict]] = defaultdict(list)
    for row in items_rows:
        item = dict(row)
        items_por_grupo[item["grupo_id"]].append({
            "producto": item["producto"],
            "cantidad": int(item["cantidad"] or 0),
            "total": float(item["total"] or 0),
        })

    resultado = []
    for row in grupos:
        items = items_por_grupo.get(row["grupo_id"], [])
        codigo = str(row["grupo_id"] or "").strip()
        if codigo.startswith("venta-"):
            codigo = "#" + codigo.split("-", 1)[-1][-6:].upper()
        elif codigo.startswith("legacy-"):
            codigo = "Venta " + codigo.split("-", 1)[-1]
        preview = ", ".join(
            f"{item['cantidad']}x {item['producto']}" for item in items[:2]
        )
        if len(items) > 2:
            preview += f" y {len(items) - 2} mas"
        resultado.append({
            "venta_grupo": row["grupo_id"],
            "codigo": codigo,
            "fecha": row["fecha"],
            "hora": row["hora"],
            "time_iso": _iso_datetime(row["fecha"], row["hora"]),
            "cajero": str(row["cajero"] or "").strip() or "Caja",
            "metodo_pago": str(row["metodo_pago"] or "").strip() or "efectivo",
            "monto_recibido": float(row["monto_recibido"] or 0),
            "cambio": float(row["cambio"] or 0),
            "unidades": int(row["unidades"] or 0),
            "total": float(row["total"] or 0),
            "items": items,
            "items_preview": preview,
        })
    return resultado


def _obtener_notificaciones_operativas(rol: str, usuario: str = "", limite: int = 30) -> list[dict]:
    rol = str(rol or "").strip()
    usuario = str(usuario or "").strip()
    limite = max(1, min(int(limite or 30), 60))
    hoy = datetime.now().strftime("%Y-%m-%d")
    corte = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    items: list[dict] = []

    with get_connection() as conn:
        historial_rows = conn.execute(
            """
            SELECT
                h.id,
                h.pedido_id,
                h.estado,
                h.cambiado_en,
                h.cambiado_por,
                h.detalle,
                p.total,
                p.mesero,
                p.metodo_pago,
                p.pagado_por,
                m.numero AS mesa_numero
            FROM pedido_estado_historial h
            JOIN pedidos p ON p.id = h.pedido_id
            LEFT JOIN mesas m ON m.id = p.mesa_id
            WHERE h.cambiado_en >= ?
              AND h.estado IN ('pendiente', 'listo', 'pagado', 'cancelado')
            ORDER BY h.cambiado_en DESC, h.id DESC
            LIMIT 120
            """,
            (corte,),
        ).fetchall()

        for row in historial_rows:
            data = dict(row)
            estado = str(data.get("estado", "") or "").strip()
            detalle = str(data.get("detalle", "") or "").strip()
            detalle_lower = detalle.lower()
            detalle_norm = _normalizar_texto(detalle)
            mesero = str(data.get("mesero", "") or "").strip()
            actor = str(data.get("cambiado_por", "") or "").strip()
            mesa_numero = data.get("mesa_numero")
            mesa_label = f"Mesa {mesa_numero}" if mesa_numero else "Mostrador"
            total_label = f"${_format_display_number(data.get('total', 0), 0)}"
            event_time = _iso_datetime(data.get("cambiado_en"))
            es_actualizacion = "actualizacion" in detalle_norm or "editado" in detalle_norm
            motivo_actualizacion = ""
            if "motivo:" in detalle_lower:
                motivo_actualizacion = detalle.split("Motivo:", 1)[-1].strip()

            if estado in ("pendiente", "listo") and actor and actor == mesero:
                if "cuenta dividida" in detalle_lower or rol not in ("panadero", "cajero"):
                    continue
                title = "Pedido actualizado" if "editado" in detalle_lower else "Nuevo pedido de mesa"
                description = f"{mesa_label} · {mesero or 'Mesero'} · {total_label}"
                items.append({
                    "_sort": event_time,
                    **_crear_notificacion(
                        notif_id=f"pedido-evento-{data['id']}",
                        title=title,
                        description=description,
                        notif_type="order",
                        when_iso=event_time,
                        sound="order",
                    ),
                })
                continue

            if estado == "listo" and es_actualizacion and rol == "panadero":
                description = f"{mesa_label} · {actor or 'Caja'} · {total_label}"
                if motivo_actualizacion:
                    description = f"{description} · {motivo_actualizacion}"
                items.append({
                    "_sort": event_time,
                    **_crear_notificacion(
                        notif_id=f"pedido-evento-{data['id']}",
                        title="Pedido ajustado por caja",
                        description=description,
                        notif_type="order",
                        when_iso=event_time,
                        sound="order",
                    ),
                })
                continue

            if estado == "pagado":
                if rol == "panadero":
                    responsable = str(data.get("pagado_por", "") or actor or "Caja").strip() or "Caja"
                    metodo_pago = str(data.get("metodo_pago", "") or "").strip()
                    metodo_label = metodo_pago.capitalize() if metodo_pago else "Pago registrado"
                    description = f"{mesa_label} · {responsable} · {metodo_label} · {total_label}"
                    items.append({
                        "_sort": event_time,
                        **_crear_notificacion(
                            notif_id=f"pedido-evento-{data['id']}",
                            title="Pedido cobrado",
                            description=description,
                            notif_type="success",
                            when_iso=event_time,
                        ),
                    })
                elif rol == "mesero" and mesero and mesero == usuario:
                    items.append({
                        "_sort": event_time,
                        **_crear_notificacion(
                            notif_id=f"pedido-evento-{data['id']}",
                            title="Tu pedido fue cobrado",
                            description=f"{mesa_label} · {total_label}",
                            notif_type="success",
                            when_iso=event_time,
                        ),
                    })
                continue

            if estado == "cancelado":
                if rol == "panadero":
                    description = f"{mesa_label} · {actor or 'Operacion'}"
                    if detalle:
                        description = f"{description} · {detalle}"
                    items.append({
                        "_sort": event_time,
                        **_crear_notificacion(
                            notif_id=f"pedido-evento-{data['id']}",
                            title="Pedido cancelado",
                            description=description,
                            notif_type="alert",
                            when_iso=event_time,
                            sound="alert",
                        ),
                    })
                elif rol == "mesero" and mesero and mesero == usuario:
                    items.append({
                        "_sort": event_time,
                        **_crear_notificacion(
                            notif_id=f"pedido-evento-{data['id']}",
                            title="Pedido cancelado",
                            description=detalle or f"{mesa_label} · Pedido cancelado",
                            notif_type="alert",
                            when_iso=event_time,
                            sound="alert",
                        ),
                    })

        if rol == "panadero":
            agotados_rows = conn.execute(
                """
                SELECT
                    rd.producto,
                    rd.producido,
                    rd.vendido,
                    MAX(v.hora) AS ultima_hora
                FROM registros_diarios rd
                LEFT JOIN ventas v
                  ON v.fecha = rd.fecha
                 AND v.producto = rd.producto
                WHERE rd.fecha = ?
                  AND COALESCE(rd.producido, 0) > 0
                  AND COALESCE(rd.vendido, 0) >= COALESCE(rd.producido, 0)
                GROUP BY rd.producto, rd.producido, rd.vendido
                ORDER BY COALESCE(MAX(v.hora), '00:00:00') DESC, rd.producto ASC
                LIMIT 40
                """,
                (hoy,),
            ).fetchall()

            for row in agotados_rows:
                data = dict(row)
                producto = str(data.get("producto", "") or "").strip()
                producto_id = re.sub(r"[^a-z0-9]+", "-", _normalizar_texto(producto)).strip("-") or "producto"
                hora_evento = str(data.get("ultima_hora", "") or "").strip() or datetime.now().strftime("%H:%M:%S")
                total_producido = int(data.get("producido", 0) or 0)
                total_vendido = int(data.get("vendido", 0) or 0)
                items.append({
                    "_sort": _iso_datetime(hoy, hora_evento),
                    **_crear_notificacion(
                        notif_id=f"stock-agotado-{hoy}-{producto_id}",
                        title="Se vendio todo el producido",
                        description=f"{producto} · {total_vendido}/{total_producido} unidades vendidas",
                        notif_type="stock",
                        when_iso=_iso_datetime(hoy, hora_evento),
                        sound="alert",
                    ),
                })

    items.sort(key=lambda item: str(item.get("_sort", "")), reverse=True)
    return [
        {key: value for key, value in item.items() if key != "_sort"}
        for item in items[:limite]
    ]


@app.before_request
def _resolve_request_context():
    g.request_id = generate_request_id()
    g.client_ip = _client_ip()
    g.request_started_at = time.perf_counter()
    tenant_context, sede_context, brand_context, subscription_context, terminal_context = _resolver_contextos_request()
    g.tenant_context = tenant_context
    g.sede_context = sede_context
    g.brand_context = brand_context
    g.subscription_context = subscription_context
    g.terminal_context = terminal_context
    g.csrf_token = _current_csrf_token()
    set_query_context(tenant_context.id, sede_context.id)

    # Registrar actividad del terminal activo
    if terminal_context is not None and terminal_context.available:
        TenantService.touch_terminal(terminal_context.id)

    session_user = session.get("usuario", {})
    session_panaderia_id = session_user.get("panaderia_id")
    session_rol = str(session_user.get("rol", "") or "")
    _is_platform_admin = session_rol == "platform_superadmin"

    # platform_superadmin opera sin tenant/sede fijos — saltear ambas guardas
    if not _is_platform_admin:
        # Bloquear acceso si el tenant está suspendido
        if session_panaderia_id and not tenant_context.is_active:
            try:
                TenantService.assert_tenant_active(tenant_context)
            except TenantSuspendedError:
                abort(503)
        # Bloquear acceso si la suscripción venció
        if session_panaderia_id and subscription_context.is_expired:
            try:
                TenantService.assert_subscription_active(subscription_context)
            except SubscriptionExpiredError:
                abort(402)

    # Invalidación server-side: si session_version en sesión no coincide con el de la BD,
    # la sesión fue revocada (usuario desactivado, membresía eliminada, etc.).
    if "usuario" in session and request.endpoint not in ("auth.login", "auth.logout", "static"):
        _uid = session["usuario"].get("id")
        _sv_session = session["usuario"].get("session_version", 0)
        if _uid:
            _sv_db = obtener_session_version_usuario(int(_uid))
            if _sv_db >= 0 and int(_sv_session or 0) != _sv_db:
                session.clear()
                if wants_json_response():
                    return json_error("Sesión expirada. Vuelve a iniciar sesión.", 401)
                return redirect(url_for("auth.login"))

    # El endpoint /login crea/reemplaza la sesión por sí mismo; no necesita CSRF.
    if request.endpoint != "auth.login" and \
            request.method in {"POST", "PUT", "PATCH", "DELETE"} and "usuario" in session:
        csrf_token = str(request.headers.get("X-CSRF-Token", "") or "").strip()
        if not csrf_token and request.form:
            csrf_token = str(request.form.get("_csrf_token", "") or "").strip()
        if csrf_token != session.get(CSRF_SESSION_KEY):
            return _csrf_invalid_response()


@app.after_request
def _set_security_headers(response):
    duration_ms = None
    started_at = getattr(g, "request_started_at", None)
    if started_at is not None:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    response.headers["X-Request-Id"] = getattr(g, "request_id", "")
    if duration_ms is not None:
        response.headers["X-Response-Time-ms"] = str(duration_ms)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if request.is_secure:
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
    if duration_ms is not None and (
        request.method in {"POST", "PUT", "PATCH", "DELETE"}
        or response.status_code >= 400
        or duration_ms >= 1000
    ):
        _log_event(
            "request_complete",
            request_id=getattr(g, "request_id", ""),
            method=request.method,
            path=request.path,
            status=response.status_code,
            duration_ms=duration_ms,
            usuario=_nombre_usuario_actual(),
            panaderia_id=_panaderia_actual_id(),
            sede_id=_sede_actual_id(),
        )
    return response


def _etiqueta_modelo(modelo: str) -> str:
    texto = str(modelo or "").strip().replace("_", " ")
    return texto.capitalize() if texto else "--"


def _obtener_registro_diario_producto(fecha: str, producto: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute("""
            SELECT fecha, producto, producido, vendido, sobrante,
                   sobrante_inicial, observaciones
            FROM registros_diarios
            WHERE fecha = ? AND producto = ?
        """, (fecha, producto)).fetchone()
    return dict(row) if row else None


def _sobrante_dia_anterior(fecha: str, producto: str) -> int:
    """Retorna el sobrante efectivo del ultimo registro previo disponible."""
    with get_connection() as conn:
        row = conn.execute("""
            SELECT sobrante_inicial, producido, vendido
            FROM registros_diarios
            WHERE fecha < ? AND producto = ?
            ORDER BY fecha DESC, id DESC
            LIMIT 1
        """, (fecha, producto)).fetchone()
    if not row:
        return 0
    return max(
        int(row["sobrante_inicial"] or 0)
        + int(row["producido"] or 0)
        - int(row["vendido"] or 0),
        0,
    )


def _combinar_observaciones(base: str = "", extra: str = "") -> str:
    base_texto = str(base or "").strip()
    extra_texto = str(extra or "").strip()
    if not base_texto:
        return extra_texto
    if not extra_texto or extra_texto in base_texto:
        return base_texto
    return f"{base_texto} | {extra_texto}"


def _guardar_lote_produccion(fecha: str, producto: str, cantidad: int,
                             observaciones: str = "") -> bool:
    cantidad_int = max(int(cantidad or 0), 0)
    if cantidad_int <= 0:
        return False

    previo = _obtener_registro_diario_producto(fecha, producto) or {}
    es_nuevo = not previo
    producido_actual = int(previo.get("producido", 0) or 0)
    vendido_registro = int(previo.get("vendido", 0) or 0)
    vendido_real = int(obtener_vendido_dia_producto(fecha, producto) or 0)
    vendido_actual = max(vendido_registro, vendido_real)
    observaciones_finales = _combinar_observaciones(previo.get("observaciones", ""), observaciones)

    # Solo calcular sobrante_inicial cuando se crea el registro del dia por primera vez
    sobrante_ini = int(previo.get("sobrante_inicial", 0) or 0) if not es_nuevo \
        else _sobrante_dia_anterior(fecha, producto)

    return guardar_registro(
        fecha,
        producto,
        producido_actual + cantidad_int,
        vendido_actual,
        observaciones_finales,
        sobrante_inicial=sobrante_ini,
    )


def _construir_contexto_produccion_producto(
    fecha: str,
    producto: str,
    stock_detalle_map: dict[str, dict] | None = None,
) -> dict:
    fecha_str = _parse_fecha_iso(fecha)
    registro = _obtener_registro_diario_producto(fecha_str, producto) or {}
    stock_detalle = (stock_detalle_map or obtener_stock_operativo_detalle(fecha_str)).get(producto, {})
    producido_actual = int(stock_detalle.get("producido_hoy", registro.get("producido", 0)) or 0)
    sobrante_inicial_actual = int(stock_detalle.get("sobrante_inicial_hoy", registro.get("sobrante_inicial", 0)) or 0)
    vendido_actual = int(
        stock_detalle.get(
            "vendido_operativo_hoy",
            obtener_vendido_dia_producto(fecha_str, producto) or registro.get("vendido", 0) or 0,
        ) or 0
    )
    disponible_actual = int(
        stock_detalle.get(
            "disponible_bruto",
            max(sobrante_inicial_actual + producido_actual - vendido_actual, 0),
        ) or 0
    )
    stock_total = max(sobrante_inicial_actual + producido_actual, 0)

    encargos_confirmados = obtener_encargos_confirmados_para_fecha(producto, fecha_str)
    resultado = calcular_pronostico(
        producto, fecha_objetivo=fecha_str,
        stock_actual=disponible_actual,
        encargos_confirmados=encargos_confirmados,
    )
    detalles = getattr(resultado, "detalles", {}) or {}
    ajuste = obtener_ajuste_pronostico(fecha_str, producto) or {}

    sugerido = int(resultado.produccion_sugerida or 0)
    meta_operativa = int(ajuste.get("ajustado") or sugerido)
    restante_meta = max(meta_operativa - producido_actual, 0)
    cumplimiento_pct = round((producido_actual / meta_operativa) * 100, 1) if meta_operativa > 0 else 0.0
    faltante_actual = max(vendido_actual - stock_total, 0)
    sobrante_actual = max(disponible_actual, 0)

    lotes = []
    if sobrante_inicial_actual > 0:
        lotes.append({
            "producto": producto,
            "cantidad": sobrante_inicial_actual,
            "observaciones": "Sobrante arrastrado desde el ultimo registro disponible",
            "registrado_en": f"{fecha_str} 00:00:00",
            "registrado_por": "Sistema",
        })
    if producido_actual > 0 or registro.get("observaciones"):
        lotes.append({
            "producto": producto,
            "cantidad": producido_actual,
            "observaciones": registro.get("observaciones") or "Acumulado del dia",
            "registrado_en": f"{fecha_str} 00:00:00",
            "registrado_por": "",
        })

    return {
        "ok": True,
        "fecha": fecha_str,
        "producto": producto,
        "sugerencia": {
            "sugerido": sugerido,
            "demanda_estimada": int(resultado.demanda_estimada or 0),
            "demanda_comprometida": int(resultado.demanda_comprometida or 0),
            "produccion_recomendada": int(resultado.produccion_recomendada or 0),
            "promedio": float(resultado.promedio_ventas or 0),
            "modelo": resultado.modelo_usado,
            "modelo_label": _etiqueta_modelo(resultado.modelo_usado),
            "confianza": resultado.confianza,
            "mensaje": resultado.mensaje,
            "tendencia": detalles.get("tendencia", "sin datos"),
            "dia_objetivo": detalles.get("dia_objetivo", fecha_str),
        },
        "meta": {
            "sugerido": sugerido,
            "operativa": meta_operativa,
            "origen": "ajuste_manual" if ajuste else "sugerido_sistema",
            "motivo": ajuste.get("motivo", "") if ajuste else "",
            "registrado_por": ajuste.get("registrado_por", "") if ajuste else "",
        },
        "avance": {
            "sobrante_inicial_actual": sobrante_inicial_actual,
            "stock_total": stock_total,
            "producido_actual": producido_actual,
            "vendido_actual": vendido_actual,
            "disponible_actual": disponible_actual,
            "comprometido_actual": int(stock_detalle.get("comprometido_hoy", 0) or 0),
            "restante_meta": restante_meta,
            "cumplimiento_pct": cumplimiento_pct,
            "faltante_actual": faltante_actual,
            "sobrante_actual": sobrante_actual,
        },
        "lotes": lotes,
    }


def _construir_contexto_produccion_masivo(fecha: str) -> dict:
    fecha_str = _parse_fecha_iso(fecha)
    stock_detalle = obtener_stock_operativo_detalle(fecha_str)
    items = []
    for producto in obtener_productos_panaderia():
        contexto = _construir_contexto_produccion_producto(fecha_str, producto, stock_detalle_map=stock_detalle)
        sugerencia = contexto["sugerencia"]
        meta = contexto["meta"]
        avance = contexto["avance"]
        items.append({
            "producto": producto,
            "sugerido": meta["sugerido"],
            "meta_operativa": meta["operativa"],
            "producido_actual": avance["producido_actual"],
            "vendido_actual": avance["vendido_actual"],
            "restante_meta": avance["restante_meta"],
            "disponible_actual": avance["disponible_actual"],
            "faltante_actual": avance["faltante_actual"],
            "sobrante_actual": avance["sobrante_actual"],
            "modelo_label": sugerencia["modelo_label"],
            "confianza": sugerencia["confianza"],
        })

    items.sort(key=lambda item: (-int(item["restante_meta"] or 0), item["producto"]))
    resumen = {
        "productos": len(items),
        "sugerido_total": sum(int(item["sugerido"] or 0) for item in items),
        "meta_total": sum(int(item["meta_operativa"] or 0) for item in items),
        "producido_total": sum(int(item["producido_actual"] or 0) for item in items),
        "restante_total": sum(int(item["restante_meta"] or 0) for item in items),
    }
    return {"ok": True, "fecha": fecha_str, "items": items, "resumen": resumen}


def _evaluar_insumos_lotes(lotes: list[dict]) -> dict:
    lotes_validos = []
    for lote in lotes or []:
        producto = str((lote or {}).get("producto", "") or "").strip()
        try:
            cantidad = max(int((lote or {}).get("cantidad", 0) or 0), 0)
        except (TypeError, ValueError):
            cantidad = 0
        if producto and cantidad > 0:
            lotes_validos.append({"producto": producto, "cantidad": cantidad})

    if not lotes_validos:
        return {
            "hay_riesgo": False,
            "criticos": [],
            "alertas": [],
            "productos_sin_receta": [],
            "productos_sin_rendimiento": [],
            "insumos": [],
        }

    insumos_catalogo = {int(item["id"]): item for item in obtener_insumos()}
    productos_sin_receta = []
    detalle_por_insumo: dict[int, dict] = {}

    with get_connection() as conn:
        for lote in lotes_validos:
            producto = lote["producto"]
            cantidad = lote["cantidad"]
            receta = obtener_receta(producto)
            if not receta.get("ingredientes") and not receta.get("componentes"):
                productos_sin_receta.append(producto)
                continue

            consumo = _consumo_producto(conn, producto, cantidad, incluir_panaderia=True)
            for insumo_id, datos in consumo.items():
                insumo = insumos_catalogo.get(int(insumo_id))
                if not insumo:
                    continue
                bucket = detalle_por_insumo.setdefault(int(insumo_id), {
                    "id": int(insumo_id),
                    "nombre": insumo["nombre"],
                    "unidad": insumo["unidad"],
                    "stock_actual": float(insumo["stock"] or 0),
                    "stock_minimo": float(insumo["stock_minimo"] or 0),
                    "requerido": 0.0,
                    "productos_map": {},
                })
                bucket["requerido"] += float(datos.get("cantidad", 0) or 0)
                bucket["productos_map"][producto] = bucket["productos_map"].get(producto, 0) + cantidad

    criticos = []
    alertas = []
    insumos = []
    for bucket in sorted(detalle_por_insumo.values(), key=lambda item: item["nombre"].lower()):
        requerido = round(float(bucket["requerido"] or 0), 3)
        stock_actual = round(float(bucket["stock_actual"] or 0), 3)
        stock_minimo = round(float(bucket["stock_minimo"] or 0), 3)
        disponible_post = round(stock_actual - requerido, 3)

        if disponible_post < 0:
            estado = "critico"
        elif requerido > 0 and disponible_post <= stock_minimo:
            estado = "alerta"
        else:
            estado = "ok"

        row = {
            "nombre": bucket["nombre"],
            "unidad": bucket["unidad"],
            "stock_actual": stock_actual,
            "requerido": requerido,
            "disponible_post": disponible_post,
            "stock_minimo": stock_minimo,
            "estado": estado,
            "deficit": round(abs(disponible_post), 3) if disponible_post < 0 else 0,
            "productos": [
                {"producto": producto, "cantidad": cantidad}
                for producto, cantidad in sorted(bucket["productos_map"].items())
            ],
        }
        insumos.append(row)
        if estado == "critico":
            criticos.append(row)
        elif estado == "alerta":
            alertas.append(row)

    return {
        "hay_riesgo": bool(criticos or alertas or productos_sin_receta),
        "criticos": criticos,
        "alertas": alertas,
        "productos_sin_receta": sorted(set(productos_sin_receta)),
        "productos_sin_rendimiento": [],
        "insumos": insumos,
    }


def _excel_col_to_index(ref_celda):
    letras = "".join(ch for ch in ref_celda if ch.isalpha()).upper()
    indice = 0
    for letra in letras:
        indice = (indice * 26) + (ord(letra) - 64)
    return max(indice - 1, 0)


def _leer_shared_strings(zip_xlsx):
    if "xl/sharedStrings.xml" not in zip_xlsx.namelist():
        return []

    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(zip_xlsx.read("xl/sharedStrings.xml"))
    valores = []
    for si in root.findall("main:si", ns):
        partes = [n.text or "" for n in si.findall(".//main:t", ns)]
        valores.append("".join(partes))
    return valores


def _resolver_primer_sheet(zip_xlsx):
    ns_main = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    ns_rel = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}

    workbook = ET.fromstring(zip_xlsx.read("xl/workbook.xml"))
    sheet = workbook.find("main:sheets/main:sheet", ns_main)
    if sheet is None:
        raise ValueError("El archivo no contiene hojas")

    rel_id = None
    for clave, valor in sheet.attrib.items():
        if clave.endswith("}id"):
            rel_id = valor
            break
    if not rel_id:
        raise ValueError("No se pudo identificar la hoja principal")

    rels = ET.fromstring(zip_xlsx.read("xl/_rels/workbook.xml.rels"))
    for rel in rels.findall("rel:Relationship", ns_rel):
        if rel.attrib.get("Id") == rel_id:
            target = rel.attrib.get("Target", "")
            if target.startswith("/"):
                return target.lstrip("/")
            if target.startswith("xl/"):
                return target
            return f"xl/{target}"

    raise ValueError("No se pudo resolver la hoja principal del archivo")


def _valor_celda_xlsx(celda, shared_strings, ns):
    tipo = celda.attrib.get("t")
    if tipo == "inlineStr":
        return "".join((n.text or "") for n in celda.findall(".//main:t", ns))

    valor = celda.findtext("main:v", default="", namespaces=ns)
    if tipo == "s" and valor != "":
        try:
            return shared_strings[int(valor)]
        except (ValueError, IndexError):
            return ""
    return valor


def _leer_filas_xlsx(contenido):
    ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    with zipfile.ZipFile(BytesIO(contenido)) as zip_xlsx:
        shared_strings = _leer_shared_strings(zip_xlsx)
        sheet_path = _resolver_primer_sheet(zip_xlsx)
        root = ET.fromstring(zip_xlsx.read(sheet_path))

    filas = []
    for fila in root.findall(".//main:sheetData/main:row", ns):
        celdas = {}
        for celda in fila.findall("main:c", ns):
            ref = celda.attrib.get("r", "")
            indice = _excel_col_to_index(ref)
            celdas[indice] = _valor_celda_xlsx(celda, shared_strings, ns)
        if celdas:
            max_idx = max(celdas)
            filas.append([celdas.get(i, "") for i in range(max_idx + 1)])
    return filas


def _leer_filas_csv(contenido):
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            texto = contenido.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("No se pudo leer el archivo CSV")

    return list(csv.reader(texto.splitlines()))


def _parsear_precio(valor):
    if valor is None:
        raise ValueError("Precio vacio")

    texto = str(valor).strip()
    if not texto:
        raise ValueError("Precio vacio")

    texto = re.sub(r"[^0-9,.\-]", "", texto)
    if not texto:
        raise ValueError("Precio invalido")

    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        partes = texto.split(",")
        if len(partes) == 2 and len(partes[1]) <= 2:
            texto = partes[0].replace(".", "") + "." + partes[1]
        else:
            texto = texto.replace(",", "")
    elif texto.count(".") > 1:
        texto = texto.replace(".", "")

    precio = float(texto)
    if precio < 0:
        raise ValueError("Precio negativo")
    return round(precio, 2)


def _parsear_numero_positivo(valor, etiqueta="Valor"):
    try:
        return _parsear_precio(valor)
    except ValueError as exc:
        mensaje = str(exc)
        mensaje = mensaje.replace("Precio", etiqueta).replace("precio", etiqueta.lower())
        raise ValueError(mensaje) from exc


def _parsear_surtido_tipo(valor) -> str:
    surtido_tipo = str(valor or "").strip().lower()
    return surtido_tipo if surtido_tipo in {"none", "sal", "dulce", "ambos"} else "none"


def _ordenar_menus_catalogo(menus: set[str]) -> list[str]:
    resto = sorted(menu for menu in menus if menu not in MENUS_PREFERIDOS)
    return [menu for menu in MENUS_PREFERIDOS if menu in menus] + resto


def _etiqueta_categoria_para_nombre(categoria: str) -> str:
    categoria = str(categoria or "").strip()
    categoria_norm = _normalizar_texto(categoria)
    if categoria_norm == "sandwiches croissant":
        return "Croissant"
    if categoria_norm == "sandwiches pan saludable":
        return "Pan Saludable"
    return categoria


def _compactar_catalogo_oficial(productos: list[dict]) -> list[dict]:
    if not productos:
        return []

    agrupados: dict[tuple[str, str], dict] = {}
    for producto in productos:
        nombre_base = str(producto.get("nombre_base", producto.get("nombre", "")) or "").strip()
        categoria = str(producto.get("categoria", "") or "").strip() or "Panaderia"
        menus_producto = {
            menu.strip()
            for menu in str(producto.get("menu", "") or "").split("|")
            if menu.strip()
        }
        clave = (_normalizar_texto(nombre_base), _normalizar_texto(categoria))
        existente = agrupados.get(clave)
        if existente is None:
            agrupados[clave] = {
                "nombre_base": nombre_base,
                "categoria": categoria,
                "precio": float(producto.get("precio", 0) or 0),
                "descripcion": str(producto.get("descripcion", "") or "").strip(),
                "menus": set(_ordenar_menus_catalogo(menus_producto)),
                "es_adicional": bool(producto.get("es_adicional", False)),
                "es_panaderia": bool(producto.get("es_panaderia", es_categoria_panaderia(categoria))),
            }
            continue

        existente["precio"] = float(producto.get("precio", existente["precio"]) or 0)
        descripcion = str(producto.get("descripcion", "") or "").strip()
        if len(descripcion) > len(existente["descripcion"]):
            existente["descripcion"] = descripcion
        for menu_limpio in menus_producto:
            existente["menus"].add(menu_limpio)

    por_nombre: defaultdict[str, list[dict]] = defaultdict(list)
    for item in agrupados.values():
        por_nombre[_normalizar_texto(item["nombre_base"])].append(item)

    resultado = []
    for grupo in por_nombre.values():
        solo_panaderia = all(bool(item.get("es_panaderia")) for item in grupo)
        requiere_desambiguacion = len(grupo) > 1 and not solo_panaderia
        for item in grupo:
            nombre_final = item["nombre_base"]
            if requiere_desambiguacion:
                nombre_final = f"{item['nombre_base']} · {_etiqueta_categoria_para_nombre(item['categoria'])}"
            menus_ordenados = _ordenar_menus_catalogo(set(item["menus"]))
            resultado.append({
                "nombre": nombre_final,
                "precio": round(float(item["precio"] or 0), 2),
                "categoria": item["categoria"],
                "menu": "|".join(menus_ordenados),
                "descripcion": item["descripcion"],
                "es_adicional": bool(item["es_adicional"]),
                "es_panaderia": bool(item["es_panaderia"]),
            })

    return sorted(resultado, key=lambda item: (item["categoria"], item["nombre"]))


def _extraer_catalogo_productos(archivo):
    nombre_archivo = (archivo.filename or "").strip()
    if not nombre_archivo:
        raise ValueError("Selecciona un archivo")

    extension = nombre_archivo.rsplit(".", 1)[-1].lower() if "." in nombre_archivo else ""
    contenido = archivo.read()
    if not contenido:
        raise ValueError("El archivo esta vacio")
    if len(contenido) > 2 * 1024 * 1024:
        raise ValueError("El archivo supera el limite de 2 MB")

    try:
        if extension == "xlsx":
            filas = _leer_filas_xlsx(contenido)
        elif extension == "csv":
            filas = _leer_filas_csv(contenido)
        else:
            raise ValueError("Formato no soportado. Usa .xlsx o .csv")
    except (ValueError, zipfile.BadZipFile, ET.ParseError, csv.Error) as exc:
        raise ValueError(f"No se pudo leer el archivo: {exc}") from exc

    if not filas:
        raise ValueError("El archivo no contiene datos")

    while filas and not any(str(celda).strip() for celda in filas[0]):
        filas.pop(0)

    if not filas:
        raise ValueError("El archivo no contiene datos validos")

    encabezados = [_normalizar_texto(celda) for celda in filas[0]]
    alias_menu = {"menu", "menú", "franja", "seccion", "sección", "carta"}
    alias_nombre = {"nombre", "producto", "referencia", "item"}
    alias_precio = {"precio", "precio cop", "precio (cop)", "valor", "precio venta", "precio_venta", "precio unitario"}
    alias_categoria = {"categoria", "categoría", "categoria producto", "categoría producto", "tipo", "tipo producto"}
    alias_descripcion = {"descripcion", "descripción", "detalle", "observaciones"}
    alias_adicional = {"es adicional", "adicional", "puede ser adicional", "extra"}

    idx_menu = next((i for i, valor in enumerate(encabezados) if valor in alias_menu), None)
    idx_nombre = next((i for i, valor in enumerate(encabezados) if valor in alias_nombre), None)
    idx_precio = next((i for i, valor in enumerate(encabezados) if valor in alias_precio), None)
    idx_categoria = next((i for i, valor in enumerate(encabezados) if valor in alias_categoria), None)
    idx_descripcion = next((i for i, valor in enumerate(encabezados) if valor in alias_descripcion), None)
    idx_adicional = next((i for i, valor in enumerate(encabezados) if valor in alias_adicional), None)

    if idx_nombre is None or idx_precio is None:
        raise ValueError("El archivo debe tener columnas 'nombre' y 'precio'")

    productos = []
    errores = []

    for numero_fila, fila in enumerate(filas[1:], start=2):
        menu = str(fila[idx_menu]).strip() if idx_menu is not None and idx_menu < len(fila) else ""
        nombre = str(fila[idx_nombre]).strip() if idx_nombre < len(fila) else ""
        precio_raw = fila[idx_precio] if idx_precio < len(fila) else ""
        categoria = str(fila[idx_categoria]).strip() if idx_categoria is not None and idx_categoria < len(fila) else ""
        descripcion = str(fila[idx_descripcion]).strip() if idx_descripcion is not None and idx_descripcion < len(fila) else ""
        adicional_raw = str(fila[idx_adicional]).strip().lower() if idx_adicional is not None and idx_adicional < len(fila) else ""

        if not nombre and str(precio_raw).strip() == "":
            continue
        if not nombre:
            errores.append(f"Fila {numero_fila}: falta el nombre del producto")
            continue

        try:
            precio = _parsear_precio(precio_raw)
        except ValueError as exc:
            errores.append(f"Fila {numero_fila} ({nombre}): {exc}")
            continue

        productos.append({
            "nombre_base": nombre,
            "nombre": nombre,
            "precio": precio,
            "categoria": categoria or "Panaderia",
            "menu": menu,
            "descripcion": descripcion,
            "es_adicional": adicional_raw in {"1", "si", "sí", "true", "x", "extra", "adicional"},
            "es_panaderia": es_categoria_panaderia(categoria or "Panaderia"),
        })

    if not productos:
        raise ValueError("No se pudo importar ninguna fila valida")

    return _compactar_catalogo_oficial(productos), errores


def _extraer_catalogo_insumos(archivo):
    nombre_archivo = (archivo.filename or "").strip()
    if not nombre_archivo:
        raise ValueError("Selecciona un archivo")

    extension = nombre_archivo.rsplit(".", 1)[-1].lower() if "." in nombre_archivo else ""
    contenido = archivo.read()
    if not contenido:
        raise ValueError("El archivo esta vacio")
    if len(contenido) > 2 * 1024 * 1024:
        raise ValueError("El archivo supera el limite de 2 MB")

    try:
        if extension == "xlsx":
            filas = _leer_filas_xlsx(contenido)
        elif extension == "csv":
            filas = _leer_filas_csv(contenido)
        else:
            raise ValueError("Formato no soportado. Usa .xlsx o .csv")
    except (ValueError, zipfile.BadZipFile, ET.ParseError, csv.Error) as exc:
        raise ValueError(f"No se pudo leer el archivo: {exc}") from exc

    if not filas:
        raise ValueError("El archivo no contiene datos")

    while filas and not any(str(celda).strip() for celda in filas[0]):
        filas.pop(0)

    if not filas:
        raise ValueError("El archivo no contiene datos validos")

    encabezados = [_normalizar_texto(celda) for celda in filas[0]]
    alias_nombre = {"nombre", "insumo", "ingrediente", "producto"}
    alias_stock = {"stock", "cantidad", "existencia", "inventario"}
    alias_unidad = {"unidad", "medida", "unidad medida", "unidad_medida"}
    alias_minimo = {"stock minimo", "stock_minimo", "minimo", "min", "alerta minima", "alerta_minima"}

    idx_nombre = next((i for i, valor in enumerate(encabezados) if valor in alias_nombre), None)
    idx_stock = next((i for i, valor in enumerate(encabezados) if valor in alias_stock), None)
    idx_unidad = next((i for i, valor in enumerate(encabezados) if valor in alias_unidad), None)
    idx_minimo = next((i for i, valor in enumerate(encabezados) if valor in alias_minimo), None)

    if idx_nombre is None or idx_stock is None:
        raise ValueError("El archivo debe tener columnas 'nombre' y 'stock'")

    insumos = []
    errores = []

    for numero_fila, fila in enumerate(filas[1:], start=2):
        nombre = str(fila[idx_nombre]).strip() if idx_nombre < len(fila) else ""
        stock_raw = fila[idx_stock] if idx_stock < len(fila) else ""
        unidad = str(fila[idx_unidad]).strip() if idx_unidad is not None and idx_unidad < len(fila) else ""
        minimo_raw = fila[idx_minimo] if idx_minimo is not None and idx_minimo < len(fila) else ""

        if not nombre and str(stock_raw).strip() == "":
            continue
        if not nombre:
            errores.append(f"Fila {numero_fila}: falta el nombre del insumo")
            continue

        try:
            stock = _parsear_numero_positivo(stock_raw, "Stock")
            stock_minimo = None
            if str(minimo_raw).strip() != "":
                stock_minimo = _parsear_numero_positivo(minimo_raw, "Stock minimo")
        except ValueError as exc:
            errores.append(f"Fila {numero_fila} ({nombre}): {exc}")
            continue

        insumos.append({
            "nombre": nombre,
            "unidad": unidad or None,
            "stock": stock,
            "stock_minimo": stock_minimo,
        })

    if not insumos:
        raise ValueError("No se pudo importar ninguna fila valida")

    return insumos, errores


def _puede_gestionar_mesas() -> bool:
    return _rol_usuario_actual() in {"panadero", "tenant_admin", PLATFORM_ADMIN_ROLE}


def _registrar_auditoria_mesa(resultado: dict, accion: str, detalle: str) -> None:
    mesa = (resultado or {}).get("mesa") or {}
    mesa_antes = (resultado or {}).get("mesa_antes") or {}
    mesa_id = mesa.get("id") or mesa_antes.get("id")
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=_panaderia_actual_id(),
        sede_id=_sede_actual_id(),
        ip=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion=accion,
        entidad="mesa",
        entidad_id=str(mesa_id or ""),
        detalle=detalle,
        valor_antes=json.dumps(mesa_antes, ensure_ascii=True),
        valor_nuevo=json.dumps(mesa, ensure_ascii=True),
    )


def _pedido_pertenece_filtro_mesero(pedido: dict, filtro: str) -> bool:
    estado = str((pedido or {}).get("estado", "") or "").strip().lower()
    if filtro == "cancelado":
        return estado == "cancelado"
    if filtro == "pendiente":
        return estado in {"pendiente", "en_preparacion"}
    return estado == "listo"


def _meta_filtros_mesero(pedidos: list[dict], filtro_actual: str) -> list[dict]:
    filtros = [
        {
            "key": "listo",
            "label": "Listos para caja",
            "copy": "Pedidos que ya pueden pasar a cobro.",
        },
        {
            "key": "pendiente",
            "label": "Pendientes",
            "copy": "Incluye pedidos recibidos y en preparacion.",
        },
        {
            "key": "cancelado",
            "label": "Cancelados",
            "copy": "Pedidos anulados por operacion o por el cliente.",
        },
    ]
    for filtro in filtros:
        filtro["count"] = sum(1 for pedido in pedidos if _pedido_pertenece_filtro_mesero(pedido, filtro["key"]))
        filtro["active"] = filtro["key"] == filtro_actual
    return filtros


# ── Decoradores: importados de app.web.decorators ────────────────────────────
# login_required, tenant_scope_required, sede_scope_required,
# roles_required, admin_required — ver app/web/decorators.py


# ══════════════════════════════════════════════
# RUTAS DE PAGINAS
# ══════════════════════════════════════════════
# Rutas de autenticación (/, /login, /logout, /health, /ready,
# /favicon.ico, /cambiar-password) → app/web/auth.py (auth_bp)

@app.errorhandler(404)
def pagina_no_encontrada(e):
    if request.path.startswith('/api/'):
        return jsonify({"ok": False, "error": "Recurso no encontrado"}), 404
    return render_template("error.html", codigo=404, mensaje="Página no encontrada"), 404

@app.errorhandler(500)
def error_interno(e):
    exc = getattr(e, "original_exception", e)
    if isinstance(exc, Exception):
        _log_exception(
            "request_error",
            exc,
            request_id=getattr(g, "request_id", ""),
            method=request.method,
            path=request.path,
            usuario=_nombre_usuario_actual(),
        )
    else:
        _log_event(
            "request_error",
            level="error",
            request_id=getattr(g, "request_id", ""),
            method=request.method,
            path=request.path,
            error=str(e),
            usuario=_nombre_usuario_actual(),
        )
    if request.path.startswith('/api/'):
        return jsonify({"ok": False, "error": "Error interno del servidor"}), 500
    return render_template("error.html", codigo=500, mensaje="Error interno del servidor"), 500

# Login / logout / cambiar-password → auth_bp (app/web/auth.py)


# ── Cajero ──

@app.route("/cajero/pos")
@login_required
@roles_required("cajero", "tenant_admin", "platform_superadmin")
def cajero_pos():
    productos = obtener_productos_con_precio()
    categorias = obtener_categorias_producto()
    adicionales = _obtener_adicionales_operativos()
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])
    caja = obtener_resumen_caja_dia()
    return render_template("cajero_pos.html",
                           productos=productos,
                           categorias=categorias,
                           adicionales=adicionales,
                           caja=caja,
                           layout="cajero", active_page="pos")


@app.route("/cajero/ventas")
@login_required
@roles_required("cajero", "tenant_admin", "platform_superadmin")
def cajero_ventas():
    caja = obtener_resumen_caja_dia()
    historial_arqueos = obtener_historial_arqueos(8)
    movimientos_caja = obtener_movimientos_caja(limite=10)
    return render_template("dashboard_ventas.html",
                           caja=caja,
                           historial_arqueos=historial_arqueos,
                           movimientos_caja=movimientos_caja,
                           layout="cajero", active_page="ventas")


@app.route("/cajero/pedidos")
@login_required
@roles_required("cajero", "tenant_admin", "platform_superadmin")
def cajero_pedidos():
    pagination_args = _parse_pagination_args()
    pedidos_data = obtener_pedidos_con_detalle_paginados(
        page=pagination_args["page"],
        size=pagination_args["size"],
    )
    pedidos = pedidos_data["items"]
    pagination = pedidos_data["pagination"]
    for pedido in pedidos:
        pedido["comandas"] = obtener_comandas_por_pedido(int(pedido.get("id", 0) or 0))
        pedido["ultima_comanda"] = pedido["comandas"][0] if pedido["comandas"] else None
        pedido["documentos"] = obtener_documentos_por_origen("pedido", int(pedido.get("id", 0) or 0))
        pedido["ultimo_documento"] = pedido["documentos"][0] if pedido["documentos"] else None
    caja = obtener_resumen_caja_dia()
    mesas_index: dict[int, dict] = {}
    for pedido in pedidos:
        mesa_numero = pedido.get("mesa_numero")
        if mesa_numero in (None, ""):
            continue
        key = int(mesa_numero)
        entry = mesas_index.setdefault(key, {
            "numero": key,
            "total": 0,
            "por_cobrar": 0,
        })
        entry["total"] += 1
        if str(pedido.get("estado", "") or "").strip() == "listo":
            entry["por_cobrar"] += 1
    mesas_filtro = [mesas_index[key] for key in sorted(mesas_index)]
    return render_template("cajero_pedidos.html",
                           pedidos=pedidos,
                           pagination=pagination,
                           pagination_links=_build_pagination_links("cajero_pedidos", pagination),
                           mesas_filtro=mesas_filtro,
                           caja=caja,
                           layout="cajero", active_page="pedidos")


@app.route("/cajero/pedido/<int:pedido_id>/editar")
@login_required
@roles_required("cajero", "tenant_admin", "platform_superadmin")
def cajero_editar_pedido(pedido_id):
    if _rol_usuario_actual() != "cajero":
        flash("Solo caja puede editar pedidos", "error")
        return redirect(url_for("cajero_pedidos"))

    pedido = obtener_pedido(pedido_id)
    if not pedido:
        flash("Pedido no encontrado", "error")
        return redirect(url_for("cajero_pedidos"))
    if str(pedido.get("estado", "") or "").strip() in ("pagado", "cancelado"):
        flash("Ese pedido ya no se puede editar", "warning")
        return redirect(url_for("cajero_pedidos"))

    productos = obtener_productos_con_precio()
    categorias = obtener_categorias_producto()
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])

    mesa = {
        "id": pedido.get("mesa_id"),
        "numero": pedido.get("mesa_numero"),
        "nombre": pedido.get("mesa_nombre") or f"Mesa {pedido.get('mesa_numero') or '?'}",
    }

    return render_template(
        "mesero_pedido.html",
        mesa=mesa,
        productos=productos,
        categorias=categorias,
        adicionales=_obtener_adicionales_operativos(),
        pedido_editable=pedido,
        editor_role="cajero",
        pedido_page_title=f"Mesa {mesa['numero']} · Editar pedido",
        pedido_page_copy="Ajusta el pedido antes de cobrarlo. Cada cambio exige un motivo y queda notificado al admin.",
        pedido_submit_label="Guardar ajuste y regenerar comanda",
        pedido_return_url=url_for("cajero_pedidos"),
        pedido_return_label="Salir a pedidos",
        layout="cajero",
        active_page="pedidos",
    )


# ── Mesero ──

@app.route("/mesero/mesas")
@login_required
@roles_required("mesero", "tenant_admin", "platform_superadmin")
def mesero_mesas():
    mesas = obtener_resumen_mesas()
    menu_qr_options = [
        {
            "id": "manana",
            "label": "Menu de manana",
            "subtitle": "Desayunos",
            "url": "https://panaderiarichs.my.canva.site/men-desayunos-panader-a-rich-s",
        },
        {
            "id": "tardes",
            "label": "Menu de tardes",
            "subtitle": "Panaderia y onces",
            "url": "https://panaderiarichs.my.canva.site/men-de-tardes-panader-a-rich-s",
        },
    ]
    return render_template("mesero_mesas.html",
                           mesas=mesas,
                           menu_qr_options=menu_qr_options,
                           layout="mesero", active_page="mesas")


@app.route("/mesero/pedido/<int:mesa_id>")
@login_required
@roles_required("mesero", "tenant_admin", "platform_superadmin")
def mesero_pedido(mesa_id):
    productos = obtener_productos_con_precio()
    categorias = obtener_categorias_producto()
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])
    adicionales = _obtener_adicionales_operativos()
    mesas = obtener_mesas()
    mesa = next((m for m in mesas if m["id"] == mesa_id), None)
    if not mesa:
        flash("Mesa no encontrada", "error")
        return redirect(url_for("mesero_mesas"))
    return render_template("mesero_pedido.html",
                           mesa=mesa, productos=productos,
                           categorias=categorias,
                           adicionales=adicionales,
                           pedido_editable=None,
                           editor_role="mesero",
                           pedido_page_title=f"Mesa {mesa['numero']} - {mesa['nombre']}",
                           pedido_page_copy="Toca productos para crear el pedido de esta mesa. Si ya existe uno activo, se generara una nueva comanda completa para cocina.",
                           pedido_submit_label="Enviar a cocina",
                           pedido_return_url=url_for("mesero_mesas"),
                           pedido_return_label="Salir a mesas",
                           layout="mesero", active_page="mesas")


@app.route("/mesero/pedidos")
@login_required
@roles_required("mesero", "tenant_admin", "platform_superadmin")
def mesero_pedidos():
    filtro_estado = str(request.args.get("estado", "listo") or "listo").strip().lower()
    if filtro_estado not in {"listo", "pendiente", "cancelado"}:
        filtro_estado = "listo"

    pedidos_todos = obtener_pedidos_con_detalle(mesero=_nombre_usuario_actual())
    for pedido in pedidos_todos:
        pedido["comandas"] = obtener_comandas_por_pedido(int(pedido.get("id", 0) or 0))
        pedido["ultima_comanda"] = pedido["comandas"][0] if pedido["comandas"] else None
    pedidos = [pedido for pedido in pedidos_todos if _pedido_pertenece_filtro_mesero(pedido, filtro_estado)]
    mesas_index: dict[int, dict] = {}
    for pedido in pedidos:
        mesa_numero = pedido.get("mesa_numero")
        if mesa_numero in (None, ""):
            continue
        try:
            key = int(mesa_numero)
        except (TypeError, ValueError):
            continue
        entry = mesas_index.setdefault(key, {
            "numero": key,
            "total": 0,
        })
        entry["total"] += 1
    mesas_filtro = [mesas_index[key] for key in sorted(mesas_index)]
    filtros_estado = _meta_filtros_mesero(pedidos_todos, filtro_estado)
    return render_template("mesero_pedidos.html",
                           pedidos=pedidos,
                           filtro_estado=filtro_estado,
                           filtros_estado=filtros_estado,
                           mesas_filtro=mesas_filtro,
                           layout="mesero", active_page="pedidos")


# ── Panadero ──

@app.route("/panadero/pronostico")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_pronostico():
    productos = obtener_productos_panaderia()
    producto_default = productos[0] if productos else ""
    return render_template("panadero_pronostico.html",
                           productos=productos,
                           producto_default=producto_default,
                           layout="panadero", active_page="pronostico")


@app.route("/panadero/produccion", methods=["GET", "POST"])
@login_required
@roles_required("panadero", "cajero", "tenant_admin", "platform_superadmin")
def panadero_produccion():
    rol_actual = _rol_usuario_actual()
    if rol_actual not in ("panadero", "cajero"):
        flash("No tienes permiso para registrar produccion", "error")
        return redirect(url_for("auth.index"))

    productos = obtener_productos_panaderia()
    producto_default = productos[0] if productos else ""
    if request.method == "POST":
        try:
            fecha = request.form["fecha"]
            producto = request.form["producto"]
            cantidad_lote = int(request.form.get("cantidad_lote", 0) or 0)
            obs = request.form.get("observaciones", "")

            datetime.strptime(fecha, "%Y-%m-%d")

            if cantidad_lote <= 0:
                flash("La cantidad de la tanda debe ser mayor a cero", "error")
            elif producto not in productos:
                flash("Solo puedes registrar produccion de productos de Panaderia", "error")
            else:
                ok = _guardar_lote_produccion(fecha, producto, cantidad_lote, obs)
                if ok:
                    flash(f"Registro guardado: {producto} - {fecha}", "success")
                else:
                    app.logger.error(
                        "Error guardando tanda individual: producto=%s fecha=%s cantidad=%s usuario=%s",
                        producto,
                        fecha,
                        cantidad_lote,
                        _nombre_usuario_actual(),
                    )
                    flash("No se pudo guardar", "error")
        except (ValueError, KeyError) as e:
            flash(f"Datos invalidos: {e}", "error")

    hoy = datetime.now().strftime("%Y-%m-%d")
    registros_recientes = obtener_registros(dias=30)
    productos_panaderia = set(productos)
    registros_recientes = [r for r in registros_recientes if r.get("producto") in productos_panaderia]
    for registro in registros_recientes:
        producido = int(registro.get("producido", 0) or 0)
        vendido = int(registro.get("vendido", 0) or 0)
        sobrante_inicial = int(registro.get("sobrante_inicial", 0) or 0)
        sobrante = int(registro.get("sobrante", 0) or 0)
        registro["faltante"] = max(vendido - (sobrante_inicial + producido), 0)
        registro["sobrante"] = max(sobrante, 0)
        registro.setdefault("registrado_por", "")
        registro.setdefault("registrado_en", "")
    return render_template("panadero_produccion.html",
                           productos=productos,
                           producto_default=producto_default,
                           hoy=hoy,
                           registros_recientes=registros_recientes,
                           puede_ajustar_meta=(rol_actual == "panadero"),
                           layout="cajero" if rol_actual == "cajero" else "panadero",
                           active_page="produccion")


@app.route("/panadero/ventas")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_ventas():
    caja = obtener_resumen_caja_dia()
    historial_arqueos = obtener_historial_arqueos(8)
    movimientos_caja = obtener_movimientos_caja(limite=10)
    return render_template("dashboard_ventas.html",
                           caja=caja,
                           historial_arqueos=historial_arqueos,
                           movimientos_caja=movimientos_caja,
                           layout="panadero", active_page="ventas")


@app.route("/panadero/historial")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_historial():
    producto = request.args.get("producto", "Todos")
    productos = obtener_productos()
    filtros = _resolver_filtro_historial(default_days=30)

    return render_template("panadero_historial.html",
                           productos=productos,
                           filtro_producto=producto,
                           filtro_dias=filtros["dias"],
                           hoy_str=filtros["hoy_str"],
                           filtro_fecha_inicio=filtros["fecha_inicio"],
                           filtro_fecha_fin=filtros["fecha_fin"],
                           filtro_personalizado=filtros["filtro_personalizado"],
                           layout="panadero", active_page="historial")


@app.route("/panadero/cartera")
@login_required
@roles_required("cajero", "panadero", "tenant_admin", "platform_superadmin")
def panadero_cartera():
    estado = request.args.get("estado", "").strip().lower()
    busqueda = request.args.get("q", "").strip()
    fecha_desde = request.args.get("fecha_desde", "").strip()
    fecha_hasta = request.args.get("fecha_hasta", "").strip()
    pagination_args = _parse_pagination_args()
    cuentas_data = obtener_cuentas_por_cobrar_paginadas(
        estado=estado or None,
        busqueda_cliente=busqueda,
        fecha_desde=fecha_desde or None,
        fecha_hasta=fecha_hasta or None,
        page=pagination_args["page"],
        size=pagination_args["size"],
    )
    cuentas = cuentas_data["items"]
    pagination = cuentas_data["pagination"]
    resumen = obtener_resumen_cartera()
    layout = "cajero" if _rol_usuario_actual() == "cajero" else "panadero"
    return render_template(
        "panadero_cartera.html",
        cuentas=cuentas,
        pagination=pagination,
        pagination_links=_build_pagination_links(
            "panadero_cartera",
            pagination,
            estado=estado,
            q=busqueda,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
        ),
        resumen=resumen,
        filtro_estado=estado,
        filtro_busqueda=busqueda,
        filtro_fecha_desde=fecha_desde,
        filtro_fecha_hasta=fecha_hasta,
        layout=layout,
        active_page="cartera",
    )


@app.route("/clientes/<int:cliente_id>/historial")
@login_required
@roles_required("cajero", "panadero", "tenant_admin", "platform_superadmin")
def cliente_historial(cliente_id: int):
    pagination_args = _parse_pagination_args()
    historial = obtener_historial_cliente(
        cliente_id,
        page=pagination_args["page"],
        size=pagination_args["size"],
    )
    if not historial.get("ok"):
        return render_template("error.html", codigo=404, mensaje=historial.get("error") or "Cliente no encontrado"), 404
    layout = "cajero" if _rol_usuario_actual() == "cajero" else "panadero"
    pagination = (historial.get("pagination") or {}).get("global", {})
    return render_template(
        "cliente_historial.html",
        historial=historial,
        cliente=historial.get("cliente") or {},
        resumen=historial.get("resumen") or {},
        pagination=pagination,
        pagination_links=_build_pagination_links(
            "cliente_historial",
            pagination,
            cliente_id=cliente_id,
        ),
        layout=layout,
        active_page="cartera",
    )


@app.route("/panadero/documentos")
@login_required
@roles_required("cajero", "panadero", "tenant_admin", "platform_superadmin")
def panadero_documentos():
    pagination_args = _parse_pagination_args()
    origen_tipo = request.args.get("origen_tipo", "").strip().lower()
    estado_documento = request.args.get("estado", "").strip().lower()
    tipo_documento = request.args.get("tipo_documento", "").strip().lower()
    estado_envio = request.args.get("estado_envio", "").strip().lower()
    filtro_cliente = request.args.get("cliente", "").strip()
    filtro_fecha_desde = request.args.get("fecha_desde", "").strip()
    filtro_fecha_hasta = request.args.get("fecha_hasta", "").strip()
    try:
        filtro_fecha_desde = _parse_fecha_iso(filtro_fecha_desde) if filtro_fecha_desde else ""
    except ValueError:
        filtro_fecha_desde = ""
    try:
        filtro_fecha_hasta = _parse_fecha_iso(filtro_fecha_hasta) if filtro_fecha_hasta else ""
    except ValueError:
        filtro_fecha_hasta = ""
    documentos_data = obtener_documentos_recientes_paginados(
        page=pagination_args["page"],
        size=pagination_args["size"],
        origen_tipo=origen_tipo or None,
        estado=estado_documento or None,
        tipo_documento=tipo_documento or None,
        cliente=filtro_cliente or None,
        fecha_desde=filtro_fecha_desde or None,
        fecha_hasta=filtro_fecha_hasta or None,
        estado_envio=estado_envio or None,
    )
    documentos = [doc for doc in documentos_data["items"] if _documento_visible_para_usuario(doc)]
    pagination = dict(documentos_data["pagination"])
    pagination["items_count"] = len(documentos)
    layout = "cajero" if _rol_usuario_actual() == "cajero" else "panadero"
    return render_template(
        "panadero_documentos.html",
        documentos=documentos,
        pagination=pagination,
        pagination_links=_build_pagination_links(
            "panadero_documentos",
            pagination,
            origen_tipo=origen_tipo,
            estado=estado_documento,
            tipo_documento=tipo_documento,
            estado_envio=estado_envio,
            cliente=filtro_cliente,
            fecha_desde=filtro_fecha_desde,
            fecha_hasta=filtro_fecha_hasta,
        ),
        filtro_origen_tipo=origen_tipo,
        filtro_estado_documento=estado_documento,
        filtro_tipo_documento=tipo_documento,
        filtro_estado_envio=estado_envio,
        filtro_cliente=filtro_cliente,
        filtro_fecha_desde=filtro_fecha_desde,
        filtro_fecha_hasta=filtro_fecha_hasta,
        smtp_ready=_smtp_disponible(),
        layout=layout,
        active_page="documentos",
    )


@app.route("/panadero/operaciones")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_operaciones():
    stats = obtener_estadisticas_pedidos()
    consumo = obtener_consumo_diario()
    insumos = obtener_insumos()
    alertas_stock = obtener_insumos_bajo_stock()
    mesas = obtener_resumen_mesas(include_inactive=True)
    ventas_resumen = obtener_resumen_ventas_dia()
    ventas_total = obtener_total_ventas_dia()
    pedidos = obtener_pedidos_con_detalle()
    ventas_pos = _obtener_ventas_pos_operaciones()
    stats = {
        **stats,
        "ventas_pos": len(ventas_pos),
        "total_cobrado_global": float((ventas_total or {}).get("dinero", 0) or 0),
        "transacciones_totales": int((ventas_total or {}).get("transacciones", 0) or 0),
    }
    proxima_mesa = (max((mesa["numero"] for mesa in mesas if not mesa.get("eliminada")), default=0) + 1) if mesas else 1
    return render_template("panadero_operaciones.html",
                           stats=stats,
                           consumo=consumo,
                           insumos=insumos,
                           alertas_stock=alertas_stock,
                           mesas=mesas,
                           proxima_mesa=proxima_mesa,
                           ventas_resumen=ventas_resumen,
                           ventas_total=ventas_total,
                           ventas_pos=ventas_pos,
                           pedidos=pedidos,
                           layout="panadero", active_page="operaciones")


@app.route("/panadero/inventario")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_inventario():
    insumos = obtener_insumos()
    productos_catalogo = obtener_productos_con_precio()
    productos = [p["nombre"] for p in productos_catalogo]
    adicionales = obtener_adicionales()
    recetas = {}
    for p in productos:
        recetas[p] = obtener_receta(p)
    alertas_stock = obtener_insumos_bajo_stock()
    return render_template("panadero_inventario.html",
                           insumos=insumos,
                           adicionales=adicionales,
                           productos=productos,
                           productos_catalogo=productos_catalogo,
                           recetas=recetas,
                           alertas_stock=alertas_stock,
                           layout="panadero", active_page="inventario")


@app.route("/panadero/jornada")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_jornada():
    return render_template("panadero_jornada.html",
                           layout="panadero", active_page="jornada")


@app.route("/panadero/backups")
@login_required
@admin_required
def panadero_backups():
    info = obtener_info_backup()
    backups = listar_backups()
    return render_template("panadero_backups.html",
                           info=info, backups=backups,
                           layout="panadero", active_page="backups")


@app.route("/panadero/config")
@login_required
@admin_required
def panadero_config():
    from data.database import obtener_usuarios_panaderia

    productos = obtener_productos_con_precio()
    categorias = obtener_categorias_producto()
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])

    panaderia_id = _panaderia_actual_id()
    usuarios = obtener_usuarios_panaderia(int(panaderia_id)) if panaderia_id else obtener_usuarios()
    local_ip = _get_local_ip()
    tenant_slug = getattr(g, "tenant_context", TenantContext()).slug or "principal"
    sede_slug = getattr(g, "sede_context", SedeContext()).slug or "principal"
    qr_url = f"http://{local_ip}:5000/public/{tenant_slug}/{sede_slug}/cliente/pedido"
    codigo_caja = obtener_codigo_verificacion_caja()

    return render_template("panadero_config.html",
                           productos=productos,
                           categorias=categorias,
                           usuarios=usuarios,
                           codigo_caja=codigo_caja,
                           qr_url=qr_url,
                           layout="panadero", active_page="config")


# ── Cliente (publico, sin login) ──

@app.route("/public/<panaderia_slug>/<sede_slug>/cliente/pedido")
def public_cliente_pedido(panaderia_slug: str, sede_slug: str):
    tenant = getattr(g, "tenant_context", TenantContext())
    sede = getattr(g, "sede_context", SedeContext())
    if tenant.slug and sede.slug and (panaderia_slug != tenant.slug or sede_slug != sede.slug):
        return render_template("error.html", codigo=404, mensaje="Recurso no encontrado"), 404
    return cliente_pedido()


@app.route("/cliente/pedido")
def cliente_pedido():
    productos = obtener_productos_con_precio()
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])
    return render_template("cliente_pedido.html", productos=productos)


# ══════════════════════════════════════════════
# API JSON
# ══════════════════════════════════════════════

@app.route("/api/productos")
def api_productos():
    productos = obtener_productos_con_precio()
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])
    return jsonify(productos)


@app.route("/api/pronostico/dashboard")
@login_required
@tenant_scope_required
@sede_scope_required
def api_pronostico_dashboard():
    """API compatible con el dashboard de pronostico actual del frontend."""
    productos = obtener_productos_panaderia()
    if not productos:
        return jsonify({
            "producto": "",
            "productos": [],
            "prediccion_semana": [],
            "historial_producto": [],
            "serie_ventas_producto": [],
            "ranking_productos": [],
            "resumen": {},
            "lectura_operativa": generar_lectura_operativa(None, producto_seleccionado=False),
        })

    producto_query = request.args.get("producto")
    if producto_query is None:
        producto = productos[0]
    else:
        producto = str(producto_query or "").strip()
        if not producto:
            return jsonify({
                "producto": "",
                "productos": productos,
                "prediccion_semana": [],
                "historial_producto": [],
                "serie_ventas_producto": [],
                "ranking_productos": [],
                "resumen": {},
                "prediccion_semanal": {},
                "insights": {},
                "periodo": {"dias": 0, "desde": "", "hasta": "", "muestras": 0},
                "matriz_semana": [],
                "backtesting": None,
                "lectura_operativa": generar_lectura_operativa(None, producto_seleccionado=False),
            })
    dias = int(request.args.get("dias", 30))
    if producto not in productos:
        producto = productos[0]

    hoy = datetime.now().date()
    hoy_str = hoy.strftime("%Y-%m-%d")
    try:
        stock_detalle_hoy = obtener_stock_operativo_detalle(hoy_str)
    except Exception:
        app.logger.exception("Error obteniendo stock operativo para pronostico de %s", producto)
        stock_detalle_hoy = {}
    dias_es = {
        "Monday": "Lunes",
        "Tuesday": "Martes",
        "Wednesday": "Miercoles",
        "Thursday": "Jueves",
        "Friday": "Viernes",
        "Saturday": "Sabado",
        "Sunday": "Domingo",
    }

    prediccion_semana = []
    lectura_operativa = generar_lectura_operativa(None, producto_seleccionado=bool(producto))
    for i in range(7):
        fecha = hoy + timedelta(days=i)
        fecha_str = fecha.strftime("%Y-%m-%d")
        dia_es = dias_es.get(fecha.strftime("%A"), fecha.strftime("%A"))
        try:
            enc_conf = obtener_encargos_confirmados_para_fecha(producto, fecha_str)
            stock_actual = None
            if fecha_str == hoy_str:
                detalle_stock = (stock_detalle_hoy or {}).get(producto, {}) or {}
                stock_actual = int(detalle_stock.get("disponible_bruto", 0) or 0)
            resultado = calcular_pronostico(
                producto, fecha_objetivo=fecha_str,
                stock_actual=stock_actual,
                encargos_confirmados=enc_conf,
            )
            if fecha_str == hoy_str:
                lectura_operativa = generar_lectura_operativa(resultado, producto_seleccionado=True)
            detalles = getattr(resultado, "detalles", {}) or {}
            prediccion_semana.append({
                "fecha": fecha_str,
                "dia": dia_es,
                "sugerido": resultado.produccion_recomendada,
                "venta_estimada": resultado.demanda_estimada,
                "demanda_estimada": resultado.demanda_estimada,
                "demanda_comprometida": resultado.demanda_comprometida,
                "produccion_recomendada": resultado.produccion_recomendada,
                "promedio": round(resultado.promedio_ventas, 1),
                "delta": round(resultado.produccion_recomendada - resultado.promedio_ventas, 1),
                "tipo_dia": TIPO_DIA.get(dia_es, "laboral"),
                "estado": resultado.estado,
                "confianza": resultado.confianza,
                "modelo": resultado.modelo_usado,
                "mensaje": resultado.mensaje,
                "nivel_calidad": resultado.nivel_calidad,
                "encargos_confirmados": enc_conf,
                "stock_actual": int(detalles.get("stock_actual", 0) or 0),
                "stock_actual_disponible": bool(detalles.get("stock_actual_disponible")),
                "produccion_pendiente": int(detalles.get("produccion_pendiente", 0) or 0),
                "produccion_pendiente_disponible": bool(detalles.get("produccion_pendiente_disponible")),
                "detalles": detalles,
            })
        except Exception:
            app.logger.exception("Error calculando pronostico semanal para %s", producto)
            prediccion_semana.append({
                "fecha": fecha_str,
                "dia": dia_es,
                "sugerido": 0,
                "venta_estimada": 0,
                "demanda_estimada": 0,
                "demanda_comprometida": 0,
                "produccion_recomendada": 0,
                "promedio": 0,
                "delta": 0,
                "tipo_dia": TIPO_DIA.get(dia_es, "laboral"),
                "estado": "alerta",
                "confianza": "poca",
                "modelo": "error",
                "mensaje": "Error al calcular el pronóstico para este día.",
                "nivel_calidad": 0,
                "encargos_confirmados": 0,
                "detalles": {},
            })

    historial = list(reversed(obtener_historial_pronostico(producto, dias=dias)))
    serie_ventas_producto = obtener_serie_ventas_diarias(dias=dias, producto=producto)
    ranking_productos = obtener_resumen_productos_rango(dias=dias)

    total_producido = sum(int(r.get("producido", 0) or 0) for r in historial)
    total_vendido = sum(int(r.get("vendido", 0) or 0) for r in historial)
    total_sobrante = sum(max(int(r.get("sobrante", 0) or 0), 0) for r in historial)
    aprovechamiento = round((total_vendido / total_producido * 100), 1) if total_producido else 0
    tendencia = analizar_tendencia(historial)

    brecha_total_semana = round(sum(d["delta"] for d in prediccion_semana), 1)
    brecha_promedio_semana = round(brecha_total_semana / 7, 1)
    ventas_promedio_periodo = round(total_vendido / len(historial), 1) if historial else 0.0

    resumen = {
        "total_producido": total_producido,
        "total_vendido": total_vendido,
        "total_sobrante": total_sobrante,
        "aprovechamiento": aprovechamiento,
        "tendencia": tendencia,
        "sugerido_semana": sum(d["sugerido"] for d in prediccion_semana),
        "promedio_sugerido": round(sum(d["sugerido"] for d in prediccion_semana) / 7, 1),
        "brecha_total_semana": brecha_total_semana,
        "brecha_promedio_semana": brecha_promedio_semana,
        "ventas_promedio_periodo": ventas_promedio_periodo,
    }

    # Resumen por día de semana (ya usado abajo para prediccion_semanal y matriz)
    resumen_dia = obtener_resumen_pronostico_por_dia_semana(producto, dias=dias)
    prediccion_semanal = {
        dia: resumen_dia.get(dia, {}).get("promedio", 0)
        for dia in ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    }

    # insights
    from collections import Counter
    if prediccion_semana:
        _dia_pico = max(prediccion_semana, key=lambda x: x["sugerido"])
        _dia_relajado = min(prediccion_semana, key=lambda x: x["sugerido"])
    else:
        _dia_pico = {}
        _dia_relajado = {}
    _mejor_hist = max(resumen_dia.items(), key=lambda x: x[1].get("promedio", 0), default=(None, {}))
    _confianza_counts = Counter(d["confianza"] for d in prediccion_semana)
    _modelo_counter = Counter(d["modelo"] for d in prediccion_semana)
    _modelo_principal = _modelo_counter.most_common(1)[0][0] if _modelo_counter else "sin_datos"
    _variacion_semana = (
        max(d["sugerido"] for d in prediccion_semana) - min(d["sugerido"] for d in prediccion_semana)
        if prediccion_semana else 0
    )
    insights = {
        "dia_pico": {
            "dia": _dia_pico.get("dia", ""),
            "valor": _dia_pico.get("sugerido", 0),
            "fecha": _dia_pico.get("fecha", ""),
        },
        "dia_relajado": {"dia": _dia_relajado.get("dia", "")},
        "mejor_historial": {
            "dia": _mejor_hist[0] or "",
            "valor": (_mejor_hist[1] or {}).get("promedio", 0),
        },
        "confianza": {
            "buena": _confianza_counts.get("buena", 0),
            "media": _confianza_counts.get("media", 0),
            "poca": _confianza_counts.get("poca", 0),
        },
        "modelo_principal_label": _modelo_principal.replace("_", " ").capitalize(),
        "variacion_semana": _variacion_semana,
    }

    # periodo
    desde_str = historial[-1]["fecha"] if historial else hoy_str
    periodo = {
        "dias": dias,
        "desde": desde_str,
        "hasta": hoy_str,
        "muestras": len(historial),
    }

    # matriz_semana (heatmap + radar)
    _DIAS_ORDEN = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    _pred_by_dia = {d["dia"]: d for d in prediccion_semana}
    _max_sug = max((d["sugerido"] for d in prediccion_semana), default=1) or 1
    _total_sug = sum(d["sugerido"] for d in prediccion_semana) or 1
    matriz_semana = []
    for _dia in _DIAS_ORDEN:
        _rd = resumen_dia.get(_dia, {})
        _pred = _pred_by_dia.get(_dia, {})
        _sug = _pred.get("sugerido", 0)
        _prom = _rd.get("promedio", 0) or 0
        _muestras = _rd.get("muestras", 0) or 0
        matriz_semana.append({
            "dia": _dia,
            "tipo_dia": TIPO_DIA.get(_dia, "laboral"),
            "sugerido": _sug,
            "promedio": _prom,
            "muestras": _muestras,
            "intensidad": round(_sug / _max_sug, 3),
            "participacion": round(_sug / _total_sug * 100, 1),
        })

    # Backtesting (solo si el cliente lo pide explícitamente para no ralentizar la carga inicial)
    backtesting = None
    if request.args.get("backtesting") == "1":
        try:
            backtesting = calcular_backtesting(producto)
        except Exception:
            backtesting = {"ok": False, "error": "Error calculando backtesting"}

    return jsonify({
        "producto": producto,
        "productos": productos,
        "prediccion_semana": prediccion_semana,
        "historial_producto": historial,
        "serie_ventas_producto": serie_ventas_producto,
        "ranking_productos": ranking_productos,
        "resumen": resumen,
        "prediccion_semanal": prediccion_semanal,
        "insights": insights,
        "periodo": periodo,
        "matriz_semana": matriz_semana,
        "backtesting": backtesting,
        "lectura_operativa": lectura_operativa,
    })


@app.route("/api/pronostico/sugerencia")
@login_required
def api_pronostico_sugerencia():
    producto = request.args.get("producto", "").strip()
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d")).strip()
    productos_panaderia = set(obtener_productos_panaderia())

    if not producto:
        return jsonify({"ok": False, "error": "Producto requerido"}), 400
    if producto not in productos_panaderia:
        return jsonify({"ok": False, "error": "El pronostico aplica solo a productos de Panaderia"}), 400

    try:
        datetime.strptime(fecha, "%Y-%m-%d")
        resultado = calcular_pronostico(producto, fecha_objetivo=fecha)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha invalida"}), 400
    except Exception:
        app.logger.exception("Error calculando sugerencia para %s", producto)
        return jsonify({"ok": False, "error": "No se pudo calcular la sugerencia"}), 500

    return jsonify({
        "ok": True,
        "producto": producto,
        "fecha": fecha,
        "sugerido": resultado.produccion_recomendada,
        "demanda_estimada": resultado.demanda_estimada,
        "demanda_comprometida": resultado.demanda_comprometida,
        "produccion_recomendada": resultado.produccion_recomendada,
        "modelo": resultado.modelo_usado,
        "modelo_label": resultado.modelo_usado.replace("_", " ").capitalize(),
        "confianza": resultado.confianza,
        "promedio": resultado.promedio_ventas,
        "mensaje": resultado.mensaje,
        "estado": resultado.estado,
        "tendencia": resultado.detalles.get("tendencia", "sin datos"),
        "dia_objetivo": resultado.detalles.get("dia_objetivo", fecha),
        "lectura_operativa": generar_lectura_operativa(resultado, producto_seleccionado=True),
    })


@app.route("/api/produccion/contexto")
@login_required
def api_contexto_produccion():
    if not _usuario_puede_registrar_produccion():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403

    producto = str(request.args.get("producto", "") or "").strip()
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    productos_panaderia = set(obtener_productos_panaderia())

    if not producto:
        return jsonify({"ok": False, "error": "Producto requerido"}), 400
    if producto not in productos_panaderia:
        return jsonify({"ok": False, "error": "El contexto aplica solo a productos de Panaderia"}), 400

    try:
        contexto = _construir_contexto_produccion_producto(fecha, producto)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha invalida"}), 400
    except Exception:
        app.logger.exception("Error cargando contexto de produccion para %s", producto)
        return jsonify({"ok": False, "error": "No se pudo cargar el contexto de produccion"}), 500

    return jsonify(contexto)


@app.route("/api/produccion/contexto-masivo")
@login_required
def api_contexto_produccion_masivo():
    if not _usuario_puede_registrar_produccion():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403

    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    try:
        contexto = _construir_contexto_produccion_masivo(fecha)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha invalida"}), 400
    except Exception:
        app.logger.exception("Error cargando contexto de produccion masivo")
        return jsonify({"ok": False, "error": "No se pudo cargar el registro rapido"}), 500
    return jsonify(contexto)


@app.route("/api/produccion/validar-insumos", methods=["POST"])
@login_required
def api_validar_insumos_produccion():
    if not _usuario_puede_registrar_produccion():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403

    data = request.get_json(silent=True) or {}
    try:
        evaluacion = _evaluar_insumos_lotes(data.get("lotes", []))
    except Exception:
        app.logger.exception("Error validando insumos para produccion")
        return jsonify({"ok": False, "error": "No se pudo validar el inventario de insumos"}), 500
    return jsonify({"ok": True, **evaluacion})


@app.route("/api/produccion/lotes-masivos", methods=["POST"])
@login_required
def api_guardar_lotes_masivos():
    if not _usuario_puede_registrar_produccion():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403

    data = request.get_json(silent=True) or {}
    fecha = str(data.get("fecha", "") or "").strip()
    turno = str(data.get("turno", "") or "").strip()
    nota = str(data.get("nota", "") or "").strip()
    lotes = data.get("lotes", [])
    productos_panaderia = set(obtener_productos_panaderia())

    try:
        fecha = _parse_fecha_iso(fecha)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha invalida"}), 400

    guardados = 0
    total_unidades = 0
    detalle_lote = []
    if turno:
        detalle_lote.append(f"Tanda: {turno}")
    if nota:
        detalle_lote.append(nota)
    observaciones = " | ".join(detalle_lote)

    for lote in lotes:
        producto = str((lote or {}).get("producto", "") or "").strip()
        try:
            cantidad = int((lote or {}).get("cantidad", 0) or 0)
        except (TypeError, ValueError):
            cantidad = 0

        if not producto or cantidad <= 0:
            continue
        if producto not in productos_panaderia:
            return jsonify({"ok": False, "error": f"{producto} no pertenece a Panaderia"}), 400
        if not _guardar_lote_produccion(fecha, producto, cantidad, observaciones):
            app.logger.error(
                "Error guardando tanda masiva: producto=%s fecha=%s cantidad=%s turno=%s usuario=%s",
                producto,
                fecha,
                cantidad,
                turno,
                _nombre_usuario_actual(),
            )
            return jsonify({"ok": False, "error": f"No se pudo guardar la tanda para {producto}"}), 500
        guardados += 1
        total_unidades += cantidad

    if guardados == 0:
        return jsonify({"ok": False, "error": "No hay tandas validas para guardar"}), 400

    return jsonify({
        "ok": True,
        "fecha": fecha,
        "guardados": guardados,
        "total_unidades": total_unidades,
    })


@app.route("/api/produccion/descartar", methods=["POST"])
@login_required
def api_descartar_produccion():
    if not _usuario_puede_registrar_produccion():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403

    data = request.get_json(silent=True) or {}
    fecha = str(data.get("fecha", "") or "").strip()
    producto = str(data.get("producto", "") or "").strip()
    motivo = str(data.get("motivo", "") or "").strip()
    tipo = str(data.get("tipo", "vencido") or "vencido").strip()
    productos_panaderia = set(obtener_productos_panaderia())

    try:
        cantidad = int(data.get("cantidad", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Cantidad invalida"}), 400

    try:
        fecha = _parse_fecha_iso(fecha)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha invalida"}), 400

    if not producto:
        return jsonify({"ok": False, "error": "Producto requerido"}), 400
    if producto not in productos_panaderia:
        return jsonify({"ok": False, "error": "Solo puedes descartar productos de Panaderia"}), 400
    if cantidad <= 0:
        return jsonify({"ok": False, "error": "La cantidad debe ser mayor a cero"}), 400
    if not motivo:
        return jsonify({"ok": False, "error": "Debes escribir el motivo del descarte"}), 400

    resultado = descartar_stock_produccion(
        fecha=fecha,
        producto=producto,
        cantidad=cantidad,
        motivo=motivo,
        registrado_por=_nombre_usuario_actual(),
        tipo_merma=tipo,
    )
    status = 200 if resultado.get("ok") else 400
    if resultado.get("ok"):
        registrar_audit(
            usuario=_nombre_usuario_actual(),
            accion="descartar_produccion",
            entidad="produccion",
            entidad_id=f"{fecha}/{producto}",
            detalle=f"Descarte de {cantidad} und de {producto}. Motivo: {motivo}",
            valor_nuevo=f"fecha={fecha} | tipo={tipo}",
        )
    return jsonify(resultado), status


@app.route("/api/inventario/proyeccion-insumos")
@login_required
def api_proyeccion_insumos():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    try:
        contexto = _construir_contexto_produccion_masivo(fecha)
        productos = [
            {
                "producto": item["producto"],
                "cantidad": item["meta_operativa"],
                "modelo_label": item["modelo_label"],
                "confianza": item["confianza"],
            }
            for item in contexto["items"]
            if int(item.get("meta_operativa", 0) or 0) > 0
        ]
        evaluacion = _evaluar_insumos_lotes(productos)
    except ValueError:
        return jsonify({"ok": False, "error": "Fecha invalida"}), 400
    except Exception:
        app.logger.exception("Error calculando proyeccion de insumos")
        return jsonify({"ok": False, "error": "No se pudo calcular la proyeccion de insumos"}), 500

    return jsonify({
        "ok": True,
        "fecha": _parse_fecha_iso(fecha),
        "productos": productos,
        "insumos": evaluacion["insumos"],
        "criticos": evaluacion["criticos"],
        "alertas": evaluacion["alertas"],
        "productos_sin_receta": evaluacion["productos_sin_receta"],
        "productos_sin_rendimiento": evaluacion["productos_sin_rendimiento"],
        "resumen": {
            "unidades_planeadas": sum(int(item["cantidad"] or 0) for item in productos),
            "insumos_comprometidos": len(evaluacion["insumos"]),
            "insumos_criticos": len(evaluacion["criticos"]),
            "insumos_alerta": len(evaluacion["alertas"]),
        },
    })


@app.route("/api/historial/dashboard")
@login_required
@tenant_scope_required
@sede_scope_required
def api_historial_dashboard():
    """API compatible con el dashboard contable/historico del frontend."""
    filtros = _resolver_filtro_historial(default_days=30)
    dias = filtros["dias"]
    fecha_inicio = filtros["fecha_inicio"]
    fecha_fin = filtros["fecha_fin"]
    producto = request.args.get("producto", "Todos")
    producto_filtro = None if producto in ("", "Todos") else producto

    totales = obtener_totales_ventas_rango(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    serie_diaria = obtener_serie_ventas_diarias(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    resumen_productos = obtener_resumen_productos_rango(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    ventas_recientes = obtener_ventas_rango(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        limite=25,
    )
    ventas_detalle = obtener_ventas_rango(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        limite=None,
    )
    serie_pago = obtener_serie_medios_pago_diaria_rango(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    serie_horaria_rows = obtener_serie_ventas_horaria_rango(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    registros_operacion = obtener_registros(
        producto=producto_filtro,
        dias=dias,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    arqueos_periodo = obtener_arqueos_rango(
        dias=dias,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    movimientos_periodo = obtener_movimientos_caja_rango(
        dias=dias,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )
    medios_pago_db = obtener_resumen_medios_pago_rango(
        dias=dias,
        producto=producto_filtro,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
    )

    transacciones = int(totales.get("transacciones", 0) or 0)
    dinero = float(totales.get("dinero", 0) or 0)
    ticket_promedio = round(dinero / transacciones, 2) if transacciones else 0.0

    horas = {f"{h:02d}:00": 0 for h in range(6, 22)}
    for v in serie_horaria_rows:
        hora = str(v.get("hora", "") or "").zfill(2)[:2]
        if hora.isdigit():
            h = int(hora)
            if 6 <= h <= 21:
                key = f"{h:02d}:00"
                horas[key] = int(v.get("panes", 0) or 0)

    serie_horaria = [{"hora": k, "panes": v} for k, v in horas.items()]

    medios_pago = []
    medios_por_nombre = {
        "efectivo": {"metodo": "Efectivo", "total": 0.0, "transacciones": 0},
        "transferencia": {"metodo": "Transferencia", "total": 0.0, "transacciones": 0},
    }
    for row in medios_pago_db:
        metodo_key = str(row.get("metodo", "efectivo") or "efectivo").strip().lower()
        label = "Transferencia" if metodo_key == "transferencia" else "Efectivo"
        if metodo_key not in medios_por_nombre:
            medios_por_nombre[metodo_key] = {"metodo": label, "total": 0.0, "transacciones": 0}
        medios_por_nombre[metodo_key]["metodo"] = label
        medios_por_nombre[metodo_key]["total"] = round(float(row.get("total", 0) or 0), 2)
        medios_por_nombre[metodo_key]["transacciones"] = int(row.get("transacciones", 0) or 0)
    for key in ("efectivo", "transferencia"):
        medios_pago.append(medios_por_nombre[key])
    for key, row in medios_por_nombre.items():
        if key not in ("efectivo", "transferencia"):
            medios_pago.append(row)

    ventas_efectivo = round(sum(item["total"] for item in medios_pago if item["metodo"] == "Efectivo"), 2)
    ventas_transferencia = round(sum(item["total"] for item in medios_pago if item["metodo"] == "Transferencia"), 2)

    total_producido = sum(max(int(r.get("producido", 0) or 0), 0) for r in registros_operacion)
    total_vendido = sum(max(int(r.get("vendido", 0) or 0), 0) for r in registros_operacion)
    total_sobrante = sum(max(int(r.get("sobrante", 0) or 0), 0) for r in registros_operacion)
    total_faltante = sum(
        max(int(r.get("vendido", 0) or 0) - int(r.get("producido", 0) or 0), 0)
        for r in registros_operacion
    )

    aprovechamiento = round((total_vendido / total_producido) * 100, 1) if total_producido else 0.0
    desperdicio = round((total_sobrante / total_producido) * 100, 1) if total_producido else 0.0
    dias_con_quiebre = sum(
        1 for r in registros_operacion
        if (int(r.get("vendido", 0) or 0) - int(r.get("producido", 0) or 0)) > 0
    )

    por_fecha = {}
    for r in registros_operacion:
        fecha = r.get("fecha")
        if not fecha:
            continue
        if fecha not in por_fecha:
            por_fecha[fecha] = {
                "fecha": fecha,
                "producido": 0,
                "vendido": 0,
                "sobrante": 0,
                "faltante": 0,
            }
        producido = max(int(r.get("producido", 0) or 0), 0)
        vendido = max(int(r.get("vendido", 0) or 0), 0)
        sobrante = max(int(r.get("sobrante", 0) or 0), 0)
        faltante = max(vendido - producido, 0)
        por_fecha[fecha]["producido"] += producido
        por_fecha[fecha]["vendido"] += vendido
        por_fecha[fecha]["sobrante"] += sobrante
        por_fecha[fecha]["faltante"] += faltante

    serie_operativa = [por_fecha[k] for k in sorted(por_fecha.keys())]

    ingresos_manuales = round(sum(float(m.get("monto", 0) or 0) for m in movimientos_periodo if m.get("tipo") == "ingreso"), 2)
    egresos_manuales = round(sum(float(m.get("monto", 0) or 0) for m in movimientos_periodo if m.get("tipo") == "egreso"), 2)
    total_apertura = round(sum(float(a.get("monto_apertura", 0) or 0) for a in arqueos_periodo), 2)
    cierres_registrados = sum(1 for a in arqueos_periodo if a.get("cerrado_en"))
    reaperturas = sum(int(a.get("reaperturas", 0) or 0) for a in arqueos_periodo)
    diferencia_total = round(sum(float(a.get("diferencia_cierre", 0) or 0) for a in arqueos_periodo if a.get("diferencia_cierre") is not None), 2)
    efectivo_contado = round(sum(float(a.get("monto_cierre", 0) or 0) for a in arqueos_periodo if a.get("monto_cierre") is not None), 2)

    serie_caja_map = {}
    for arqueo in arqueos_periodo:
        fecha = arqueo.get("fecha")
        if not fecha:
            continue
        bucket = serie_caja_map.setdefault(fecha, {
            "fecha": fecha,
            "apertura": 0.0,
            "ventas_efectivo": 0.0,
            "ventas_transferencia": 0.0,
            "ingresos": 0.0,
            "egresos": 0.0,
            "diferencia": 0.0,
        })
        bucket["apertura"] += float(arqueo.get("monto_apertura", 0) or 0)
        bucket["diferencia"] += float(arqueo.get("diferencia_cierre", 0) or 0)

    for fila in serie_pago:
        bucket = serie_caja_map.setdefault(fila["fecha"], {
            "fecha": fila["fecha"],
            "apertura": 0.0,
            "ventas_efectivo": 0.0,
            "ventas_transferencia": 0.0,
            "ingresos": 0.0,
            "egresos": 0.0,
            "diferencia": 0.0,
        })
        bucket["ventas_efectivo"] += float(fila.get("efectivo", 0) or 0)
        bucket["ventas_transferencia"] += float(fila.get("transferencia", 0) or 0)

    for mov in movimientos_periodo:
        fecha = mov.get("fecha")
        if not fecha:
            continue
        bucket = serie_caja_map.setdefault(fecha, {
            "fecha": fecha,
            "apertura": 0.0,
            "ventas_efectivo": 0.0,
            "ventas_transferencia": 0.0,
            "ingresos": 0.0,
            "egresos": 0.0,
            "diferencia": 0.0,
        })
        monto = float(mov.get("monto", 0) or 0)
        if mov.get("tipo") == "egreso":
            bucket["egresos"] += monto
        else:
            bucket["ingresos"] += monto

    serie_caja = []
    for fecha, data in sorted(serie_caja_map.items()):
        data["apertura"] = round(data["apertura"], 2)
        data["ventas_efectivo"] = round(data["ventas_efectivo"], 2)
        data["ventas_transferencia"] = round(data["ventas_transferencia"], 2)
        data["ingresos"] = round(data["ingresos"], 2)
        data["egresos"] = round(data["egresos"], 2)
        data["diferencia"] = round(data["diferencia"], 2)
        serie_caja.append(data)

    dias_activos = sum(1 for row in serie_diaria if float(row.get("dinero", 0) or 0) > 0)
    promedio_diario = round(dinero / dias, 2) if dias else 0.0
    promedio_unidades_diario = round(int(totales.get("panes", 0) or 0) / dias, 1) if dias else 0.0
    porcentaje_efectivo = round((ventas_efectivo / dinero) * 100, 1) if dinero else 0.0
    porcentaje_transferencia = round((ventas_transferencia / dinero) * 100, 1) if dinero else 0.0

    dias_semana_es = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    serie_dia_semana_map = {
        dia: {"dia": dia, "dinero": 0.0, "panes": 0}
        for dia in dias_semana_es
    }
    for row in serie_diaria:
        fecha = row.get("fecha")
        if not fecha:
            continue
        try:
            fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
        except ValueError:
            continue
        dia = dias_semana_es[fecha_dt.weekday()]
        serie_dia_semana_map[dia]["dinero"] += float(row.get("dinero", 0) or 0)
        serie_dia_semana_map[dia]["panes"] += int(row.get("panes", 0) or 0)
    serie_dia_semana = [
        {
            "dia": dia,
            "dinero": round(datos["dinero"], 2),
            "panes": int(datos["panes"]),
        }
        for dia, datos in serie_dia_semana_map.items()
    ]

    mejor_dia = max(serie_diaria, key=lambda row: float(row.get("dinero", 0) or 0), default={})
    hora_pico = max(serie_horaria, key=lambda row: int(row.get("panes", 0) or 0), default={})
    if int(hora_pico.get("panes", 0) or 0) <= 0:
        hora_pico = {}
    producto_lider = resumen_productos[0] if resumen_productos else {}
    insights = {
        "mejor_dia": mejor_dia,
        "hora_pico": hora_pico,
        "producto_lider": producto_lider,
    }
    periodo = {
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "dias": dias,
    }

    return jsonify({
        "filtro_producto": producto,
        "dias": dias,
        "periodo": periodo,
        "insights": insights,
        "totales": {
            "panes": int(totales.get("panes", 0) or 0),
            "dinero": dinero,
            "transacciones": transacciones,
            "ticket_promedio": ticket_promedio,
            "dias_activos": dias_activos,
            "promedio_diario": promedio_diario,
            "promedio_unidades_diario": promedio_unidades_diario,
            "porcentaje_efectivo": porcentaje_efectivo,
            "porcentaje_transferencia": porcentaje_transferencia,
            "ventas_efectivo": ventas_efectivo,
            "ventas_transferencia": ventas_transferencia,
            "ingresos_manuales": ingresos_manuales,
            "egresos_manuales": egresos_manuales,
            "total_arqueos": len(arqueos_periodo),
            "cierres_registrados": cierres_registrados,
            "reaperturas": reaperturas,
            "diferencia_cierre": diferencia_total,
        },
        "serie_diaria": serie_diaria,
        "serie_pago": serie_pago,
        "serie_caja": serie_caja,
        "medios_pago": medios_pago,
        "resumen_productos": resumen_productos,
        "ventas_recientes": ventas_recientes,
        "ventas_detalle": ventas_detalle,
        "serie_horaria": serie_horaria,
        "serie_dia_semana": serie_dia_semana,
        "caja_periodo": {
            "total_apertura": total_apertura,
            "ventas_efectivo": ventas_efectivo,
            "ventas_transferencia": ventas_transferencia,
            "ingresos_manuales": ingresos_manuales,
            "egresos_manuales": egresos_manuales,
            "cierres_registrados": cierres_registrados,
            "efectivo_contado": efectivo_contado,
            "diferencia_total": diferencia_total,
            "reaperturas": reaperturas,
        },
        "arqueos_historial": arqueos_periodo[:20],
        "movimientos_historial": movimientos_periodo[:30],
        "operacion": {
            "total_producido": total_producido,
            "total_vendido": total_vendido,
            "total_sobrante": total_sobrante,
            "total_faltante": total_faltante,
            "aprovechamiento": aprovechamiento,
            "desperdicio": desperdicio,
            "dias_con_quiebre": dias_con_quiebre,
            "registros": len(registros_operacion),
        },
        "serie_operativa": serie_operativa,
    })


@app.route("/api/venta", methods=["POST"])
@login_required
@tenant_scope_required
@sede_scope_required
def api_venta():
    data = request.json
    if not data or "items" not in data:
        return jsonify({"ok": False, "error": "Sin datos"}), 400

    items_validacion = []
    for item in data["items"]:
        try:
            producto_id = int(item.get("producto_id", 0) or 0)
        except (TypeError, ValueError):
            producto_id = 0
        producto_info = obtener_producto_por_id(producto_id) if producto_id > 0 else None
        items_validacion.append({
            "producto": (producto_info or {}).get("nombre") or item.get("producto", ""),
            "producto_id": (producto_info or {}).get("id") or producto_id or None,
            "cantidad": int(item.get("cantidad", 0) or 0),
            "modificaciones": [],
        })

    validacion = validar_stock_pedido(items_validacion)
    if not validacion["ok"]:
        return jsonify({
            "ok": False,
            "error": validacion["error"],
            "faltantes": validacion["faltantes"],
        }), 400

    usuario = "Cliente"
    if "usuario" in session:
        usuario = session["usuario"]["nombre"]

    metodo_pago = str(data.get("metodo_pago", "efectivo") or "efectivo").strip().lower()
    monto_recibido = data.get("monto_recibido")
    if monto_recibido is not None:
        try:
            monto_recibido = float(monto_recibido)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Monto recibido invalido"}), 400

    raw_mp2 = data.get("metodo_pago_2")
    raw_monto2 = data.get("monto_pago_2")
    metodo_pago_2 = str(raw_mp2).strip().lower() if raw_mp2 else None
    try:
        monto_pago_2 = float(raw_monto2) if raw_monto2 not in (None, "", "null") else None
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Monto del segundo metodo invalido"}), 400

    items_venta = []
    try:
        for item in data["items"]:
            try:
                producto_id = int(item.get("producto_id", 0) or 0)
            except (TypeError, ValueError):
                producto_id = 0
            producto_info = obtener_producto_por_id(producto_id) if producto_id > 0 else None
            cantidad = int(item["cantidad"])
            precio = float((producto_info or {}).get("precio", item["precio"]))
            producto_nombre = (producto_info or {}).get("nombre") or item["producto"]
            items_venta.append({
                "producto_id": (producto_info or {}).get("id") or producto_id or None,
                "producto": producto_nombre,
                "cantidad": cantidad,
                "precio": precio,
                "total": round(cantidad * precio, 2),
            })
    except (KeyError, TypeError, ValueError):
        return jsonify({"ok": False, "error": "Items invalidos"}), 400

    resultado = registrar_venta_lote(
        items_venta,
        registrado_por=usuario,
        metodo_pago=metodo_pago,
        monto_recibido=monto_recibido,
        referencia_tipo="pos",
        metodo_pago_2=metodo_pago_2,
        monto_pago_2=monto_pago_2,
    )
    if resultado.get("ok"):
        resultado["caja"] = obtener_resumen_caja_dia()
    status = 200 if resultado.get("ok") else 400
    return jsonify(resultado), status


@app.route("/api/ventas/hoy")
@login_required
@tenant_scope_required
@sede_scope_required
def api_ventas_hoy():
    try:
        hoy = datetime.now().strftime("%Y-%m-%d")
        caja = obtener_resumen_caja_dia()
        totales = obtener_total_ventas_dia()
        ventas_hora = obtener_serie_ventas_horaria_rango(dias=1, fecha_inicio=hoy, fecha_fin=hoy)
        # ticket promedio: total / transacciones
        _trans = int(totales.get("transacciones", 0) or 0)
        _total = float(totales.get("total", 0) or 0)
        totales["ticket_promedio"] = round(_total / _trans, 0) if _trans else 0.0
        return jsonify({
            "totales": totales,
            "resumen": obtener_resumen_ventas_dia(),
            "ventas": obtener_ventas_dia(),
            "ventas_por_responsable": obtener_resumen_ventas_por_responsable(),
            "caja": caja,
            "arqueos": obtener_historial_arqueos(6),
            "movimientos": obtener_movimientos_caja(limite=20),
            "metodos_pago": caja.get("metodos_pago", []),
            "ventas_por_hora": ventas_hora,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/caja/abrir", methods=["POST"])
@login_required
def api_abrir_caja():
    data = request.get_json(silent=True) or {}
    try:
        monto_apertura = float(data.get("monto_apertura", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Monto de apertura invalido"}), 400

    notas = str(data.get("notas", "") or "").strip()
    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = abrir_arqueo_caja(usuario, monto_apertura, notas=notas)
    status = 200 if resultado.get("ok") else 400
    if resultado.get("ok"):
        resultado["caja"] = obtener_resumen_caja_dia()
    return jsonify(resultado), status


@app.route("/api/caja/movimiento", methods=["POST"])
@login_required
def api_registrar_movimiento_caja():
    data = request.get_json(silent=True) or {}
    tipo = str(data.get("tipo", "") or "").strip().lower()
    concepto = str(data.get("concepto", "") or "").strip()
    notas = str(data.get("notas", "") or "").strip()
    try:
        monto = float(data.get("monto", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Monto invalido"}), 400

    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = registrar_movimiento_caja(
        tipo=tipo,
        concepto=concepto,
        monto=monto,
        registrado_por=usuario,
        notas=notas,
    )
    status = 200 if resultado.get("ok") else 400
    if resultado.get("ok"):
        resultado["caja"] = obtener_resumen_caja_dia()
        resultado["movimientos"] = obtener_movimientos_caja(limite=20)
    return jsonify(resultado), status


@app.route("/api/caja/cerrar", methods=["POST"])
@login_required
def api_cerrar_caja():
    data = request.get_json(silent=True) or {}
    try:
        monto_cierre = float(data.get("monto_cierre", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Monto de cierre invalido"}), 400

    notas_cierre = str(data.get("notas_cierre", "") or "").strip()
    codigo_verificacion = str(data.get("codigo_verificacion", "") or "").strip()

    raw_tarjeta = data.get("monto_tarjeta_cierre")
    raw_transferencia = data.get("monto_transferencia_cierre")
    try:
        monto_tarjeta_cierre = float(raw_tarjeta) if raw_tarjeta not in (None, "", "null") else None
    except (TypeError, ValueError):
        monto_tarjeta_cierre = None
    try:
        monto_transferencia_cierre = float(raw_transferencia) if raw_transferencia not in (None, "", "null") else None
    except (TypeError, ValueError):
        monto_transferencia_cierre = None

    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = cerrar_arqueo_caja(
        cerrado_por=usuario,
        monto_cierre=monto_cierre,
        notas_cierre=notas_cierre,
        codigo_verificacion=codigo_verificacion,
        monto_tarjeta_cierre=monto_tarjeta_cierre,
        monto_transferencia_cierre=monto_transferencia_cierre,
    )
    status = 200 if resultado.get("ok") else 400
    if resultado.get("ok"):
        diferencia = resultado.get("diferencia", 0)
        registrar_audit(
            usuario=usuario,
            accion="cierre_caja",
            entidad="caja",
            detalle=f"Caja cerrada. Monto: {monto_cierre}. Diferencia: {diferencia}",
            valor_antes=str(resultado.get("efectivo_esperado", "")),
            valor_nuevo=str(monto_cierre),
        )
        resultado["caja"] = obtener_resumen_caja_dia()
        resultado["arqueos"] = obtener_historial_arqueos(6)
    return jsonify(resultado), status


@app.route("/api/ventas/vendido")
@login_required
@tenant_scope_required
@sede_scope_required
def api_vendido_dia():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    producto = request.args.get("producto", "")
    if producto:
        vendido = obtener_vendido_dia_producto(fecha, producto)
        return jsonify({"vendido": vendido})
    return jsonify({"vendido": 0})


@app.route("/api/producto", methods=["POST"])
@login_required
def api_agregar_producto():
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    categoria = str(data.get("categoria", "Panaderia") or "").strip() or "Panaderia"
    es_adicional = bool(data.get("es_adicional", False))
    surtido_tipo = _parsear_surtido_tipo(data.get("surtido_tipo"))
    raw_es_panaderia = data.get("es_panaderia")
    es_panaderia = None if raw_es_panaderia in (None, "") else str(raw_es_panaderia).strip().lower() in {
        "1", "true", "si", "sí", "yes", "on"
    }
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    try:
        precio = float(data.get("precio", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Precio invalido"}), 400

    # Verificar límite de productos del plan (platform_superadmin no tiene límite)
    if _rol_usuario_actual() != PLATFORM_ADMIN_ROLE:
        panaderia_id = _panaderia_actual_id()
        if panaderia_id:
            limite = TenantService.check_limite_productos(int(panaderia_id))
            if not limite.get("puede_agregar", True):
                return jsonify({
                    "ok": False,
                    "error": f"Límite de productos alcanzado ({limite['actual']}/{limite['maximo']}). "
                             "Actualiza tu plan para agregar más.",
                    "code": "plan_limit_productos",
                }), 402

    ok = agregar_producto(
        nombre,
        precio,
        categoria,
        es_adicional=es_adicional,
        es_panaderia=es_panaderia,
        surtido_tipo=surtido_tipo,
    )
    status = 200 if ok else 409
    return jsonify({"ok": ok, "error": None if ok else "Ese producto ya existe en esa categoria"}), status


@app.route("/api/producto/<int:producto_id>", methods=["PUT"])
@login_required
def api_actualizar_producto_completo(producto_id):
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    categoria = str(data.get("categoria", "Panaderia") or "").strip() or "Panaderia"
    es_adicional = bool(data.get("es_adicional", False))
    surtido_tipo = _parsear_surtido_tipo(data.get("surtido_tipo"))
    raw_es_panaderia = data.get("es_panaderia")
    es_panaderia = None if raw_es_panaderia in (None, "") else str(raw_es_panaderia).strip().lower() in {
        "1", "true", "si", "sí", "yes", "on"
    }

    try:
        precio = float(data.get("precio", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Precio invalido"}), 400

    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400

    ok = actualizar_producto_completo(
        producto_id,
        nombre,
        precio,
        categoria,
        es_adicional,
        es_panaderia=es_panaderia,
        surtido_tipo=surtido_tipo,
    )
    return jsonify({"ok": ok, "error": None if ok else "No se pudo actualizar el producto"})


@app.route("/api/producto/<int:producto_id>", methods=["DELETE"])
@login_required
def api_eliminar_producto(producto_id):
    nombre = request.args.get("nombre", str(producto_id))
    ok = eliminar_producto_por_id(producto_id)
    if ok:
        usuario = session.get("usuario", {}).get("nombre", "")
        registrar_audit(
            usuario=usuario,
            accion="eliminar_producto",
            entidad="producto",
            entidad_id=str(producto_id),
            detalle=f"Producto eliminado: {nombre}",
        )
    return jsonify({"ok": ok, "error": None if ok else "No se pudo eliminar el producto"})


@app.route("/api/productos/importar", methods=["POST"])
@login_required
def api_importar_productos():
    archivo = request.files.get("archivo")
    if archivo is None:
        return jsonify({"ok": False, "error": "Adjunta un archivo .xlsx o .csv"}), 400

    try:
        productos, errores = _extraer_catalogo_productos(archivo)
        resultado = guardar_catalogo_productos(productos, sincronizar=True)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        app.logger.exception("Error importando catalogo de productos")
        return jsonify({"ok": False, "error": "No se pudo importar el catalogo"}), 500

    return jsonify({
        "ok": True,
        "creados": resultado["creados"],
        "actualizados": resultado["actualizados"],
        "desactivados": resultado.get("desactivados", 0),
        "errores": errores,
        "procesados": len(productos),
    })


@app.route("/api/categoria-producto", methods=["POST"])
@login_required
def api_agregar_categoria_producto():
    data = request.json
    nombre = data.get("nombre", "").strip()
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    ok = agregar_categoria_producto(nombre)
    return jsonify({"ok": ok})

@app.route("/api/categoria-producto/<path:nombre>", methods=["DELETE"])
@login_required
def api_eliminar_categoria_producto(nombre):
    from data.database import eliminar_categoria_producto
    if not nombre or nombre.strip() == "":
        return jsonify({"ok": False, "error": "Nombre de categoria invalido"}), 400
        
    resultado = eliminar_categoria_producto(nombre.strip())
    return jsonify(resultado)


@app.route("/api/producto/precio", methods=["PUT"])
@login_required
def api_actualizar_precio():
    data = request.json
    nombre = data.get("nombre", "")
    precio_nuevo = float(data.get("precio", 0))
    precio_anterior = data.get("precio_anterior")
    ok = actualizar_precio(nombre, precio_nuevo)
    if ok:
        usuario = session.get("usuario", {}).get("nombre", "")
        registrar_audit(
            usuario=usuario,
            accion="cambio_precio",
            entidad="producto",
            entidad_id=nombre,
            detalle=f"Precio actualizado: {nombre}",
            valor_antes=str(precio_anterior) if precio_anterior is not None else "",
            valor_nuevo=str(precio_nuevo),
        )
    return jsonify({"ok": ok})


@app.route("/api/producto/categoria", methods=["PUT"])
@login_required
def api_actualizar_categoria_producto():
    data = request.json
    nombre = data.get("nombre", "").strip()
    categoria = data.get("categoria", "").strip()
    if not nombre or not categoria:
        return jsonify({"ok": False, "error": "Datos incompletos"}), 400
    ok = actualizar_categoria_producto(nombre, categoria)
    return jsonify({"ok": ok})


@app.route("/api/producto/adicional", methods=["PUT"])
@login_required
def api_actualizar_producto_adicional():
    data = request.json
    nombre = data.get("nombre", "").strip()
    es_adicional = bool(data.get("es_adicional", False))
    if not nombre:
        return jsonify({"ok": False, "error": "Producto invalido"}), 400
    ok = actualizar_producto_adicional(nombre, es_adicional)
    return jsonify({"ok": ok})

@app.route("/api/mesa/<int:mesa_id>/unir-cuentas", methods=["POST"])
@login_required
def api_unir_cuentas_mesa(mesa_id):
    from data.database import unir_cuentas_mesa
    res = unir_cuentas_mesa(mesa_id)
    if res.get("ok"):
        return jsonify(res)
    return jsonify(res), 400


@app.route("/api/config/codigo-caja", methods=["PUT"])
@login_required
@admin_required
def api_guardar_codigo_caja():
    data = request.get_json(silent=True) or {}
    codigo = str(data.get("codigo", "") or "").strip()
    if len(codigo) < 4:
        return jsonify({"ok": False, "error": "El codigo debe tener al menos 4 caracteres"}), 400
    ok = guardar_codigo_verificacion_caja(codigo)
    return jsonify({"ok": ok})


@app.route("/api/usuario", methods=["POST"])
@login_required
def api_agregar_usuario():
    if not _rol_es_admin():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    pin = str(data.get("pin", "") or "").strip()
    username = str(data.get("username", "") or "").strip()
    rol = str(data.get("rol", "cajero") or "cajero").strip().lower()
    sede_id = data.get("sede_id")
    if not nombre or not pin:
        return jsonify({"ok": False, "error": "Llena nombre y PIN"}), 400
    if rol not in VALID_ROLES:
        return jsonify({"ok": False, "error": "Rol invalido"}), 400
    if len(pin) < 4:
        return jsonify({"ok": False, "error": "El PIN debe tener al menos 4 caracteres"}), 400

    resultado = agregar_usuario(
        nombre,
        pin,
        rol,
        username=username,
        sede_id=sede_id,
    )
    if not resultado.get("ok"):
        error_text = str(resultado.get("error", "")).lower()
        status = 409 if "pin" in error_text or "username" in error_text else 400
        return jsonify(resultado), status
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=_panaderia_actual_id(),
        sede_id=_sede_actual_id(),
        ip=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="crear_usuario",
        entidad="usuario",
        detalle=f"Usuario creado: {nombre} ({rol}) @ {resultado.get('username')}",
        resultado="ok",
    )
    return jsonify(resultado)


@app.route("/api/usuario/<int:uid>", methods=["PUT"])
@login_required
def api_actualizar_usuario(uid):
    if not _rol_es_admin():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    rol = str(data.get("rol", "") or "").strip().lower()
    pin = str(data.get("pin", "") or "").strip()
    username = str(data.get("username", "") or "").strip()
    sede_id = data.get("sede_id")
    if not nombre:
        return jsonify({"ok": False, "error": "El nombre es obligatorio"}), 400
    if rol not in VALID_ROLES:
        return jsonify({"ok": False, "error": "Rol invalido"}), 400
    if not username:
        return jsonify({"ok": False, "error": "El username es obligatorio"}), 400
    if pin and len(pin) < 4:
        return jsonify({"ok": False, "error": "El PIN debe tener al menos 4 caracteres"}), 400
    resultado = actualizar_usuario(uid, nombre, rol, pin, username=username, sede_id=sede_id)
    if not resultado.get("ok"):
        error_text = str(resultado.get("error", "")).lower()
        status = 409 if "pin" in error_text or "username" in error_text else 400
        return jsonify(resultado), status
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=_panaderia_actual_id(),
        sede_id=_sede_actual_id(),
        ip=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="editar_usuario",
        entidad="usuario",
        entidad_id=str(uid),
        detalle=f"Usuario actualizado: {nombre} ({rol}) @ {resultado.get('username')}",
        resultado="ok",
    )
    return jsonify(resultado)


@app.route("/api/usuario/<int:uid>/reset-pin", methods=["POST"])
@login_required
def api_resetear_pin_usuario(uid):
    if not _rol_es_admin():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    panaderia_id = _panaderia_actual_id()
    if not panaderia_id:
        return jsonify({"ok": False, "error": "Sin contexto de panaderia"}), 400
    data = request.get_json(silent=True) or {}
    nuevo_pin = str(data.get("pin", "") or "").strip()
    if nuevo_pin and len(nuevo_pin) < 4:
        return jsonify({"ok": False, "error": "El PIN debe tener al menos 4 caracteres"}), 400
    resultado = resetear_pin_usuario(uid, int(panaderia_id), nuevo_pin=nuevo_pin)
    if not resultado.get("ok"):
        error_text = str(resultado.get("error", "")).lower()
        status = 409 if "pin" in error_text else 400
        return jsonify(resultado), status
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=panaderia_id,
        sede_id=_sede_actual_id(),
        ip=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="reset_pin_usuario",
        entidad="usuario",
        entidad_id=str(uid),
        detalle="PIN operativo reseteado e invalida sesion previa",
        resultado="ok",
    )
    return jsonify(resultado)


@app.route("/api/usuario/<int:uid>", methods=["DELETE"])
@login_required
def api_eliminar_usuario(uid):
    if not _rol_es_admin():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    # Desactivar en vez de eliminar para preservar historial y auditoría.
    # La desactivación invalida inmediatamente todas las sesiones del usuario.
    ok = set_usuario_activo(uid, False)
    if not ok:
        return jsonify({"ok": False, "error": "No se pudo desactivar el usuario"}), 400
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=_panaderia_actual_id(),
        sede_id=_sede_actual_id(),
        ip=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="desactivar_usuario",
        entidad="usuario",
        entidad_id=str(uid),
        detalle="Usuario desactivado — sesiones invalidadas",
        resultado="ok",
    )
    return jsonify({"ok": True})


@app.route("/api/usuario/<int:uid>/toggle-activo", methods=["POST"])
@login_required
def api_toggle_usuario_activo(uid):
    """Activa o desactiva un usuario. Desactivar invalida sus sesiones inmediatamente."""
    if not _rol_es_admin():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    activo = bool(data.get("activo", False))
    ok = set_usuario_activo(uid, activo)
    if not ok:
        return jsonify({"ok": False, "error": "No se pudo actualizar el estado"}), 400
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=_panaderia_actual_id(),
        sede_id=_sede_actual_id(),
        ip=request.remote_addr or "",
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="activar_usuario" if activo else "desactivar_usuario",
        entidad="usuario",
        entidad_id=str(uid),
        detalle=f"Usuario {'activado' if activo else 'desactivado — sesiones invalidadas'}",
        resultado="ok",
    )
    return jsonify({"ok": True, "activo": activo})


# ── API Pedidos ──

@app.route("/api/pedido", methods=["POST"])
@login_required
def api_crear_pedido():
    data = request.get_json(silent=True) or {}
    if not data or "items" not in data or not data["items"]:
        return jsonify({"ok": False, "error": "Sin items"}), 400

    try:
        mesa_id = int(data.get("mesa_id", 0) or 0)
    except (TypeError, ValueError):
        mesa_id = 0
    if mesa_id <= 0:
        return jsonify({"ok": False, "error": "Mesa invalida"}), 400

    pedido_id = data.get("pedido_id")
    if pedido_id not in (None, ""):
        try:
            pedido_id = int(pedido_id)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Pedido invalido"}), 400
    else:
        pedido_id = None

    notas = data.get("notas", "")
    usuario_actual = _nombre_usuario_actual()
    rol_actual = _rol_usuario_actual()
    motivo_actualizacion = str(data.get("motivo_actualizacion", "") or "").strip()

    if rol_actual not in ("mesero", "cajero"):
        return jsonify({"ok": False, "error": "No autorizado"}), 403

    catalogo_productos_por_id = {
        int(p.get("id", 0) or 0): p
        for p in obtener_productos_con_precio()
        if int(p.get("id", 0) or 0) > 0
    }
    catalogo_productos = {
        _normalizar_texto(p.get("nombre", "")): p
        for p in obtener_productos_con_precio()
    }
    catalogo_adicionales = {
        _normalizar_texto(a.get("nombre", "")): a
        for a in obtener_adicionales()
    }

    items = []
    for item in data["items"]:
        try:
            producto_id = int(item.get("producto_id", 0) or 0)
        except (TypeError, ValueError):
            producto_id = 0
        producto = str(item.get("producto", "") or "").strip()
        try:
            cantidad = int(item.get("cantidad", 0) or 0)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": f"Cantidad invalida para {producto or 'el producto'}"}), 400
        producto_info = catalogo_productos_por_id.get(producto_id) if producto_id > 0 else None
        if producto_info is None:
            producto_info = catalogo_productos.get(_normalizar_texto(producto))

        if not producto or not producto_info:
            return jsonify({"ok": False, "error": f"Producto invalido: {producto or '--'}"}), 400
        if cantidad <= 0:
            return jsonify({"ok": False, "error": f"Cantidad invalida para {producto_info['nombre']}"}), 400

        entry = {
            "producto_id": int(producto_info.get("id", 0) or 0) or None,
            "producto": producto_info["nombre"],
            "cantidad": cantidad,
            "precio_unitario": float(producto_info.get("precio", 0) or 0),
            "notas": item.get("notas", ""),
        }
        # Procesar modificaciones (adicionales/exclusiones)
        if "modificaciones" in item:
            entry["modificaciones"] = []
            for mod in item["modificaciones"]:
                descripcion = str(mod.get("descripcion", "") or "").strip()
                tipo = mod.get("tipo", "adicional")
                try:
                    cantidad = int(mod.get("cantidad", 1) or 0)
                except (TypeError, ValueError):
                    return jsonify({"ok": False, "error": f"Cantidad invalida para la modificacion {descripcion or '--'}"}), 400
                if not descripcion:
                    continue
                if tipo == "adicional" and cantidad <= 0:
                    continue
                precio_extra = 0.0
                if tipo == "adicional":
                    adicional_info = catalogo_adicionales.get(_normalizar_texto(descripcion))
                    if not adicional_info:
                        return jsonify({"ok": False, "error": f"Adicional invalido: {descripcion}"}), 400
                    descripcion = adicional_info["nombre"]
                    precio_extra = float(adicional_info.get("precio", 0) or 0)
                if tipo == "exclusion":
                    cantidad = 1
                entry["modificaciones"].append({
                    "tipo": tipo,
                    "descripcion": descripcion,
                    "cantidad": cantidad,
                    "precio_extra": precio_extra,
                })
        items.append(entry)

    # Validar stock real: aplica a TODOS los productos con producción registrada hoy
    validacion = validar_stock_pedido(items, excluir_pedido_id=pedido_id)
    if not validacion["ok"]:
        return jsonify({
            "ok": False,
            "error": validacion["error"],
            "faltantes": validacion["faltantes"],
        }), 400

    if pedido_id is not None:
        if rol_actual != "cajero":
            return jsonify({"ok": False, "error": "Solo caja puede editar pedidos existentes"}), 403
        resultado = actualizar_pedido(
            pedido_id,
            usuario_actual,
            items,
            notas,
            motivo=motivo_actualizacion,
            rol=rol_actual,
        )
        status = int(resultado.pop("status", 200 if resultado.get("ok") else 400))
        if resultado.get("ok"):
            resultado["accion"] = "actualizado"
            registrar_audit(
                usuario=usuario_actual,
                accion="editar_pedido",
                entidad="pedido",
                entidad_id=str(pedido_id),
                detalle=f"Pedido #{pedido_id} ajustado. Motivo: {motivo_actualizacion}",
            )
        return jsonify(resultado), status

    if rol_actual != "mesero":
        return jsonify({"ok": False, "error": "Solo los meseros pueden crear nuevos pedidos de mesa"}), 403

    resultado = crear_pedido(mesa_id, usuario_actual, items, notas)
    if not resultado.get("ok"):
        status = 400 if "mesa" in str(resultado.get("error", "")).lower() else 500
        return jsonify(resultado), status
    return jsonify(resultado)


@app.route("/api/pedido/<int:pedido_id>/estado", methods=["PUT"])
@login_required
def api_cambiar_estado(pedido_id):
    data = request.get_json(silent=True) or {}
    nuevo_estado = data.get("estado", "")
    if nuevo_estado not in ("pendiente", "en_preparacion", "listo", "pagado", "cancelado"):
        return jsonify({"ok": False, "error": "Estado invalido"}), 400

    pedido = obtener_pedido(pedido_id)
    if not pedido:
        return jsonify({"ok": False, "error": "Pedido no encontrado"}), 404
    estado_actual = str(pedido.get("estado", "") or "").strip()
    if estado_actual == nuevo_estado:
        return jsonify({"ok": True, "pedido": pedido})
    if estado_actual in ("pagado", "cancelado"):
        return jsonify({"ok": False, "error": "Este pedido ya no admite cambios"}), 400
    if not _pedido_visible_para_usuario(pedido):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    if _rol_usuario_actual() == "mesero" and nuevo_estado != "cancelado":
        return jsonify({"ok": False, "error": "El mesero solo puede cancelar sus pedidos"}), 403

    if nuevo_estado == "pagado":
        validacion = validar_stock_pedido(
            pedido["items"], fecha=pedido["fecha"], excluir_pedido_id=pedido_id
        )
        if not validacion["ok"]:
            return jsonify({
                "ok": False,
                "error": validacion["error"],
                "faltantes": validacion["faltantes"],
            }), 400
        usuario = session["usuario"]["nombre"] if "usuario" in session else ""
        metodo_pago = str(data.get("metodo_pago", "efectivo") or "efectivo").strip().lower()
        monto_recibido = data.get("monto_recibido")
        if monto_recibido is not None:
            try:
                monto_recibido = float(monto_recibido)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "Monto recibido invalido"}), 400
        raw_mp2 = data.get("metodo_pago_2")
        raw_monto2 = data.get("monto_pago_2")
        metodo_pago_2 = str(raw_mp2).strip().lower() if raw_mp2 else None
        try:
            monto_pago_2 = float(raw_monto2) if raw_monto2 not in (None, "", "null") else None
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Monto del segundo metodo invalido"}), 400
        resultado = pagar_pedido(
            pedido_id,
            registrado_por=usuario,
            metodo_pago=metodo_pago,
            monto_recibido=monto_recibido,
            metodo_pago_2=metodo_pago_2,
            monto_pago_2=monto_pago_2,
            cliente_id=data.get("cliente_id"),
            cliente_nombre_snapshot=data.get("cliente_nombre_snapshot", ""),
            fecha_vencimiento_credito=data.get("fecha_vencimiento_credito"),
            usuario_id=_usuario_actual_id(),
        )
        status = 200 if resultado.get("ok") else 400
        if resultado.get("ok"):
            resultado["pedido"] = obtener_pedido(pedido_id)
            resultado["caja"] = obtener_resumen_caja_dia()
        return jsonify(resultado), status
    else:
        usuario = session["usuario"]["nombre"] if "usuario" in session else ""
        motivo_cancelacion = str(data.get("motivo_cancelacion", "") or "").strip()
        detalle_estado = None
        if nuevo_estado == "cancelado":
            detalle_estado = "Pedido cancelado"
            if motivo_cancelacion:
                detalle_estado = f"{detalle_estado}. Motivo: {motivo_cancelacion}"
        ok = cambiar_estado_pedido(
            pedido_id,
            nuevo_estado,
            cambiado_por=usuario,
            detalle=detalle_estado,
        )
        if ok and nuevo_estado == "cancelado":
            detalle_audit = f"Pedido #{pedido_id} cancelado"
            if motivo_cancelacion:
                detalle_audit = f"{detalle_audit}. Motivo: {motivo_cancelacion}"
            registrar_audit(
                usuario=usuario,
                accion="cancelar_pedido",
                entidad="pedido",
                entidad_id=str(pedido_id),
                detalle=detalle_audit,
            )
    respuesta = {"ok": ok}
    if ok and nuevo_estado == "listo":
        comandas = obtener_comandas_por_pedido(pedido_id)
        if comandas:
            respuesta["comanda_id"] = comandas[0].get("id")
    return jsonify(respuesta)


@app.route("/api/pedido/<int:pedido_id>/split-pay", methods=["POST"])
@login_required
def api_dividir_y_cobrar_pedido(pedido_id):
    if _rol_usuario_actual() == "mesero":
        return jsonify({"ok": False, "error": "El mesero no puede cobrar pedidos"}), 403

    data = request.get_json(silent=True) or {}
    metodo_pago = str(data.get("metodo_pago", "efectivo") or "efectivo").strip().lower()
    monto_recibido = data.get("monto_recibido")
    if monto_recibido is not None:
        try:
            monto_recibido = float(monto_recibido)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "Monto recibido invalido"}), 400
    raw_mp2 = data.get("metodo_pago_2")
    raw_monto2 = data.get("monto_pago_2")
    metodo_pago_2 = str(raw_mp2).strip().lower() if raw_mp2 else None
    try:
        monto_pago_2 = float(raw_monto2) if raw_monto2 not in (None, "", "null") else None
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Monto del segundo metodo invalido"}), 400

    resultado = dividir_pedido_y_cobrar(
        pedido_id,
        data.get("selecciones", []),
        registrado_por=_nombre_usuario_actual(),
        metodo_pago=metodo_pago,
        monto_recibido=monto_recibido,
        metodo_pago_2=metodo_pago_2,
        monto_pago_2=monto_pago_2,
        cliente_id=data.get("cliente_id"),
        cliente_nombre_snapshot=data.get("cliente_nombre_snapshot", ""),
        fecha_vencimiento_credito=data.get("fecha_vencimiento_credito"),
        usuario_id=_usuario_actual_id(),
    )
    status = 200 if resultado.get("ok") else 400
    if resultado.get("ok"):
        resultado["caja"] = obtener_resumen_caja_dia()
    return jsonify(resultado), status


@app.route("/api/pedido/<int:pedido_id>")
@login_required
def api_obtener_pedido(pedido_id):
    pedido = obtener_pedido(pedido_id)
    if pedido and not _pedido_visible_para_usuario(pedido):
        return jsonify({"error": "No autorizado"}), 403
    if pedido:
        pedido["comandas"] = obtener_comandas_por_pedido(pedido_id)
        ultima_comanda_resumen = pedido["comandas"][0] if pedido["comandas"] else None
        pedido["ultima_comanda"] = (
            obtener_comanda(int(ultima_comanda_resumen.get("id", 0) or 0))
            if ultima_comanda_resumen
            else None
        )
        pedido["trazabilidad"] = pedido.get("trazabilidad") or obtener_trazabilidad_pedido(pedido_id)
        return jsonify(pedido)
    return jsonify({"error": "Pedido no encontrado"}), 404


@app.route("/api/pedido/<int:pedido_id>/comandas")
@login_required
def api_obtener_comandas_pedido(pedido_id):
    pedido = obtener_pedido(pedido_id)
    if not pedido:
        return jsonify({"ok": False, "error": "Pedido no encontrado"}), 404
    if not _pedido_visible_para_usuario(pedido):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    return jsonify({
        "ok": True,
        "pedido_id": pedido_id,
        "comandas": obtener_comandas_por_pedido(pedido_id),
    })


@app.route("/api/mesa/<int:mesa_id>/pedido-activo")
@login_required
def api_obtener_pedido_activo_mesa(mesa_id: int):
    if not _puede_gestionar_mesas():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403

    mesa = obtener_mesa(mesa_id)
    if not mesa:
        return jsonify({"ok": False, "error": "Mesa no encontrada"}), 404

    pedido = obtener_pedido_activo_mesa(mesa_id)
    if not pedido:
        return jsonify({"ok": False, "error": "La mesa no tiene un pedido activo"}), 404
    if not _pedido_visible_para_usuario(pedido):
        return jsonify({"ok": False, "error": "No autorizado"}), 403

    comandas = obtener_comandas_por_pedido(int(pedido.get("id", 0) or 0))
    ultima_comanda = obtener_comanda(int(comandas[0]["id"])) if comandas else None
    pedido["comandas"] = comandas
    pedido["ultima_comanda"] = ultima_comanda
    pedido["trazabilidad"] = pedido.get("trazabilidad") or obtener_trazabilidad_pedido(int(pedido.get("id", 0) or 0))
    return jsonify({
        "ok": True,
        "mesa": mesa,
        "pedido": pedido,
        "comanda": ultima_comanda,
    })


@app.route("/comanda/<int:comanda_id>/print")
@login_required
def ver_comanda_print(comanda_id):
    comanda = obtener_comanda(comanda_id)
    if not comanda:
        abort(404)
    pedido = obtener_pedido(int(comanda.get("pedido_id", 0) or 0))
    if not pedido:
        abort(404)
    if not _pedido_visible_para_usuario(pedido):
        abort(403)

    branding = obtener_branding_panaderia(comanda.get("panaderia_id"))
    auto_print = str(request.args.get("auto", "") or "").strip().lower() in {"1", "true", "si", "sí", "yes"}
    print_mode = str(request.args.get("mode", "imprimir") or "imprimir").strip().lower()
    if print_mode not in {"imprimir", "reimprimir"}:
        print_mode = "imprimir"

    return render_template(
        "comanda_print.html",
        comanda=comanda,
        branding=branding,
        auto_print=auto_print,
        print_mode=print_mode,
        layout="standalone",
    )


@app.route("/api/comanda/<int:comanda_id>/imprimir", methods=["POST"])
@login_required
def api_imprimir_comanda(comanda_id):
    comanda = obtener_comanda(comanda_id)
    if not comanda:
        return jsonify({"ok": False, "error": "Comanda no encontrada"}), 404
    pedido = obtener_pedido(int(comanda.get("pedido_id", 0) or 0))
    if not pedido or not _pedido_visible_para_usuario(pedido):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    ok = marcar_comanda_impresa(
        comanda_id,
        actor_nombre=_nombre_usuario_actual(),
        actor_id=_usuario_actual_id(),
    )
    return jsonify({"ok": ok, "estado": "impresa" if ok else "error"}), (200 if ok else 500)


@app.route("/api/comanda/<int:comanda_id>/reimprimir", methods=["POST"])
@login_required
def api_reimprimir_comanda(comanda_id):
    comanda = obtener_comanda(comanda_id)
    if not comanda:
        return jsonify({"ok": False, "error": "Comanda no encontrada"}), 404
    pedido = obtener_pedido(int(comanda.get("pedido_id", 0) or 0))
    if not pedido or not _pedido_visible_para_usuario(pedido):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    ok = marcar_comanda_reimpresa(
        comanda_id,
        actor_nombre=_nombre_usuario_actual(),
        actor_id=_usuario_actual_id(),
    )
    return jsonify({"ok": ok, "estado": "reimpresa" if ok else "error"}), (200 if ok else 500)


def _roles_documento_ok() -> set[str]:
    return {"cajero", "panadero", "tenant_admin", "platform_superadmin"}


def _datos_documentales_desde_request(data: dict | None = None) -> dict:
    data = data or {}
    return {
        "cliente_id": data.get("cliente_id"),
        "nombre": str(data.get("nombre") or data.get("nombre_comprador") or "").strip(),
        "tipo_doc": str(data.get("tipo_doc") or "").strip(),
        "numero_doc": str(data.get("numero_doc") or "").strip(),
        "email": str(data.get("email") or data.get("email_comprador") or "").strip(),
        "direccion": str(data.get("direccion") or "").strip(),
        "empresa": str(data.get("empresa") or "").strip(),
        "tipo_documento": str(data.get("tipo_documento") or "factura").strip().lower() or "factura",
    }


def _documento_visible_para_usuario(documento: dict | None) -> bool:
    if not documento:
        return False
    panaderia_id = int(documento.get("panaderia_id") or 0)
    if panaderia_id and panaderia_id != int(_panaderia_actual_id() or 0):
        return False

    origen_tipo = str(documento.get("origen_tipo") or "").strip().lower()
    origen_id = int(documento.get("origen_id") or 0)
    if origen_tipo == "pedido":
        pedido = obtener_pedido(origen_id)
        return bool(pedido and _pedido_visible_para_usuario(pedido))

    return _rol_usuario_actual() in _roles_documento_ok()


def _build_documento_payload(origen_tipo: str, origen_id: int, datos_documentales: dict) -> dict:
    tipo_documento = datos_documentales.get("tipo_documento") or "factura"
    if origen_tipo == "venta":
        return build_documento_payload_desde_venta(
            venta_id=origen_id,
            datos_cliente=datos_documentales,
            tipo_documento=tipo_documento,
        )
    if origen_tipo == "pedido":
        return build_documento_payload_desde_pedido(
            pedido_id=origen_id,
            datos_cliente=datos_documentales,
            tipo_documento=tipo_documento,
        )
    if origen_tipo == "encargo":
        return build_documento_payload_desde_encargo(
            encargo_id=origen_id,
            datos_cliente=datos_documentales,
            tipo_documento=tipo_documento,
        )
    raise ValueError("Origen de documento no soportado")


def _crear_documento_desde_origen(origen_tipo: str, origen_id: int, datos_documentales: dict) -> dict:
    payload = _build_documento_payload(origen_tipo, origen_id, datos_documentales)
    usuario_id = _usuario_actual_id()
    resultado = crear_documento_emitido(
        origen_tipo=origen_tipo,
        origen_id=origen_id,
        payload=payload,
        usuario_id=usuario_id,
    )
    documento = obtener_documento_emitido(resultado["documento_id"])
    _log_event(
        "documento_generado",
        request_id=getattr(g, "request_id", ""),
        origen_tipo=origen_tipo,
        origen_id=origen_id,
        documento_id=resultado["documento_id"],
        consecutivo=resultado.get("consecutivo"),
        usuario=_nombre_usuario_actual(),
    )
    return {
        "ok": True,
        "documento_id": resultado["documento_id"],
        "consecutivo": resultado["consecutivo"],
        "documento": documento,
        "print_url": url_for("ver_documento_print", documento_id=resultado["documento_id"]),
    }


def _smtp_config() -> dict:
    return {
        "host": str(os.environ.get("SMTP_HOST", "") or "").strip(),
        "port": int(os.environ.get("SMTP_PORT", "587") or 587),
        "user": str(os.environ.get("SMTP_USER", "") or "").strip(),
        "password": str(os.environ.get("SMTP_PASSWORD", "") or "").strip(),
        "from": str(os.environ.get("SMTP_FROM", "") or "").strip(),
        "use_tls": str(os.environ.get("SMTP_USE_TLS", "true") or "true").strip().lower() not in {"0", "false", "no"},
    }


def _smtp_disponible() -> bool:
    config = _smtp_config()
    return bool(config["host"] and config["from"])


def _documento_logo_data_uri(payload: dict) -> str:
    negocio = payload.get("negocio") or {}
    logo_path = str(negocio.get("logo_path") or "").strip()
    if not logo_path:
        return ""
    abs_path = os.path.join(app.static_folder or "", logo_path.replace("/", os.sep))
    if not os.path.isfile(abs_path):
        return ""
    ext = os.path.splitext(abs_path)[1].lower()
    mime = {
        ".svg": "image/svg+xml",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }.get(ext)
    if not mime:
        return ""
    try:
        with open(abs_path, "rb") as fh:
            encoded = base64.b64encode(fh.read()).decode("ascii")
        return f"data:{mime};base64,{encoded}"
    except OSError:
        return ""


def _enviar_documento_email(documento: dict, email_destino: str) -> None:
    config = _smtp_config()
    if not _smtp_disponible():
        raise RuntimeError("Correo no configurado. Define SMTP_HOST y SMTP_FROM para habilitar envios.")

    payload = documento.get("payload") or {}
    negocio = payload.get("negocio") or {}
    cliente = payload.get("cliente") or {}
    asunto = f"{str(payload.get('tipo_documento') or 'documento').replace('_', ' ').capitalize()} {documento.get('consecutivo')}"
    html_documento = render_template(
        "factura_print.html",
        documento=documento,
        payload=payload,
        auto_print=False,
        print_mode="imprimir",
        email_mode=True,
        logo_data_uri=_documento_logo_data_uri(payload),
        smtp_ready=_smtp_disponible(),
        layout="standalone",
    )
    html_body = render_template(
        "documento_email.html",
        documento=documento,
        payload=payload,
        cliente=cliente,
        negocio=negocio,
    ) if False else f"""
    <html>
      <body style="font-family:Segoe UI,Arial,sans-serif;color:#23160d;">
        <p>Hola {cliente.get('nombre') or 'cliente'},</p>
        <p>Adjuntamos tu {str(payload.get('tipo_documento') or 'documento').replace('_', ' ')} {documento.get('consecutivo')}.</p>
        <p>Total: ${float((payload.get('totales') or {}).get('total', 0) or 0):,.2f}</p>
        <p>{negocio.get('brand_name') or 'Panaderia'}</p>
      </body>
    </html>
    """

    message = EmailMessage()
    message["Subject"] = asunto
    message["From"] = config["from"]
    message["To"] = email_destino
    message.set_content(
        "\n".join(
            [
                f"Hola {cliente.get('nombre') or 'cliente'},",
                "",
                f"Adjuntamos tu documento {documento.get('consecutivo')}.",
                f"Total: ${float((payload.get('totales') or {}).get('total', 0) or 0):,.2f}",
                "",
                str(negocio.get("brand_name") or "Panaderia"),
            ]
        )
    )
    message.add_alternative(html_body, subtype="html")
    filename = f"{documento.get('consecutivo') or 'documento'}.html"
    message.add_attachment(html_documento.encode("utf-8"), maintype="text", subtype="html", filename=filename)

    with smtplib.SMTP(config["host"], config["port"], timeout=20) as server:
        if config["use_tls"]:
            server.starttls()
        if config["user"]:
            server.login(config["user"], config["password"])
        server.send_message(message)


@app.route("/documento/<int:documento_id>/print")
@login_required
def ver_documento_print(documento_id):
    documento = obtener_documento_emitido(documento_id)
    if not documento:
        abort(404)
    if not _documento_visible_para_usuario(documento):
        abort(403)

    auto_print = str(request.args.get("auto", "") or "").strip().lower() in {"1", "true", "si", "sí", "yes"}
    print_mode = str(request.args.get("mode", "imprimir") or "imprimir").strip().lower()
    if print_mode not in {"imprimir", "reimprimir"}:
        print_mode = "imprimir"

    return render_template(
        "factura_print.html",
        documento=documento,
        payload=documento.get("payload") or {},
        auto_print=auto_print,
        print_mode=print_mode,
        email_mode=False,
        logo_data_uri="",
        smtp_ready=_smtp_disponible(),
        layout="standalone",
    )


@app.route("/api/documento/generar-desde-venta/<int:venta_id>", methods=["POST"])
@login_required
def api_generar_documento_desde_venta(venta_id: int):
    if _rol_usuario_actual() not in _roles_documento_ok():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    try:
        return jsonify(_crear_documento_desde_origen("venta", venta_id, _datos_documentales_desde_request(data)))
    except Exception as exc:
        _log_exception(
            "documento_generacion_error",
            exc,
            request_id=getattr(g, "request_id", ""),
            origen_tipo="venta",
            origen_id=venta_id,
            usuario=_nombre_usuario_actual(),
        )
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/documento/generar-desde-pedido/<int:pedido_id>", methods=["POST"])
@login_required
def api_generar_documento_desde_pedido(pedido_id: int):
    pedido = obtener_pedido(pedido_id)
    if not pedido:
        return jsonify({"ok": False, "error": "Pedido no encontrado"}), 404
    if not _pedido_visible_para_usuario(pedido):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    if _rol_usuario_actual() not in _roles_documento_ok():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    try:
        return jsonify(_crear_documento_desde_origen("pedido", pedido_id, _datos_documentales_desde_request(data)))
    except Exception as exc:
        _log_exception(
            "documento_generacion_error",
            exc,
            request_id=getattr(g, "request_id", ""),
            origen_tipo="pedido",
            origen_id=pedido_id,
            usuario=_nombre_usuario_actual(),
        )
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/documento/generar-desde-encargo/<int:encargo_id>", methods=["POST"])
@login_required
def api_generar_documento_desde_encargo(encargo_id: int):
    if _rol_usuario_actual() not in _roles_documento_ok():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    encargo = obtener_encargo_v2(encargo_id)
    if not encargo:
        return jsonify({"ok": False, "error": "Encargo no encontrado"}), 404
    data = request.get_json(silent=True) or {}
    try:
        return jsonify(_crear_documento_desde_origen("encargo", encargo_id, _datos_documentales_desde_request(data)))
    except Exception as exc:
        _log_exception(
            "documento_generacion_error",
            exc,
            request_id=getattr(g, "request_id", ""),
            origen_tipo="encargo",
            origen_id=encargo_id,
            usuario=_nombre_usuario_actual(),
        )
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/api/documentos", methods=["GET"])
@login_required
def api_documentos_recientes():
    if _rol_usuario_actual() not in _roles_documento_ok():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    pagination_args = _parse_pagination_args()
    fecha_desde = request.args.get("fecha_desde", "").strip()
    fecha_hasta = request.args.get("fecha_hasta", "").strip()
    try:
        fecha_desde = _parse_fecha_iso(fecha_desde) if fecha_desde else ""
    except ValueError:
        fecha_desde = ""
    try:
        fecha_hasta = _parse_fecha_iso(fecha_hasta) if fecha_hasta else ""
    except ValueError:
        fecha_hasta = ""
    documentos_data = obtener_documentos_recientes_paginados(
        page=pagination_args["page"],
        size=pagination_args["size"],
        origen_tipo=request.args.get("origen_tipo", "").strip().lower() or None,
        estado=request.args.get("estado", "").strip().lower() or None,
        tipo_documento=request.args.get("tipo_documento", "").strip().lower() or None,
        cliente=request.args.get("cliente", "").strip() or None,
        fecha_desde=fecha_desde or None,
        fecha_hasta=fecha_hasta or None,
        estado_envio=request.args.get("estado_envio", "").strip().lower() or None,
    )
    visibles = [doc for doc in documentos_data["items"] if _documento_visible_para_usuario(doc)]
    return jsonify({
        "ok": True,
        "documentos": visibles,
        "pagination": {
            **documentos_data["pagination"],
            "items_count": len(visibles),
        },
    })


@app.route("/api/documento/<int:documento_id>")
@login_required
def api_obtener_documento(documento_id: int):
    documento = obtener_documento_emitido(documento_id)
    if not documento:
        return jsonify({"ok": False, "error": "Documento no encontrado"}), 404
    if not _documento_visible_para_usuario(documento):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    return jsonify({"ok": True, "documento": documento, "smtp_ready": _smtp_disponible()})


@app.route("/api/documento/<int:documento_id>/envios")
@login_required
def api_envios_documento(documento_id: int):
    documento = obtener_documento_emitido(documento_id)
    if not documento:
        return jsonify({"ok": False, "error": "Documento no encontrado"}), 404
    if not _documento_visible_para_usuario(documento):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    return jsonify({"ok": True, "envios": obtener_envios_documento(documento_id)})


@app.route("/api/documento/<int:documento_id>/imprimir", methods=["POST"])
@login_required
def api_imprimir_documento(documento_id: int):
    documento = obtener_documento_emitido(documento_id)
    if not documento:
        return jsonify({"ok": False, "error": "Documento no encontrado"}), 404
    if not _documento_visible_para_usuario(documento):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    ok = marcar_documento_impreso(documento_id, usuario_id=_usuario_actual_id())
    return jsonify({"ok": ok, "estado": "emitido" if ok else "error"}), (200 if ok else 500)


@app.route("/api/documento/<int:documento_id>/reimprimir", methods=["POST"])
@login_required
def api_reimprimir_documento(documento_id: int):
    documento = obtener_documento_emitido(documento_id)
    if not documento:
        return jsonify({"ok": False, "error": "Documento no encontrado"}), 404
    if not _documento_visible_para_usuario(documento):
        return jsonify({"ok": False, "error": "No autorizado"}), 403
    ok = marcar_documento_reimpreso(documento_id, usuario_id=_usuario_actual_id())
    return jsonify({"ok": ok, "estado": "reimpresa" if ok else "error"}), (200 if ok else 500)


@app.route("/api/documento/<int:documento_id>/enviar", methods=["POST"])
@login_required
def api_enviar_documento(documento_id: int):
    documento = obtener_documento_emitido(documento_id)
    if not documento:
        return jsonify({"ok": False, "error": "Documento no encontrado"}), 404
    if not _documento_visible_para_usuario(documento):
        return jsonify({"ok": False, "error": "No autorizado"}), 403

    data = request.get_json(silent=True) or {}
    email_destino = str(data.get("email") or documento.get("cliente_email_snapshot") or "").strip()
    if not email_destino:
        return jsonify({"ok": False, "error": "Debes indicar un correo destino"}), 400

    try:
        _enviar_documento_email(documento, email_destino)
        registrar_envio_documento(
            documento_id=documento_id,
            email_destino=email_destino,
            estado="enviado",
            usuario_id=_usuario_actual_id(),
        )
        return jsonify({"ok": True, "email": email_destino})
    except Exception as exc:
        try:
            registrar_envio_documento(
                documento_id=documento_id,
                email_destino=email_destino,
                estado="error",
                error=str(exc),
                usuario_id=_usuario_actual_id(),
            )
        except Exception:
            app.logger.exception("No se pudo registrar el error de envio del documento")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/origen/<tipo>/<int:origen_id>/documentos")
@login_required
def api_documentos_por_origen(tipo: str, origen_id: int):
    try:
        documentos = obtener_documentos_por_origen(tipo, origen_id)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    visibles = [doc for doc in documentos if _documento_visible_para_usuario(doc)]
    return jsonify({"ok": True, "documentos": visibles})


@app.route("/api/pedidos")
@login_required
def api_obtener_pedidos():
    estado = request.args.get("estado")
    mesa_id = request.args.get("mesa_id", type=int)
    mesero = _nombre_usuario_actual() if _rol_usuario_actual() == "mesero" else None
    pedidos = obtener_pedidos(estado=estado, mesa_id=mesa_id, mesero=mesero)
    return jsonify(pedidos)


@app.route("/api/notificaciones/feed")
@login_required
def api_notificaciones_feed():
    try:
        limite = int(request.args.get("limit", 30) or 30)
    except (TypeError, ValueError):
        limite = 30
    items = _obtener_notificaciones_operativas(
        rol=_rol_usuario_actual(),
        usuario=_nombre_usuario_actual(),
        limite=limite,
    )
    return jsonify({
        "ok": True,
        "items": items,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    })


@app.route("/api/adicionales")
@login_required
def api_obtener_adicionales():
    return jsonify(_obtener_adicionales_operativos())


@app.route("/api/adicional", methods=["POST"])
@login_required
def api_agregar_adicional():
    data = request.json
    nombre = data.get("nombre", "").strip()
    precio = float(data.get("precio", 0))
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    ok = agregar_adicional(nombre, precio)
    return jsonify({"ok": ok, "error": None if ok else "Ese adicional ya existe"})


@app.route("/api/adicional/<int:aid>/precio", methods=["PUT"])
@login_required
def api_actualizar_adicional(aid):
    data = request.json
    precio = float(data.get("precio", 0))
    ok = actualizar_adicional(aid, precio)
    return jsonify({"ok": ok})


@app.route("/api/adicional/<int:aid>/configuracion", methods=["PUT"])
@login_required
def api_guardar_configuracion_adicional(aid):
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    insumos = data.get("insumos", [])
    componentes = data.get("componentes", [])
    try:
        precio = float(data.get("precio", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Precio invalido"}), 400
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre invalido"}), 400

    ok_precio = actualizar_adicional_detalle(aid, nombre, precio)
    if not ok_precio:
        return jsonify({"ok": False, "error": "No se pudo actualizar el adicional"}), 400

    ok_config = guardar_configuracion_adicional(aid, insumos, componentes)
    return jsonify({"ok": bool(ok_config), "error": None if ok_config else "No se pudo guardar la configuracion"})


@app.route("/api/adicional/<int:aid>", methods=["DELETE"])
@login_required
def api_eliminar_adicional(aid):
    ok = eliminar_adicional(aid)
    return jsonify({"ok": ok})


@app.route("/api/insumos")
@login_required
def api_obtener_insumos():
    return jsonify(obtener_insumos())


@app.route("/api/insumo", methods=["POST"])
@login_required
def api_agregar_insumo():
    data = request.json
    nombre = data.get("nombre", "").strip()
    unidad = data.get("unidad", "unidad").strip()
    stock = float(data.get("stock", 0))
    stock_minimo = float(data.get("stock_minimo", 0))
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    ok = agregar_insumo(nombre, unidad, stock, stock_minimo)
    return jsonify({"ok": ok})


@app.route("/api/insumos/importar", methods=["POST"])
@login_required
def api_importar_insumos():
    archivo = request.files.get("archivo")
    if archivo is None:
        return jsonify({"ok": False, "error": "Adjunta un archivo .xlsx o .csv"}), 400

    try:
        insumos, errores = _extraer_catalogo_insumos(archivo)
        resultado = guardar_catalogo_insumos(insumos)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        app.logger.exception("Error importando inventario de insumos")
        return jsonify({"ok": False, "error": "No se pudo importar el inventario"}), 500

    return jsonify({
        "ok": True,
        "creados": resultado["creados"],
        "actualizados": resultado["actualizados"],
        "errores": errores,
        "procesados": len(insumos),
    })


@app.route("/api/insumo/<int:iid>/stock", methods=["PUT"])
@login_required
def api_actualizar_stock(iid):
    data = request.json
    stock_nuevo = float(data.get("stock", 0))
    stock_anterior = data.get("stock_anterior")
    nombre_insumo = data.get("nombre", str(iid))
    ok = actualizar_stock(iid, stock_nuevo)
    if ok:
        usuario = session.get("usuario", {}).get("nombre", "")
        registrar_audit(
            usuario=usuario,
            accion="ajuste_inventario",
            entidad="insumo",
            entidad_id=str(iid),
            detalle=f"Stock ajustado: {nombre_insumo}",
            valor_antes=str(stock_anterior) if stock_anterior is not None else "",
            valor_nuevo=str(stock_nuevo),
        )
    return jsonify({"ok": ok})


@app.route("/api/insumo/<int:iid>", methods=["DELETE"])
@login_required
def api_eliminar_insumo(iid):
    ok = eliminar_insumo(iid)
    return jsonify({"ok": ok})


@app.route("/api/receta/<producto>")
@login_required
def api_obtener_receta(producto):
    return jsonify(obtener_receta(producto))


@app.route("/api/receta/<producto>", methods=["PUT"])
@login_required
def api_guardar_receta(producto):
    data = request.get_json(silent=True) or {}
    ingredientes = data.get("ingredientes", [])
    ficha = data.get("ficha", {})
    componentes = data.get("componentes", [])
    ok = guardar_receta(producto, ingredientes, ficha, componentes)
    return jsonify({"ok": ok})


@app.route("/api/mesa", methods=["POST"])
@login_required
def api_agregar_mesa():
    if not _puede_gestionar_mesas():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    try:
        numero = int(data.get("numero", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Numero invalido"}), 400
    nombre = (data.get("nombre", "") or "").strip()
    if numero <= 0:
        return jsonify({"ok": False, "error": "Numero invalido"}), 400
    resultado = agregar_mesa(numero, nombre)
    if not resultado.get("ok"):
        return jsonify(resultado), 400

    mesa = resultado.get("mesa") or {}
    accion = str(resultado.get("accion", "") or "").strip()
    detalle = f"Mesa {mesa.get('numero')} {accion}"
    _registrar_auditoria_mesa(resultado, f"{accion}_mesa", detalle)
    return jsonify({
        "ok": True,
        "accion": accion,
        "mesa": mesa,
        "mensaje": "Mesa agregada correctamente" if accion == "creada" else "Mesa reactivada correctamente",
    })


@app.route("/api/mesa/<int:mesa_id>", methods=["PUT"])
@login_required
def api_actualizar_mesa(mesa_id: int):
    if not _puede_gestionar_mesas():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    try:
        numero = int(data.get("numero", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Numero invalido"}), 400
    nombre = str(data.get("nombre", "") or "").strip()
    resultado = actualizar_mesa(mesa_id, numero, nombre)
    if not resultado.get("ok"):
        return jsonify(resultado), 400
    mesa = resultado.get("mesa") or {}
    _registrar_auditoria_mesa(resultado, "actualizar_mesa", f"Mesa {mesa.get('numero')} actualizada")
    return jsonify(resultado)


@app.route("/api/mesa/<int:mesa_id>/activar", methods=["POST"])
@login_required
def api_activar_mesa(mesa_id: int):
    if not _puede_gestionar_mesas():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    resultado = activar_mesa(mesa_id)
    if not resultado.get("ok"):
        return jsonify(resultado), 400
    mesa = resultado.get("mesa") or {}
    _registrar_auditoria_mesa(resultado, "activar_mesa", f"Mesa {mesa.get('numero')} activada")
    return jsonify(resultado)


@app.route("/api/mesa/<int:mesa_id>/desactivar", methods=["POST"])
@login_required
def api_desactivar_mesa(mesa_id: int):
    if not _puede_gestionar_mesas():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    resultado = desactivar_mesa(mesa_id)
    if not resultado.get("ok"):
        return jsonify(resultado), 400
    mesa = resultado.get("mesa") or {}
    _registrar_auditoria_mesa(resultado, "desactivar_mesa", f"Mesa {mesa.get('numero')} desactivada")
    return jsonify(resultado)


@app.route("/api/mesa/<int:mesa_id>", methods=["DELETE"])
@login_required
def api_eliminar_mesa(mesa_id: int):
    if not _puede_gestionar_mesas():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    resultado = eliminar_mesa(mesa_id)
    if not resultado.get("ok"):
        status = 409 if resultado.get("codigo") == "mesa_con_pedido_abierto" else 400
        return jsonify(resultado), status
    mesa = resultado.get("mesa") or {}
    _registrar_auditoria_mesa(resultado, "eliminar_mesa", f"Mesa {mesa.get('numero')} eliminada logicamente")
    return jsonify(resultado)


# ── API Backups ──

@app.route("/api/backup", methods=["POST"])
@login_required
@admin_required
def api_crear_backup():
    data = request.json or {}
    nota = data.get("nota", "Backup manual")
    result = crear_backup(nota)
    return jsonify(result)


@app.route("/api/backup/restaurar", methods=["POST"])
@login_required
@admin_required
def api_restaurar_backup():
    data = request.json
    timestamp = data.get("timestamp", "")
    if not timestamp:
        return jsonify({"ok": False, "error": "Timestamp requerido"}), 400
    result = restaurar_backup(timestamp)
    return jsonify(result)


@app.route("/api/backup/<timestamp>", methods=["DELETE"])
@login_required
@admin_required
def api_eliminar_backup(timestamp):
    result = eliminar_backup(timestamp)
    return jsonify(result)


@app.route("/api/backup/limpiar", methods=["POST"])
@login_required
@admin_required
def api_limpiar_backups():
    result = limpiar_backups_antiguos()
    return jsonify(result)


# ══════════════════════════════════════════════
# NUEVAS APIs - FASE 2
# ══════════════════════════════════════════════

# ── Top 3 productos del día ──────────────────────────────────────────────────

@app.route("/api/top-productos")
@login_required
def api_top_productos():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    limite = int(request.args.get("limite", 3))
    top = obtener_top_productos_dia(fecha=fecha, limite=limite)
    return jsonify({"ok": True, "fecha": fecha, "top": top})


@app.route("/api/surtido/sugerir", methods=["POST"])
@login_required
@roles_required("cajero", "mesero", "tenant_admin", "platform_superadmin")
def api_sugerir_surtido():
    data = request.get_json(silent=True) or {}
    try:
        valor_objetivo = _parsear_numero_positivo(data.get("valor_objetivo"), "Valor objetivo")
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    pedido_id_raw = data.get("pedido_id")
    try:
        pedido_id = int(pedido_id_raw) if pedido_id_raw not in (None, "") else None
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Pedido invalido"}), 400

    items_actuales = data.get("items_actuales", [])
    if items_actuales is None:
        items_actuales = []
    if not isinstance(items_actuales, list):
        return jsonify({"ok": False, "error": "items_actuales debe ser una lista"}), 400

    resultado = generar_surtido_por_valor(
        valor_objetivo=valor_objetivo,
        excluir_pedido_id=pedido_id,
        items_existentes=items_actuales,
    )
    status = 200 if resultado.get("ok") else 400
    return jsonify(resultado), status


# ── Alertas de stock por producto ────────────────────────────────────────────

@app.route("/api/alertas-stock")
@login_required
def api_alertas_stock():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    alertas = obtener_alertas_stock_productos(fecha=fecha)
    return jsonify({"ok": True, "fecha": fecha, "alertas": alertas})


@app.route("/api/stock-disponible")
@login_required
def api_stock_disponible():
    """Stock disponible real por producto (producido - ventas - pedidos activos)."""
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    disponibles = obtener_stock_disponible_hoy(fecha)
    for producto in obtener_productos_panaderia():
        disponibles.setdefault(producto, 0)
    return jsonify({"ok": True, "fecha": fecha, "stock": disponibles})


@app.route("/api/producto/<int:producto_id>/stock-minimo", methods=["PUT"])
@login_required
def api_actualizar_stock_minimo(producto_id):
    data = request.get_json(silent=True) or {}
    try:
        stock_minimo = int(data.get("stock_minimo", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Valor inválido"}), 400
    ok = actualizar_stock_minimo_producto(producto_id, stock_minimo)
    if ok:
        usuario = session.get("usuario", {}).get("nombre", "")
        registrar_audit(
            usuario=usuario,
            accion="cambio_stock_minimo",
            entidad="producto",
            entidad_id=str(producto_id),
            detalle=f"Stock mínimo actualizado a {stock_minimo}",
            valor_nuevo=str(stock_minimo),
        )
    return jsonify({"ok": ok})


# ── Audit Log ────────────────────────────────────────────────────────────────

@app.route("/api/audit-log")
@login_required
@admin_required
def api_audit_log():
    if not _rol_es_admin():
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    dias = int(request.args.get("dias", 30))
    limite = int(request.args.get("limite", 200))
    log = obtener_audit_log(dias=dias, limite=limite)
    return jsonify({"ok": True, "log": log})


# ── Ajuste manual de pronóstico ──────────────────────────────────────────────

@app.route("/api/pronostico/ajuste", methods=["POST"])
@login_required
def api_guardar_ajuste_pronostico():
    data = request.get_json(silent=True) or {}
    fecha = data.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    producto = str(data.get("producto", "")).strip()
    motivo = str(data.get("motivo", "")).strip()
    usuario = session.get("usuario", {}).get("nombre", "")
    try:
        sugerido = int(data.get("sugerido", 0))
        ajustado = int(data.get("ajustado", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Valores inválidos"}), 400
    if not producto:
        return jsonify({"ok": False, "error": "Producto requerido"}), 400
    ok = guardar_ajuste_pronostico(fecha, producto, sugerido, ajustado, motivo, usuario)
    if ok:
        registrar_audit(
            usuario=usuario,
            accion="ajuste_pronostico",
            entidad="pronostico",
            entidad_id=f"{fecha}/{producto}",
            detalle=f"Pronóstico ajustado: {producto} | {fecha}",
            valor_antes=str(sugerido),
            valor_nuevo=f"{ajustado} | motivo: {motivo}",
        )
    return jsonify({"ok": ok})


@app.route("/api/pronostico/ajuste")
@login_required
def api_obtener_ajuste_pronostico():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    producto = request.args.get("producto", "")
    if not producto:
        return jsonify({"ok": False, "error": "Producto requerido"}), 400
    ajuste = obtener_ajuste_pronostico(fecha, producto)
    historial = obtener_historial_ajustes(producto, dias=30)
    return jsonify({"ok": True, "ajuste": ajuste, "historial": historial})


# ── Backtesting pronóstico ────────────────────────────────────────────────────

@app.route("/api/pronostico/backtesting")
@login_required
def api_pronostico_backtesting():
    producto = request.args.get("producto", "").strip()
    if not producto:
        return jsonify({"ok": False, "error": "Producto requerido"}), 400
    ventana = int(request.args.get("ventana", 21))
    max_eval = int(request.args.get("max", 20))
    resultado = calcular_backtesting(producto, ventana_entrenamiento=ventana, max_evaluaciones=max_eval)
    return jsonify(resultado)


# ── Mermas ───────────────────────────────────────────────────────────────────

@app.route("/api/merma", methods=["POST"])
@login_required
def api_registrar_merma():
    data = request.get_json(silent=True) or {}
    producto = str(data.get("producto", "")).strip()
    notas = str(data.get("notas", "")).strip()
    tipo = str(data.get("tipo", "sobrante")).strip()
    usuario = session.get("usuario", {}).get("nombre", "")
    try:
        cantidad = float(data.get("cantidad", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Cantidad inválida"}), 400
    if not producto:
        return jsonify({"ok": False, "error": "Producto requerido"}), 400
    ok = registrar_merma(producto, cantidad, tipo=tipo, registrado_por=usuario, notas=notas)
    return jsonify({"ok": ok})


@app.route("/api/mermas")
@login_required
def api_mermas_dia():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    mermas = obtener_mermas_dia(fecha=fecha)
    resumen = obtener_resumen_mermas(dias=30)
    return jsonify({"ok": True, "fecha": fecha, "mermas": mermas, "resumen": resumen})


# ── Días especiales ───────────────────────────────────────────────────────────

@app.route("/api/dias-especiales")
@login_required
def api_dias_especiales():
    dias = obtener_dias_especiales()
    return jsonify({"ok": True, "dias_especiales": dias})


@app.route("/api/dia-especial", methods=["POST"])
@login_required
def api_guardar_dia_especial():
    if session.get("usuario", {}).get("rol") != "panadero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    fecha = str(data.get("fecha", "")).strip()
    descripcion = str(data.get("descripcion", "")).strip()
    tipo = str(data.get("tipo", "festivo")).strip()
    try:
        factor = float(data.get("factor", 1.0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Factor inválido"}), 400
    if not fecha or not descripcion:
        return jsonify({"ok": False, "error": "Fecha y descripción requeridas"}), 400
    ok = guardar_dia_especial(fecha, descripcion, factor=factor, tipo=tipo)
    return jsonify({"ok": ok})


# ── Dashboard de cierre diario ────────────────────────────────────────────────

@app.route("/api/cierre-diario")
@login_required
def api_cierre_diario():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    # Incluir pronóstico del día siguiente
    from logic.pronostico import calcular_pronostico
    from datetime import date, timedelta
    manana = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    productos_panaderia = obtener_productos_panaderia()

    pronostico_manana = []
    for p in productos_panaderia:
        try:
            res = calcular_pronostico(p, fecha_objetivo=manana)
            pronostico_manana.append({
                "producto": p,
                "sugerido": res.produccion_sugerida,
                "confianza": res.confianza,
            })
        except Exception:
            pass

    resumen = obtener_resumen_cierre_diario(fecha=fecha)
    resumen["pronostico_manana"] = pronostico_manana
    return jsonify({"ok": True, **resumen})


# ── Exportaciones CSV ─────────────────────────────────────────────────────────

@app.route("/api/export/ventas")
@login_required
def api_export_ventas():
    if session.get("usuario", {}).get("rol") != "panadero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    dias = int(request.args.get("dias", 30))
    ventas = exportar_ventas_csv(dias=dias)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "fecha", "hora", "producto", "cantidad", "precio_unitario", "total",
        "metodo_pago", "registrado_por"
    ])
    writer.writeheader()
    writer.writerows(ventas)

    fecha_hoy = datetime.now().strftime("%Y%m%d")
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=ventas_{fecha_hoy}.csv"}
    )


@app.route("/api/export/inventario")
@login_required
def api_export_inventario():
    if session.get("usuario", {}).get("rol") != "panadero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    insumos = exportar_inventario_csv()

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["nombre", "unidad", "stock", "stock_minimo", "activo"])
    writer.writeheader()
    writer.writerows(insumos)

    fecha_hoy = datetime.now().strftime("%Y%m%d")
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=inventario_{fecha_hoy}.csv"}
    )


@app.route("/api/export/productos")
@login_required
def api_export_productos():
    if session.get("usuario", {}).get("rol") != "panadero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403

    productos = exportar_productos_sistema()
    columnas = [
        ("id", "ID"),
        ("nombre", "Nombre"),
        ("categoria", "Categoria"),
        ("menu", "Menu"),
        ("descripcion", "Descripcion"),
        ("precio", "Precio"),
        ("activo", "Activo"),
        ("es_panaderia", "Es panaderia"),
        ("es_adicional", "Es adicional"),
        ("stock_minimo", "Stock minimo"),
    ]
    filas = []
    for producto in productos:
        filas.append({
            "id": str(producto.get("id", "") or ""),
            "nombre": str(producto.get("nombre", "") or ""),
            "categoria": str(producto.get("categoria", "") or ""),
            "menu": str(producto.get("menu", "") or ""),
            "descripcion": str(producto.get("descripcion", "") or ""),
            "precio": str(producto.get("precio", "") or 0),
            "activo": "Si" if int(producto.get("activo", 0) or 0) else "No",
            "es_panaderia": "Si" if int(producto.get("es_panaderia", 0) or 0) else "No",
            "es_adicional": "Si" if int(producto.get("es_adicional", 0) or 0) else "No",
            "stock_minimo": str(producto.get("stock_minimo", "") or 0),
        })

    contenido = _crear_excel_simple("Productos", columnas, filas)
    fecha_hoy = datetime.now().strftime("%Y%m%d")
    return Response(
        contenido,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=productos_sistema_{fecha_hoy}.xlsx"}
    )


# ── Vista de cierre diario ───────────────────────────────────────────────────

@app.route("/panadero/cierre")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_cierre():
    return render_template("panadero_cierre.html",
                           layout="panadero", active_page="cierre")


# ── Vista de audit log ────────────────────────────────────────────────────────

@app.route("/panadero/audit")
@login_required
@admin_required
def panadero_audit():
    if not _rol_es_admin():
        return redirect(url_for("auth.index"))
    return render_template("panadero_audit.html",
                           layout="panadero", active_page="audit")


# ══════════════════════════════════════════════
# PLATAFORMA (platform_superadmin)
# ══════════════════════════════════════════════

@app.route("/tenant/panel")
@login_required
@roles_required("tenant_admin", PLATFORM_ADMIN_ROLE)
@tenant_scope_required
def tenant_admin_panel():
    from data.database import (
        obtener_usuarios_panaderia,
        obtener_terminales_panaderia,
        obtener_sedes_de_panaderia,
    )
    panaderia_id = _panaderia_actual_id()
    if not panaderia_id:
        flash("No se encontró el contexto de panadería.", "error")
        return redirect(url_for("auth.index"))

    usuarios   = obtener_usuarios_panaderia(int(panaderia_id))
    sedes      = obtener_sedes_de_panaderia(int(panaderia_id))
    terminales = obtener_terminales_panaderia(int(panaderia_id))

    return render_template(
        "tenant_admin_panel.html",
        usuarios=usuarios,
        sedes=sedes,
        terminales=terminales,
        layout="panadero",
        active_page="tenant_panel",
    )


@app.route("/api/tenant/usuarios")
@login_required
@roles_required("tenant_admin", PLATFORM_ADMIN_ROLE)
def api_tenant_usuarios():
    from data.database import obtener_usuarios_panaderia
    panaderia_id = _panaderia_actual_id()
    if not panaderia_id:
        return jsonify({"ok": False, "error": "Sin contexto de panadería"}), 400
    return jsonify({"ok": True, "data": obtener_usuarios_panaderia(int(panaderia_id))})


@app.route("/api/tenant/terminales")
@login_required
@roles_required("tenant_admin", PLATFORM_ADMIN_ROLE)
def api_tenant_terminales():
    from data.database import obtener_terminales_panaderia
    panaderia_id = _panaderia_actual_id()
    if not panaderia_id:
        return jsonify({"ok": False, "error": "Sin contexto de panadería"}), 400
    return jsonify({"ok": True, "data": obtener_terminales_panaderia(int(panaderia_id))})


@app.route("/platform/panel")
@login_required
@roles_required(PLATFORM_ADMIN_ROLE)
def platform_panel():
    from data.database import listar_panaderias_plataforma
    panaderias = listar_panaderias_plataforma()
    return render_template("platform_panel.html", panaderias=panaderias)


@app.route("/api/platform/panaderias")
@login_required
@roles_required(PLATFORM_ADMIN_ROLE)
def api_platform_panaderias():
    from data.database import listar_panaderias_plataforma
    return jsonify({"ok": True, "data": listar_panaderias_plataforma()})


@app.route("/api/platform/panaderia/<int:panaderia_id>/suscripcion", methods=["POST"])
@login_required
@roles_required(PLATFORM_ADMIN_ROLE)
def api_platform_actualizar_suscripcion(panaderia_id: int):
    from data.database import actualizar_plan_suscripcion
    data = request.get_json(silent=True) or {}
    plan = str(data.get("plan", "") or "").strip().lower()
    estado = str(data.get("estado", "activa") or "activa").strip().lower()
    fecha_vencimiento = str(data.get("fecha_vencimiento", "") or "").strip() or None
    notas = str(data.get("notas", "") or "").strip()
    planes_validos = ("free", "starter", "pro", "enterprise")
    estados_validos = ("activa", "trial", "vencida", "cancelada", "suspendida")
    if plan not in planes_validos:
        return jsonify({"ok": False, "error": f"Plan inválido. Opciones: {planes_validos}"}), 400
    if estado not in estados_validos:
        return jsonify({"ok": False, "error": f"Estado inválido. Opciones: {estados_validos}"}), 400
    ok = actualizar_plan_suscripcion(panaderia_id, plan, estado, fecha_vencimiento, notas)
    if not ok:
        return jsonify({"ok": False, "error": "No se pudo actualizar la suscripción"}), 500
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=panaderia_id,
        sede_id=None,
        ip=_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="actualizar_suscripcion",
        entidad="panaderia",
        entidad_id=str(panaderia_id),
        detalle=f"Plan: {plan} | Estado: {estado} | Vence: {fecha_vencimiento or 'nunca'}",
        resultado="ok",
    )
    return jsonify({"ok": True})


@app.route("/api/platform/panaderia/<int:panaderia_id>/estado", methods=["POST"])
@login_required
@roles_required(PLATFORM_ADMIN_ROLE)
def api_platform_actualizar_estado_tenant(panaderia_id: int):
    data = request.get_json(silent=True) or {}
    estado_operativo = str(data.get("estado_operativo", "") or "").strip().lower()
    estados_validos = ("activa", "suspendida", "prueba", "bloqueada")
    if estado_operativo not in estados_validos:
        return jsonify({"ok": False, "error": f"Estado inválido. Opciones: {estados_validos}"}), 400
    with get_connection() as conn:
        conn.execute(
            "UPDATE panaderias SET estado_operativo = ? WHERE id = ?",
            (estado_operativo, panaderia_id),
        )
        conn.commit()
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=_usuario_actual_id(),
        panaderia_id=panaderia_id,
        sede_id=None,
        ip=_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="actualizar_estado_operativo",
        entidad="panaderia",
        entidad_id=str(panaderia_id),
        detalle=f"Estado operativo → {estado_operativo}",
        resultado="ok",
    )
    return jsonify({"ok": True})


# ══════════════════════════════════════════════
# UTILIDADES
# ══════════════════════════════════════════════

def _get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def _format_ui_number(value, decimals: int = 0) -> str:
    """Formato visible para la UI con separadores de miles legibles."""
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        number = 0.0
    decimals = max(0, int(decimals or 0))
    return f"{number:,.{decimals}f}"


def _format_ui_money(value, decimals: int = 0) -> str:
    return f"${_format_ui_number(value, decimals)}"


def _xml_escape(texto) -> str:
    return (
        str(texto or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _xlsx_column_letter(index: int) -> str:
    resultado = ""
    numero = max(1, int(index or 1))
    while numero:
        numero, resto = divmod(numero - 1, 26)
        resultado = chr(65 + resto) + resultado
    return resultado


def _crear_excel_simple(nombre_hoja: str, columnas: list[tuple[str, str]], filas: list[dict]) -> bytes:
    nombre_hoja = str(nombre_hoja or "Hoja1").strip() or "Hoja1"
    ancho_maximo = []
    for key, titulo in columnas:
        valores = [str(titulo or "")]
        for fila in filas:
            valores.append(str(fila.get(key, "") or ""))
        ancho = max(len(valor) for valor in valores) if valores else 10
        ancho_maximo.append(min(max(ancho + 2, 10), 48))

    filas_xml = []
    encabezado = []
    for indice, (_, titulo) in enumerate(columnas, start=1):
        ref = f"{_xlsx_column_letter(indice)}1"
        encabezado.append(
            f'<c r="{ref}" t="inlineStr" s="1"><is><t>{_xml_escape(titulo)}</t></is></c>'
        )
    filas_xml.append(f'<row r="1">{"".join(encabezado)}</row>')

    for fila_idx, fila in enumerate(filas, start=2):
        celdas = []
        for col_idx, (key, _) in enumerate(columnas, start=1):
            ref = f"{_xlsx_column_letter(col_idx)}{fila_idx}"
            valor = str(fila.get(key, "") or "")
            celdas.append(
                f'<c r="{ref}" t="inlineStr"><is><t>{_xml_escape(valor)}</t></is></c>'
            )
        filas_xml.append(f'<row r="{fila_idx}">{"".join(celdas)}</row>')

    ultima_columna = _xlsx_column_letter(len(columnas))
    ultima_fila = max(len(filas) + 1, 1)
    columnas_xml = "".join(
        f'<col min="{indice}" max="{indice}" width="{ancho}" customWidth="1"/>'
        for indice, ancho in enumerate(ancho_maximo, start=1)
    )
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:{ultima_columna}{ultima_fila}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        f'<cols>{columnas_xml}</cols>'
        f'<sheetData>{"".join(filas_xml)}</sheetData>'
        '</worksheet>'
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets>'
        f'<sheet name="{_xml_escape(nombre_hoja)}" sheetId="1" r:id="rId1"/>'
        '</sheets>'
        '</workbook>'
    )

    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        '</Relationships>'
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2">'
        '<font><sz val="11"/><name val="Calibri"/></font>'
        '<font><b/><sz val="11"/><name val="Calibri"/></font>'
        '</fonts>'
        '<fills count="2">'
        '<fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill>'
        '</fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2">'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>'
        '</cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        '</styleSheet>'
    )

    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '<Override PartName="/docProps/core.xml" '
        'ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        '<Override PartName="/docProps/app.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>'
        '</Types>'
    )

    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '<Relationship Id="rId2" '
        'Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" '
        'Target="docProps/core.xml"/>'
        '<Relationship Id="rId3" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" '
        'Target="docProps/app.xml"/>'
        '</Relationships>'
    )

    generado_en = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    core_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" '
        'xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:dcmitype="http://purl.org/dc/dcmitype/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        '<dc:creator>Codex</dc:creator>'
        '<cp:lastModifiedBy>Codex</cp:lastModifiedBy>'
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{generado_en}</dcterms:created>'
        f'<dcterms:modified xsi:type="dcterms:W3CDTF">{generado_en}</dcterms:modified>'
        '</cp:coreProperties>'
    )

    app_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" '
        'xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">'
        '<Application>Codex</Application>'
        '</Properties>'
    )

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archivo_zip:
        archivo_zip.writestr("[Content_Types].xml", content_types_xml)
        archivo_zip.writestr("_rels/.rels", root_rels_xml)
        archivo_zip.writestr("docProps/core.xml", core_xml)
        archivo_zip.writestr("docProps/app.xml", app_xml)
        archivo_zip.writestr("xl/workbook.xml", workbook_xml)
        archivo_zip.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        archivo_zip.writestr("xl/styles.xml", styles_xml)
        archivo_zip.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


def _database_engine() -> str:
    return str(get_database_info().get("type", "sqlite") or "sqlite").strip().lower()


def _supports_app_file_backups() -> bool:
    return bool(get_database_info().get("supports_app_file_backup"))


@app.context_processor
def utility_processor():
    """Variables globales disponibles en todos los templates."""
    return {
        "ahora": datetime.now(),
        "icono": icono,
        "color_prod": color_prod,
        "number_ui": _format_ui_number,
        "money_ui": _format_ui_money,
        "request_id": getattr(g, "request_id", ""),
        "csrf_token": getattr(g, "csrf_token", _current_csrf_token()),
        "tenant_context": getattr(g, "tenant_context", TenantContext()),
        "sede_context": getattr(g, "sede_context", SedeContext()),
        "brand_context": getattr(g, "brand_context", _build_brand_context()),
        "subscription_context": getattr(g, "subscription_context", SubscriptionContext()),
        "terminal_context": getattr(g, "terminal_context", None),
    }


# ══════════════════════════════════════════════
# PUNTO DE ENTRADA
# ══════════════════════════════════════════════

def _iniciar_scheduler():
    if not _env_bool("ENABLE_IN_APP_SCHEDULER", False):
        app.logger.info(
            "Scheduler embebido desactivado. Usa `python jobs_runner.py daemon` para correr jobs fuera del proceso web.",
        )
        return None
    """Inicia el scheduler de backups automáticos diarios."""
    if not _supports_app_file_backups():
        app.logger.info(
            "Backups automáticos en la app desactivados para %s. Usa backups del proveedor.",
            _database_engine(),
        )
        return None
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        backup_hour = int(os.environ.get("BACKUP_AUTO_HOUR", "23"))
        retention_days = int(os.environ.get("BACKUP_RETENTION_DAYS", "30"))

        def _backup_diario():
            result = crear_backup(f"Backup automático diario - {datetime.now().strftime('%Y-%m-%d')}")
            if result["ok"]:
                limpiar_backups_antiguos(dias_retencion=retention_days)
                app.logger.info(f"Backup automático completado: {result['backup']['archivo']}")
            else:
                app.logger.error(f"Backup automático falló: {result.get('error', 'Error desconocido')}")

        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(_backup_diario, "cron", hour=backup_hour, minute=0)
        scheduler.start()
        app.logger.info(f"Backup automático programado a las {backup_hour:02d}:00")
        return scheduler
    except ImportError:
        app.logger.debug("APScheduler no instalado. Backups automáticos desactivados.")
        return None
    except Exception as e:
        app.logger.error(f"No se pudo iniciar scheduler de backups: {e}")
        return None


def _inicializar_base_de_datos_con_retry() -> None:
    """Inicializa la BD con pequeños reintentos para despliegues en contenedores."""
    db_info = get_database_info()
    motor = str(db_info.get("type", "sqlite") or "sqlite").strip().lower()
    app.logger.info(
        "Configuracion de BD detectada: type=%s railway=%s require_postgres=%s url=%s",
        motor,
        db_info.get("is_railway"),
        db_info.get("require_postgres"),
        db_info.get("database_url") or "(sin DATABASE_URL)",
    )
    intentos = int(os.environ.get("DB_INIT_MAX_RETRIES", "12" if motor == "postgresql" else "1"))
    espera = float(os.environ.get("DB_INIT_RETRY_DELAY", "2.5" if motor == "postgresql" else "0"))

    ultimo_error = None
    for intento in range(1, max(intentos, 1) + 1):
        try:
            inicializar_base_de_datos()
            if intento > 1:
                app.logger.info("Base de datos lista tras %s intentos.", intento)
            return
        except Exception as exc:
            ultimo_error = exc
            if intento >= max(intentos, 1):
                break
            app.logger.warning(
                "No se pudo inicializar la base de datos (%s/%s): %s. Reintentando en %.1fs.",
                intento,
                intentos,
                exc,
                espera,
            )
            if espera > 0:
                time.sleep(espera)

    if ultimo_error is not None:
        raise ultimo_error


# ──────────────────────────────────────────────
# Encargos
# ──────────────────────────────────────────────

def _render_encargos_view(*, layout: str, active_page: str,
                          puede_crear_encargos: bool,
                          puede_cambiar_estado_encargo: bool,
                          puede_eliminar_encargo: bool,
                          titulo: str,
                          descripcion: str):
    productos = obtener_productos_con_precio()
    encargos = obtener_encargos_v2(dias=60)
    for encargo in encargos:
        if encargo.get("cliente_id"):
            cliente_master = obtener_cliente(int(encargo.get("cliente_id") or 0))
            if cliente_master:
                encargo["cliente_master"] = cliente_master
                encargo["email"] = cliente_master.get("email") or encargo.get("email") or ""
                encargo["tipo_doc"] = cliente_master.get("tipo_doc") or encargo.get("tipo_doc") or ""
                encargo["numero_doc"] = cliente_master.get("numero_doc") or encargo.get("numero_doc") or ""
                encargo["direccion_documento"] = cliente_master.get("direccion") or encargo.get("direccion_documento") or ""
                encargo["empresa"] = cliente_master.get("empresa") or encargo.get("empresa") or ""
        encargo["documentos"] = obtener_documentos_por_origen("encargo", int(encargo.get("id", 0) or 0))
        encargo["ultimo_documento"] = encargo["documentos"][0] if encargo["documentos"] else None
    return render_template(
        "cajero_encargos.html",
        productos=productos,
        encargos=encargos,
        layout=layout,
        active_page=active_page,
        puede_crear_encargos=puede_crear_encargos,
        puede_cambiar_estado_encargo=puede_cambiar_estado_encargo,
        puede_eliminar_encargo=puede_eliminar_encargo,
        encargo_view_title=titulo,
        encargo_view_description=descripcion,
    )


@app.route("/cajero/encargos")
@login_required
@roles_required("cajero", "tenant_admin", "platform_superadmin")
def cajero_encargos():
    return _render_encargos_view(
        layout="cajero",
        active_page="encargos",
        puede_crear_encargos=True,
        puede_cambiar_estado_encargo=True,
        puede_eliminar_encargo=True,
        titulo="Encargos",
        descripcion="Registra, filtra y da seguimiento a los encargos que salen desde caja.",
    )


@app.route("/panadero/encargos")
@login_required
@roles_required("panadero", "tenant_admin", "platform_superadmin")
def panadero_encargos():
    return _render_encargos_view(
        layout="panadero",
        active_page="encargos",
        puede_crear_encargos=False,
        puede_cambiar_estado_encargo=False,
        puede_eliminar_encargo=False,
        titulo="Encargos de Caja",
        descripcion="Consulta los encargos registrados por caja para organizar la produccion y las entregas.",
    )


@app.route("/api/encargo", methods=["POST"])
@login_required
def api_crear_encargo():
    if _rol_usuario_actual() not in ("cajero", "panadero"):
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    fecha_entrega = str(data.get("fecha_entrega", "") or "").strip()
    cliente = str(data.get("cliente", "") or "").strip()
    empresa = str(data.get("empresa", "") or "").strip()
    notas = str(data.get("notas", "") or "").strip()
    items = data.get("items", [])
    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = crear_encargo(fecha_entrega, cliente, items,
                              empresa=empresa, notas=notas, registrado_por=usuario)
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/encargo/<int:encargo_id>", methods=["GET"])
@login_required
def api_obtener_encargo(encargo_id):
    encargo = obtener_encargo(encargo_id)
    if not encargo:
        return jsonify({"ok": False, "error": "Encargo no encontrado"}), 404
    return jsonify({"ok": True, "encargo": encargo})


@app.route("/api/encargo/<int:encargo_id>/estado", methods=["PUT"])
@login_required
def api_estado_encargo(encargo_id):
    if _rol_usuario_actual() not in ("cajero", "panadero"):
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    estado = str(data.get("estado", "") or "").strip()
    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = actualizar_estado_encargo(encargo_id, estado, usuario)
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/encargo/<int:encargo_id>", methods=["DELETE"])
@login_required
def api_eliminar_encargo(encargo_id):
    if _rol_usuario_actual() not in ("cajero", "panadero"):
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    resultado = eliminar_encargo(encargo_id)
    return jsonify(resultado), 200 if resultado.get("ok") else 400


# ──────────────────────────────────────────────
# Union de pedidos
# ──────────────────────────────────────────────

@app.route("/api/pedidos/unificar", methods=["POST"])
@login_required
def api_unificar_pedidos():
    if _rol_usuario_actual() == "mesero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    pedido_ids = data.get("pedido_ids", [])
    if not pedido_ids or len(pedido_ids) < 2:
        return jsonify({"ok": False, "error": "Se necesitan al menos dos pedidos"}), 400
    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = unificar_pedidos(pedido_ids, unificado_por=usuario)
    return jsonify(resultado), 200 if resultado.get("ok") else 400


# ──────────────────────────────────────────────────────────────────────────────
# Release 3 — Clientes maestros
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/api/clientes", methods=["GET"])
@login_required
def api_clientes_listar():
    q = request.args.get("q", "").strip()
    clientes = obtener_clientes(busqueda=q)
    return jsonify({"ok": True, "clientes": clientes})


@app.route("/api/clientes", methods=["POST"])
@login_required
def api_clientes_crear():
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    resultado = crear_cliente(
        nombre=data.get("nombre", ""),
        telefono=data.get("telefono", ""),
        email=data.get("email", ""),
        tipo_doc=data.get("tipo_doc", ""),
        numero_doc=data.get("numero_doc", ""),
        empresa=data.get("empresa", ""),
        direccion=data.get("direccion", ""),
        notas=data.get("notas", ""),
    )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/cliente/<int:cliente_id>", methods=["GET"])
@login_required
def api_cliente_obtener(cliente_id: int):
    c = obtener_cliente(cliente_id)
    if not c:
        return jsonify({"ok": False, "error": "Cliente no encontrado"}), 404
    return jsonify({"ok": True, "cliente": c})


@app.route("/api/cliente/<int:cliente_id>", methods=["PUT"])
@login_required
def api_cliente_actualizar(cliente_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    resultado = actualizar_cliente(
        cliente_id,
        nombre=data.get("nombre", ""),
        telefono=data.get("telefono", ""),
        email=data.get("email", ""),
        tipo_doc=data.get("tipo_doc", ""),
        numero_doc=data.get("numero_doc", ""),
        empresa=data.get("empresa", ""),
        direccion=data.get("direccion", ""),
        notas=data.get("notas", ""),
    )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


# ── Encargos v2 ───────────────────────────────────────────────────────────────

@app.route("/api/cliente/<int:cliente_id>/historial", methods=["GET"])
@login_required
def api_cliente_historial(cliente_id: int):
    pagination_args = _parse_pagination_args()
    historial = obtener_historial_cliente(
        cliente_id,
        page=pagination_args["page"],
        size=pagination_args["size"],
    )
    if not historial.get("ok"):
        return jsonify(historial), 404
    return jsonify(historial)


@app.route("/api/cartera/<int:cuenta_id>", methods=["GET"])
@login_required
def api_cartera_detalle(cuenta_id: int):
    cuenta = obtener_cuenta_por_cobrar(cuenta_id)
    if not cuenta:
        return jsonify({"ok": False, "error": "Cuenta por cobrar no encontrada"}), 404
    return jsonify({"ok": True, "cuenta": cuenta})


@app.route("/api/cartera/<int:cuenta_id>/abono", methods=["POST"])
@login_required
def api_cartera_abono(cuenta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    try:
        monto = float(data.get("monto") or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Monto invalido"}), 400
    resultado = registrar_abono_cuenta(
        cuenta_id=cuenta_id,
        monto=monto,
        metodo_pago=data.get("metodo_pago", "efectivo"),
        referencia=data.get("referencia", ""),
        nota=data.get("nota", ""),
        usuario_id=_usuario_actual_id(),
        usuario_nombre=_nombre_usuario_actual(),
    )
    if resultado.get("ok"):
        _log_event(
            "cartera_abono_registrado",
            request_id=getattr(g, "request_id", ""),
            cuenta_id=cuenta_id,
            monto=monto,
            metodo_pago=data.get("metodo_pago", "efectivo"),
            usuario=_nombre_usuario_actual(),
            saldo_pendiente=resultado.get("saldo_pendiente"),
            estado=resultado.get("estado"),
        )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/encargo/v2", methods=["POST"])
@login_required
def api_crear_encargo_v2():
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    usuario = session.get("usuario", {})
    resultado = crear_encargo_v2(
        fecha_entrega=data.get("fecha_entrega", ""),
        cliente=data.get("cliente", ""),
        items=data.get("items", []),
        empresa=data.get("empresa", ""),
        notas=data.get("notas", ""),
        registrado_por=usuario.get("nombre", ""),
        hora_entrega=data.get("hora_entrega", ""),
        telefono=data.get("telefono", ""),
        anticipo=float(data.get("anticipo") or 0),
        canal_venta=data.get("canal_venta", "tienda"),
        tipo_encargo=data.get("tipo_encargo", "orden"),
        direccion_entrega=data.get("direccion_entrega", ""),
        cliente_id=data.get("cliente_id"),
        estado_inicial=data.get("estado_inicial", "confirmado"),
    )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/encargo/v2/<int:encargo_id>", methods=["PUT"])
@login_required
def api_actualizar_encargo_v2(encargo_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    resultado = actualizar_encargo(
        encargo_id,
        fecha_entrega=data.get("fecha_entrega", ""),
        cliente=data.get("cliente", ""),
        items=data.get("items", []),
        empresa=data.get("empresa", ""),
        notas=data.get("notas", ""),
        hora_entrega=data.get("hora_entrega", ""),
        telefono=data.get("telefono", ""),
        canal_venta=data.get("canal_venta", "tienda"),
        tipo_encargo=data.get("tipo_encargo", "orden"),
        direccion_entrega=data.get("direccion_entrega", ""),
        cliente_id=data.get("cliente_id"),
    )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/encargo/v2/<int:encargo_id>/estado", methods=["PUT"])
@login_required
def api_estado_encargo_v2(encargo_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    usuario = session.get("usuario", {})
    resultado = actualizar_estado_encargo_v2(
        encargo_id, data.get("estado", ""), usuario.get("nombre", "")
    )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/encargo/v2/<int:encargo_id>/pago", methods=["POST"])
@login_required
def api_pago_encargo_v2(encargo_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    usuario = session.get("usuario", {})
    try:
        monto = float(data.get("monto") or 0)
    except (ValueError, TypeError):
        return jsonify({"ok": False, "error": "monto invalido"}), 400
    resultado = registrar_pago_encargo(
        encargo_id,
        metodo=data.get("metodo", "efectivo"),
        monto=monto,
        registrado_por=usuario.get("nombre", ""),
        referencia=data.get("referencia", ""),
        notas=data.get("notas", ""),
        usuario_id=usuario.get("id"),
    )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/encargo/v2/<int:encargo_id>/pagos", methods=["GET"])
@login_required
def api_pagos_encargo_v2(encargo_id: int):
    pagos = obtener_pagos_encargo(encargo_id)
    return jsonify({"ok": True, "pagos": pagos})


@app.route("/api/encargo/v2/<int:encargo_id>", methods=["GET"])
@login_required
def api_obtener_encargo_v2(encargo_id: int):
    enc = obtener_encargo_v2(encargo_id)
    if not enc:
        return jsonify({"ok": False, "error": "Encargo no encontrado"}), 404
    return jsonify({"ok": True, "encargo": enc})


@app.route("/api/encargos/v2", methods=["GET"])
@login_required
def api_listar_encargos_v2():
    estado = request.args.get("estado", "")
    fecha = request.args.get("fecha_entrega", "")
    dias = int(request.args.get("dias", 30))
    encargos = obtener_encargos_v2(estado=estado or None, fecha_entrega=fecha or None, dias=dias)
    return jsonify({"ok": True, "encargos": encargos})


# ──────────────────────────────────────────────────────────────────────────────
# POS Transaccional — venta_headers / venta_items / venta_pagos
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/api/venta/iniciar", methods=["POST"])
@login_required
def api_venta_iniciar():
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    usuario = session.get("usuario", {})
    tipo_venta = data.get("tipo_venta", "rapida")
    if tipo_venta not in ("rapida", "con_documento"):
        return jsonify({"ok": False, "error": "tipo_venta invalido"}), 400
    try:
        venta_id = crear_venta_header(
            cajero=usuario.get("nombre", ""),
            cajero_id=usuario.get("id"),
            sede_id=_sede_actual_id(),
            panaderia_id=_panaderia_actual_id(),
            terminal_id=usuario.get("terminal_id"),
            tipo_venta=tipo_venta,
        )
        _log_event(
            "venta_iniciada",
            request_id=getattr(g, "request_id", ""),
            venta_id=venta_id,
            tipo_venta=tipo_venta,
            usuario=usuario.get("nombre", ""),
            panaderia_id=_panaderia_actual_id(),
            sede_id=_sede_actual_id(),
        )
        return jsonify({"ok": True, "venta_id": venta_id})
    except Exception as exc:
        _log_exception(
            "venta_inicio_error",
            exc,
            request_id=getattr(g, "request_id", ""),
            tipo_venta=tipo_venta,
            usuario=usuario.get("nombre", ""),
        )
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/venta/<int:venta_id>/items", methods=["PUT"])
@login_required
def api_venta_items(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    items = data.get("items", [])
    if not isinstance(items, list):
        return jsonify({"ok": False, "error": "items debe ser lista"}), 400
    usuario = session.get("usuario", {})
    resultado = actualizar_items_venta(
        venta_id,
        items,
        actor_role=_rol_usuario_actual(),
        actor_name=usuario.get("nombre", ""),
        panaderia_id=_panaderia_actual_id(),
    )
    if resultado.get("ok"):
        for manual in resultado.get("manuales", []):
            registrar_audit(
                usuario=usuario.get("nombre", ""),
                usuario_id=usuario.get("id"),
                panaderia_id=_panaderia_actual_id(),
                sede_id=_sede_actual_id(),
                ip=request.remote_addr or "",
                user_agent=request.headers.get("User-Agent", ""),
                request_id=getattr(g, "request_id", ""),
                accion="precio_manual_venta",
                entidad="venta",
                entidad_id=str(venta_id),
                detalle=(
                    f"{manual.get('producto')}: {manual.get('precio_base')} -> "
                    f"{manual.get('precio_aplicado')} | motivo: {manual.get('motivo_precio')} | "
                    f"autorizado por: {manual.get('autorizado_por')}"
                ),
                resultado="ok",
            )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/venta/<int:venta_id>/comprador", methods=["PUT"])
@login_required
def api_venta_comprador(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    ok = actualizar_comprador_venta(
        venta_id,
        nombre_comprador=data.get("nombre_comprador", ""),
        tipo_doc=data.get("tipo_doc", ""),
        numero_doc=data.get("numero_doc", ""),
        email_comprador=data.get("email_comprador", ""),
        empresa_comprador=data.get("empresa_comprador", ""),
        direccion_comprador=data.get("direccion_comprador", ""),
        cliente_id=data.get("cliente_id"),
    )
    return jsonify({"ok": ok}), 200 if ok else 400


@app.route("/api/venta/<int:venta_id>/cliente", methods=["PUT"])
@login_required
def api_venta_cliente(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    ok = actualizar_cliente_venta(
        venta_id,
        cliente_id=data.get("cliente_id"),
        cliente_nombre_snapshot=data.get("cliente_nombre_snapshot", ""),
    )
    return jsonify({"ok": ok}), 200 if ok else 400


@app.route("/api/venta/<int:venta_id>/pagar", methods=["POST"])
@login_required
def api_venta_pagar(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    metodo = data.get("metodo", "efectivo")
    monto = data.get("monto")
    recibido = data.get("recibido")
    if monto is None:
        return jsonify({"ok": False, "error": "monto requerido"}), 400
    try:
        monto = float(monto)
        recibido = float(recibido) if recibido is not None else monto
    except (ValueError, TypeError):
        return jsonify({"ok": False, "error": "monto invalido"}), 400
    usuario = session.get("usuario", {})
    resultado = registrar_pago_venta(
        venta_id=venta_id,
        metodo=metodo,
        monto=monto,
        registrado_por=usuario.get("nombre", ""),
        referencia=data.get("referencia", ""),
        recibido=recibido,
    )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/venta/<int:venta_id>/cerrar", methods=["POST"])
@login_required
def api_venta_cerrar(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    resultado = cerrar_venta(
        venta_id,
        usuario_id=_usuario_actual_id(),
        usuario_nombre=_nombre_usuario_actual(),
        fecha_vencimiento_credito=data.get("fecha_vencimiento_credito"),
    )
    if resultado.get("ok"):
        _log_event(
            "venta_cerrada",
            request_id=getattr(g, "request_id", ""),
            venta_id=venta_id,
            usuario=_nombre_usuario_actual(),
            venta_grupo=resultado.get("venta_grupo"),
            cuenta_por_cobrar_id=resultado.get("cuenta_por_cobrar_id"),
            credito_total=resultado.get("credito_total"),
        )
    return jsonify(resultado), 200 if resultado.get("ok") else 400


@app.route("/api/venta/<int:venta_id>/suspender", methods=["POST"])
@login_required
def api_venta_suspender(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    usuario = session.get("usuario", {})
    ok = suspender_venta(
        venta_id,
        nota=data.get("nota", ""),
        suspendida_por=usuario.get("nombre", ""),
    )
    if ok:
        _log_event(
            "venta_suspendida",
            request_id=getattr(g, "request_id", ""),
            venta_id=venta_id,
            usuario=usuario.get("nombre", ""),
            nota=data.get("nota", ""),
        )
    return jsonify({"ok": ok}), 200 if ok else 400


@app.route("/api/ventas/suspendidas", methods=["GET"])
@login_required
def api_ventas_suspendidas():
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    ventas = obtener_ventas_suspendidas(
        panaderia_id=_panaderia_actual_id(),
        sede_id=_sede_actual_id(),
    )
    return jsonify({"ok": True, "ventas": ventas})


@app.route("/api/venta/<int:venta_id>/reanudar", methods=["POST"])
@login_required
def api_venta_reanudar(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    ok = reanudar_venta(venta_id)
    if ok:
        venta = obtener_venta_header(venta_id)
        _log_event(
            "venta_reanudada",
            request_id=getattr(g, "request_id", ""),
            venta_id=venta_id,
            usuario=_nombre_usuario_actual(),
            estado=(venta or {}).get("estado"),
        )
        return jsonify({"ok": True, "venta": venta})
    return jsonify({"ok": False, "error": "No se pudo reanudar"}), 400


@app.route("/api/venta/<int:venta_id>/anular", methods=["POST"])
@login_required
def api_venta_anular(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    motivo = (data.get("motivo") or "").strip()
    if not motivo:
        return jsonify({"ok": False, "error": "motivo requerido"}), 400
    usuario = session.get("usuario", {})
    ok = anular_venta(
        venta_id,
        motivo=motivo,
        anulada_por=usuario.get("nombre", ""),
    )
    return jsonify({"ok": ok}), 200 if ok else 400


@app.route("/api/venta/<int:venta_id>", methods=["GET"])
@login_required
def api_venta_detalle(venta_id: int):
    roles_ok = {"cajero", "panadero", "tenant_admin"}
    if _rol_usuario_actual() not in roles_ok:
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    venta = obtener_venta_header(venta_id)
    if not venta:
        return jsonify({"ok": False, "error": "Venta no encontrada"}), 404
    return jsonify({"ok": True, "venta": venta})


# ── Registrar blueprints ──────────────────────────────────────────────────────
# auth_bp: /, /login, /logout, /health, /ready, /favicon.ico, /cambiar-password
app.register_blueprint(auth_bp)

# Inicializar BD y scheduler al cargar el módulo (para Gunicorn)
_inicializar_base_de_datos_con_retry()
_scheduler = _iniciar_scheduler()


if __name__ == "__main__":
    # Backup automatico al iniciar en modo desarrollo
    if _supports_app_file_backups():
        result = crear_backup("Backup automatico al iniciar")
        if result["ok"]:
            app.logger.info("Backup automatico creado")
        limpiar_backups_antiguos()
    ip = _get_local_ip()
    port = int(os.environ.get("PORT", os.environ.get("FLASK_RUN_PORT", "5000")))
    app.logger.info("=" * 50)
    app.logger.info("  PANADERIA - Sistema de Ventas y Pronostico")
    app.logger.info("=" * 50)
    app.logger.info(f"  Motor de BD:        {_database_engine()}")
    app.logger.info(f"  Abrir en navegador: http://{ip}:{port}")
    app.logger.info(f"  QR clientes:        http://{ip}:{port}/cliente/pedido")
    app.logger.info("  ADVERTENCIA: Cambia los PINes por defecto en Configuracion > Usuarios")
    app.logger.info("=" * 50)
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("FLASK_ENV") == "development")
