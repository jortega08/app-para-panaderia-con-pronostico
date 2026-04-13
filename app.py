import csv
import io
import os
import re
import secrets
import socket
import time
import unicodedata
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
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
    url_for, session, jsonify, flash, Response,
)
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
    obtener_registros,
    obtener_productos,
    obtener_productos_panaderia,
    obtener_productos_con_precio,
    obtener_producto_por_id,
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
    obtener_usuarios,
    agregar_usuario,
    actualizar_usuario,
    eliminar_usuario,
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
    agregar_mesa,
    eliminar_mesa,
    crear_pedido,
    actualizar_pedido,
    obtener_pedidos,
    obtener_pedidos_con_detalle,
    obtener_pedido,
    obtener_pedido_activo_mesa_mesero,
    cambiar_estado_pedido,
    dividir_pedido_y_cobrar,
    pagar_pedido,
    validar_items_contra_produccion_panaderia,
    validar_stock_pedido,
    obtener_stock_disponible_hoy,
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
    TIPO_DIA,
)

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.jinja_env.auto_reload = True

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


def _safe_display_number(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


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

# ── Rate limiting (en memoria, simple) ──────────────────────────────────────────
_MAX_ATTEMPTS = int(os.environ.get("MAX_LOGIN_ATTEMPTS", "5"))
_LOCKOUT_MINUTES = int(os.environ.get("LOGIN_LOCKOUT_MINUTES", "5"))
_login_attempts: dict = defaultdict(lambda: {"count": 0, "until": None})

# ── Iconos y colores por categoria/producto ──
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


def icono_categoria(categoria):
    return ICONOS_CATEGORIA.get(categoria, "restaurant")


def icono(nombre, categoria=None):
    categoria_real = categoria or obtener_categoria_producto_nombre(nombre)
    return icono_categoria(categoria_real)


def color_prod(nombre):
    return COLORES_PROD.get(nombre, "#B0BEC5")


def _usuario_actual() -> dict:
    return session.get("usuario", {}) if "usuario" in session else {}


def _nombre_usuario_actual() -> str:
    return str(_usuario_actual().get("nombre", "") or "").strip()


def _rol_usuario_actual() -> str:
    return str(_usuario_actual().get("rol", "") or "").strip()


def _usuario_puede_registrar_produccion() -> bool:
    return _rol_usuario_actual() in ("panadero", "cajero")


def _pedido_visible_para_usuario(pedido: dict | None) -> bool:
    if not pedido:
        return False
    if _rol_usuario_actual() != "mesero":
        return True
    return str(pedido.get("mesero", "") or "").strip() == _nombre_usuario_actual()


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


@app.after_request
def _set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if request.is_secure:
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
    return response


def _normalizar_texto(texto):
    base = unicodedata.normalize("NFKD", str(texto or ""))
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.strip().lower()
    return re.sub(r"[^a-z0-9]+", " ", base).strip()


def _obtener_adicionales_operativos():
    return list(obtener_adicionales())


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
    """Retorna el sobrante efectivo del dia anterior: sobrante_inicial + producido - vendido."""
    fecha_anterior = (datetime.strptime(fecha, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    with get_connection() as conn:
        row = conn.execute("""
            SELECT sobrante_inicial, sobrante
            FROM registros_diarios
            WHERE fecha = ? AND producto = ?
        """, (fecha_anterior, producto)).fetchone()
    if not row:
        return 0
    return max(int(row["sobrante_inicial"] or 0) + int(row["sobrante"] or 0), 0)


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


def _construir_contexto_produccion_producto(fecha: str, producto: str) -> dict:
    fecha_str = _parse_fecha_iso(fecha)
    registro = _obtener_registro_diario_producto(fecha_str, producto) or {}
    vendido_real = int(obtener_vendido_dia_producto(fecha_str, producto) or registro.get("vendido", 0) or 0)
    producido_actual = int(registro.get("producido", 0) or 0)

    resultado = calcular_pronostico(producto, fecha_objetivo=fecha_str)
    detalles = getattr(resultado, "detalles", {}) or {}
    ajuste = obtener_ajuste_pronostico(fecha_str, producto) or {}

    sugerido = int(resultado.produccion_sugerida or 0)
    meta_operativa = int(ajuste.get("ajustado") or sugerido)
    restante_meta = max(meta_operativa - producido_actual, 0)
    cumplimiento_pct = round((producido_actual / meta_operativa) * 100, 1) if meta_operativa > 0 else 0.0
    faltante_actual = max(vendido_real - producido_actual, 0)
    sobrante_actual = max(producido_actual - vendido_real, 0)

    lotes = []
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
            "producido_actual": producido_actual,
            "vendido_actual": vendido_real,
            "restante_meta": restante_meta,
            "cumplimiento_pct": cumplimiento_pct,
            "faltante_actual": faltante_actual,
            "sobrante_actual": sobrante_actual,
        },
        "lotes": lotes,
    }


def _construir_contexto_produccion_masivo(fecha: str) -> dict:
    fecha_str = _parse_fecha_iso(fecha)
    items = []
    for producto in obtener_productos_panaderia():
        contexto = _construir_contexto_produccion_producto(fecha_str, producto)
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
            "disponible_actual": max(avance["producido_actual"] - avance["vendido_actual"], 0),
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


# ── Decoradores ──

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "usuario" not in session:
            if request.is_json:
                return jsonify({"ok": False, "error": "No autenticado"}), 401
            return redirect(url_for("login"))
        # Verificar expiración de sesión
        login_ts = session.get("_login_ts")
        if login_ts:
            age = datetime.now().timestamp() - float(login_ts)
            if age > _SESSION_HOURS * 3600:
                session.clear()
                if request.is_json:
                    return jsonify({"ok": False, "error": "Sesion expirada"}), 401
                flash("Tu sesion expiró. Inicia sesion de nuevo.", "info")
                return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ══════════════════════════════════════════════
# RUTAS DE PAGINAS
# ══════════════════════════════════════════════

@app.route("/")
def index():
    if "usuario" not in session:
        return redirect(url_for("login"))
    rol = session["usuario"]["rol"]
    if rol == "cajero":
        return redirect(url_for("cajero_pos"))
    if rol == "mesero":
        return redirect(url_for("mesero_mesas"))
    return redirect(url_for("panadero_pronostico"))


@app.route("/health")
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route("/favicon.ico")
def favicon():
    return redirect(url_for("static", filename="brand/richs-logo.svg", v="20260327-ui"))

@app.route("/ready")
def readiness_check():
    try:
        from data.database import get_connection
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready"}), 200
    except Exception as e:
        return jsonify({"status": "not_ready", "error": str(e)}), 503

@app.errorhandler(404)
def pagina_no_encontrada(e):
    if request.path.startswith('/api/'):
        return jsonify({"ok": False, "error": "Recurso no encontrado"}), 404
    return render_template("error.html", codigo=404, mensaje="Página no encontrada"), 404

@app.errorhandler(500)
def error_interno(e):
    app.logger.error(f"Error interno: {e}")
    if request.path.startswith('/api/'):
        return jsonify({"ok": False, "error": "Error interno del servidor"}), 500
    return render_template("error.html", codigo=500, mensaje="Error interno del servidor"), 500

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pin = request.form.get("pin", "").strip()
        ip = request.remote_addr or "unknown"

        if not pin:
            flash("Escribe tu PIN", "error")
            return render_template("login.html")

        # ── Rate limiting por IP ──────────────────────────────────────────────
        entry = _login_attempts[ip]
        if entry["until"] and datetime.now() < entry["until"]:
            restante = int((entry["until"] - datetime.now()).total_seconds() / 60) + 1
            flash(f"Demasiados intentos. Espera {restante} minuto(s).", "error")
            return render_template("login.html")

        usuario = verificar_pin(pin)
        if usuario:
            # Login exitoso: limpiar contador
            _login_attempts.pop(ip, None)
            session.clear()
            session.permanent = True
            session["usuario"] = usuario
            session["_login_ts"] = datetime.now().timestamp()
            registrar_audit(
                usuario=usuario["nombre"],
                accion="login",
                entidad="usuario",
                entidad_id=str(usuario.get("id", "")),
                detalle=f"Login exitoso - rol: {usuario['rol']}",
            )
            if usuario["rol"] == "cajero":
                return redirect(url_for("cajero_pos"))
            if usuario["rol"] == "mesero":
                return redirect(url_for("mesero_mesas"))
            return redirect(url_for("panadero_pronostico"))

        # PIN incorrecto: incrementar contador
        entry["count"] = entry.get("count", 0) + 1
        if entry["count"] >= _MAX_ATTEMPTS:
            entry["until"] = datetime.now() + timedelta(minutes=_LOCKOUT_MINUTES)
            entry["count"] = 0
            flash(f"Demasiados intentos fallidos. Espera {_LOCKOUT_MINUTES} minutos.", "error")
        else:
            restantes = _MAX_ATTEMPTS - entry["count"]
            flash(f"PIN incorrecto. Intentos restantes: {restantes}", "error")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Cajero ──

@app.route("/cajero/pos")
@login_required
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
def cajero_pedidos():
    pedidos = obtener_pedidos_con_detalle()
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
                           mesas_filtro=mesas_filtro,
                           caja=caja,
                           layout="cajero", active_page="pedidos")


@app.route("/cajero/pedido/<int:pedido_id>/editar")
@login_required
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
        pedido_submit_label="Guardar ajuste",
        pedido_return_url=url_for("cajero_pedidos"),
        layout="cajero",
        active_page="pedidos",
    )


# ── Mesero ──

@app.route("/mesero/mesas")
@login_required
def mesero_mesas():
    mesas = obtener_resumen_mesas()
    return render_template("mesero_mesas.html",
                           mesas=mesas,
                           layout="mesero", active_page="mesas")


@app.route("/mesero/pedido/<int:mesa_id>")
@login_required
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
                           pedido_page_copy="Toca productos para crear un nuevo pedido para esta mesa. Si ya existe uno y hay algo adicional, envialo como un pedido nuevo.",
                           pedido_submit_label="Enviar pedido",
                           pedido_return_url=url_for("mesero_mesas"),
                           layout="mesero", active_page="mesas")


@app.route("/mesero/pedidos")
@login_required
def mesero_pedidos():
    pedidos = obtener_pedidos_con_detalle(mesero=_nombre_usuario_actual())
    return render_template("mesero_pedidos.html",
                           pedidos=pedidos,
                           layout="mesero", active_page="pedidos")


# ── Panadero ──

@app.route("/panadero/pronostico")
@login_required
def panadero_pronostico():
    productos = obtener_productos_panaderia()
    producto_default = productos[0] if productos else ""
    return render_template("panadero_pronostico.html",
                           productos=productos,
                           producto_default=producto_default,
                           layout="panadero", active_page="pronostico")


@app.route("/panadero/produccion", methods=["GET", "POST"])
@login_required
def panadero_produccion():
    rol_actual = _rol_usuario_actual()
    if rol_actual not in ("panadero", "cajero"):
        flash("No tienes permiso para registrar produccion", "error")
        return redirect(url_for("index"))

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
        sobrante = int(registro.get("sobrante", 0) or 0)
        registro["faltante"] = max(vendido - producido, 0)
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


@app.route("/panadero/operaciones")
@login_required
def panadero_operaciones():
    stats = obtener_estadisticas_pedidos()
    consumo = obtener_consumo_diario()
    insumos = obtener_insumos()
    alertas_stock = obtener_insumos_bajo_stock()
    mesas = obtener_resumen_mesas()
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
    proxima_mesa = (max((mesa["numero"] for mesa in mesas), default=0) + 1) if mesas else 1
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


@app.route("/panadero/backups")
@login_required
def panadero_backups():
    info = obtener_info_backup()
    backups = listar_backups()
    return render_template("panadero_backups.html",
                           info=info, backups=backups,
                           layout="panadero", active_page="backups")


@app.route("/panadero/config")
@login_required
def panadero_config():
    productos = obtener_productos_con_precio()
    categorias = obtener_categorias_producto()
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])

    usuarios = obtener_usuarios()
    local_ip = _get_local_ip()
    qr_url = f"http://{local_ip}:5000/cliente/pedido"
    codigo_caja = obtener_codigo_verificacion_caja()

    return render_template("panadero_config.html",
                           productos=productos,
                           categorias=categorias,
                           usuarios=usuarios,
                           codigo_caja=codigo_caja,
                           qr_url=qr_url,
                           layout="panadero", active_page="config")


# ── Cliente (publico, sin login) ──

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
        })

    producto = request.args.get("producto", productos[0])
    dias = int(request.args.get("dias", 30))
    if producto not in productos:
        producto = productos[0]

    hoy = datetime.now().date()
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
    for i in range(7):
        fecha = hoy + timedelta(days=i)
        fecha_str = fecha.strftime("%Y-%m-%d")
        dia_es = dias_es.get(fecha.strftime("%A"), fecha.strftime("%A"))
        try:
            resultado = calcular_pronostico(producto, fecha_objetivo=fecha_str)
            prediccion_semana.append({
                "fecha": fecha_str,
                "dia": dia_es,
                "sugerido": resultado.produccion_sugerida,
                "promedio": round(resultado.promedio_ventas, 1),
                "delta": round(resultado.produccion_sugerida - resultado.promedio_ventas, 1),
                "tipo_dia": TIPO_DIA.get(dia_es, "laboral"),
                "estado": resultado.estado,
                "confianza": resultado.confianza,
                "modelo": resultado.modelo_usado,
                "mensaje": resultado.mensaje,
            })
        except Exception:
            app.logger.exception("Error calculando pronostico semanal para %s", producto)
            prediccion_semana.append({
                "fecha": fecha_str,
                "dia": dia_es,
                "sugerido": 0,
                "promedio": 0,
                "delta": 0,
                "tipo_dia": TIPO_DIA.get(dia_es, "laboral"),
                "estado": "alerta",
                "confianza": "poca",
                "modelo": "error",
                "mensaje": "Error al calcular el pronóstico para este día.",
            })

    historial = list(reversed(obtener_registros(producto, dias=dias)))
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
    resumen_dia = obtener_resumen_por_dia_semana(producto)
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
    hoy_str = hoy.strftime("%Y-%m-%d")
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
        "sugerido": resultado.produccion_sugerida,
        "modelo": resultado.modelo_usado,
        "modelo_label": resultado.modelo_usado.replace("_", " ").capitalize(),
        "confianza": resultado.confianza,
        "promedio": resultado.promedio_ventas,
        "mensaje": resultado.mensaje,
        "estado": resultado.estado,
        "tendencia": resultado.detalles.get("tendencia", "sin datos"),
        "dia_objetivo": resultado.detalles.get("dia_objetivo", fecha),
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
def api_ventas_hoy():
    try:
        caja = obtener_resumen_caja_dia()
        return jsonify({
            "totales": obtener_total_ventas_dia(),
            "resumen": obtener_resumen_ventas_dia(),
            "ventas": obtener_ventas_dia(),
            "ventas_por_responsable": obtener_resumen_ventas_por_responsable(),
            "caja": caja,
            "arqueos": obtener_historial_arqueos(6),
            "movimientos": obtener_movimientos_caja(limite=20),
            "metodos_pago": caja.get("metodos_pago", []),
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
    data = request.json
    nombre = data.get("nombre", "").strip()
    precio = float(data.get("precio", 0))
    categoria = data.get("categoria", "Panaderia").strip() or "Panaderia"
    es_adicional = bool(data.get("es_adicional", False))
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    ok = agregar_producto(nombre, precio, categoria, es_adicional=es_adicional)
    return jsonify({"ok": ok, "error": None if ok else "Ese producto ya existe"})


@app.route("/api/producto/<int:producto_id>", methods=["PUT"])
@login_required
def api_actualizar_producto_completo(producto_id):
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    categoria = str(data.get("categoria", "Panaderia") or "").strip() or "Panaderia"
    es_adicional = bool(data.get("es_adicional", False))

    try:
        precio = float(data.get("precio", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Precio invalido"}), 400

    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400

    ok = actualizar_producto_completo(producto_id, nombre, precio, categoria, es_adicional)
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
    if _rol_usuario_actual() != "panadero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    pin = str(data.get("pin", "") or "").strip()
    rol = str(data.get("rol", "cajero") or "cajero").strip().lower()
    if not nombre or not pin:
        return jsonify({"ok": False, "error": "Llena nombre y PIN"}), 400
    if len(pin) < 4:
        return jsonify({"ok": False, "error": "El PIN debe tener al menos 4 caracteres"}), 400
    ok = agregar_usuario(
        nombre,
        pin,
        rol,
    )
    if not ok:
        return jsonify({"ok": False, "error": "No se pudo agregar el usuario"}), 400
    return jsonify({"ok": True})


@app.route("/api/usuario/<int:uid>", methods=["PUT"])
@login_required
def api_actualizar_usuario(uid):
    if _rol_usuario_actual() != "panadero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    data = request.get_json(silent=True) or {}
    nombre = str(data.get("nombre", "") or "").strip()
    rol = str(data.get("rol", "") or "").strip().lower()
    pin = str(data.get("pin", "") or "").strip()
    if not nombre:
        return jsonify({"ok": False, "error": "El nombre es obligatorio"}), 400
    if rol not in ("panadero", "cajero", "mesero"):
        return jsonify({"ok": False, "error": "Rol invalido"}), 400
    if pin and len(pin) < 4:
        return jsonify({"ok": False, "error": "El PIN debe tener al menos 4 caracteres"}), 400
    ok = actualizar_usuario(uid, nombre, rol, pin)
    if not ok:
        return jsonify({"ok": False, "error": "No se pudo actualizar el usuario"}), 400
    return jsonify({"ok": True})


@app.route("/api/usuario/<int:uid>", methods=["DELETE"])
@login_required
def api_eliminar_usuario(uid):
    if _rol_usuario_actual() != "panadero":
        return jsonify({"ok": False, "error": "Sin permiso"}), 403
    ok = eliminar_usuario(uid)
    if not ok:
        return jsonify({"ok": False, "error": "No se pudo eliminar el usuario"}), 400
    return jsonify({"ok": True})


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
        return jsonify(resultado), 500
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
    return jsonify({"ok": ok})


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

    resultado = dividir_pedido_y_cobrar(
        pedido_id,
        data.get("selecciones", []),
        registrado_por=_nombre_usuario_actual(),
        metodo_pago=metodo_pago,
        monto_recibido=monto_recibido,
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
        return jsonify(pedido)
    return jsonify({"error": "Pedido no encontrado"}), 404


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
    data = request.get_json(silent=True) or {}
    try:
        numero = int(data.get("numero", 0))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Numero invalido"}), 400
    nombre = (data.get("nombre", "") or "").strip()
    if numero <= 0:
        return jsonify({"ok": False, "error": "Numero invalido"}), 400
    ok = agregar_mesa(numero, nombre)
    if not ok:
        return jsonify({
            "ok": False,
            "error": "La mesa ya existe y se encuentra activa"
        }), 400
    return jsonify({
        "ok": True,
        "mensaje": "Mesa agregada o reactivada correctamente",
        "numero": numero
    })


# ── API Backups ──

@app.route("/api/backup", methods=["POST"])
@login_required
def api_crear_backup():
    data = request.json or {}
    nota = data.get("nota", "Backup manual")
    result = crear_backup(nota)
    return jsonify(result)


@app.route("/api/backup/restaurar", methods=["POST"])
@login_required
def api_restaurar_backup():
    data = request.json
    timestamp = data.get("timestamp", "")
    if not timestamp:
        return jsonify({"ok": False, "error": "Timestamp requerido"}), 400
    result = restaurar_backup(timestamp)
    return jsonify(result)


@app.route("/api/backup/<timestamp>", methods=["DELETE"])
@login_required
def api_eliminar_backup(timestamp):
    result = eliminar_backup(timestamp)
    return jsonify(result)


@app.route("/api/backup/limpiar", methods=["POST"])
@login_required
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
def api_audit_log():
    if session.get("usuario", {}).get("rol") != "panadero":
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
def panadero_cierre():
    if session.get("usuario", {}).get("rol") != "panadero":
        return redirect(url_for("index"))
    return render_template("panadero_cierre.html",
                           layout="panadero", active_page="cierre")


# ── Vista de audit log ────────────────────────────────────────────────────────

@app.route("/panadero/audit")
@login_required
def panadero_audit():
    if session.get("usuario", {}).get("rol") != "panadero":
        return redirect(url_for("index"))
    return render_template("panadero_audit.html",
                           layout="panadero", active_page="audit")


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
    }


# ══════════════════════════════════════════════
# PUNTO DE ENTRADA
# ══════════════════════════════════════════════

def _iniciar_scheduler():
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

@app.route("/cajero/encargos")
@login_required
def cajero_encargos():
    if _rol_usuario_actual() not in ("cajero", "panadero"):
        flash("No tienes permiso para ver encargos", "error")
        return redirect(url_for("index"))
    productos = obtener_productos_con_precio()
    encargos = obtener_encargos(dias=60)
    return render_template("cajero_encargos.html",
                           productos=productos,
                           encargos=encargos,
                           layout="cajero", active_page="encargos")


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
