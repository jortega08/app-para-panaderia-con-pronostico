"""
app.py - Panaderia: Sistema de Ventas y Pronostico (Web)
========================================================
Aplicacion Flask ligera con:
  - Login por PIN con roles (panadero / cajero)
  - POS con carrito multi-producto
  - Dashboard de ventas con graficas
  - Pronostico de produccion
  - Pagina publica para clientes via QR
"""

import csv
import io
import os
import re
import secrets
import socket
import unicodedata
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from io import BytesIO
from xml.etree import ElementTree as ET

# Cargar variables de entorno desde .env si existe
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, flash, Response,
)

from data.database import (
    inicializar_base_de_datos,
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
    guardar_registro,
    obtener_registros,
    obtener_productos,
    obtener_productos_con_precio,
    obtener_productos_adicionales,
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
    eliminar_usuario,
    registrar_venta,
    registrar_venta_lote,
    obtener_ventas_dia,
    obtener_resumen_ventas_dia,
    obtener_total_ventas_dia,
    obtener_vendido_dia_producto,
    obtener_ventas_rango,
    obtener_totales_ventas_rango,
    obtener_serie_ventas_diarias,
    obtener_resumen_productos_rango,
    obtener_resumen_medios_pago_rango,
    obtener_resumen_por_dia_semana,
    obtener_arqueo_caja_activo,
    abrir_arqueo_caja,
    cerrar_arqueo_caja,
    reabrir_arqueo_caja,
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
    obtener_pedidos,
    obtener_pedido,
    cambiar_estado_pedido,
    pagar_pedido,
    validar_items_contra_produccion_panaderia,
    obtener_resumen_mesas,
    obtener_adicionales,
    agregar_adicional,
    actualizar_adicional_detalle,
    actualizar_adicional,
    eliminar_adicional,
    guardar_configuracion_adicional,
    obtener_insumos,
    agregar_insumo,
    actualizar_stock,
    eliminar_insumo,
    obtener_insumos_bajo_stock,
    obtener_receta,
    guardar_receta,
    obtener_consumo_diario,
    obtener_estadisticas_pedidos,
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
)

app = Flask(__name__)

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

# ── Rate limiting (en memoria, simple) ──────────────────────────────────────────
_MAX_ATTEMPTS = int(os.environ.get("MAX_LOGIN_ATTEMPTS", "5"))
_LOCKOUT_MINUTES = int(os.environ.get("LOGIN_LOCKOUT_MINUTES", "5"))
_login_attempts: dict = defaultdict(lambda: {"count": 0, "until": None})

# ── Iconos y colores por categoria/producto ──
ICONOS_CATEGORIA = {
    "Panaderia": "bakery_dining",
    "Bebidas Calientes": "local_cafe",
    "Bebidas Frias": "local_bar",
    "Desayunos": "breakfast_dining",
    "Almuerzos": "lunch_dining",
}
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


def _normalizar_texto(texto):
    base = unicodedata.normalize("NFKD", str(texto or ""))
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.strip().lower()
    return re.sub(r"[^a-z0-9]+", " ", base).strip()


def _obtener_adicionales_operativos():
    catalogo = []
    vistos = set()

    for adicional in list(obtener_productos_adicionales()) + list(obtener_adicionales()):
        nombre = str(adicional.get("nombre", "") or "").strip()
        clave = _normalizar_texto(nombre)
        if not clave or clave in vistos:
            continue
        catalogo.append(adicional)
        vistos.add(clave)

    return catalogo


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
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
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
    alias_nombre = {"nombre", "producto", "referencia", "item"}
    alias_precio = {"precio", "valor", "precio venta", "precio_venta", "precio unitario"}
    alias_categoria = {"categoria", "categoria producto", "tipo", "tipo producto"}
    alias_adicional = {"es adicional", "adicional", "puede ser adicional", "extra"}

    idx_nombre = next((i for i, valor in enumerate(encabezados) if valor in alias_nombre), None)
    idx_precio = next((i for i, valor in enumerate(encabezados) if valor in alias_precio), None)
    idx_categoria = next((i for i, valor in enumerate(encabezados) if valor in alias_categoria), None)
    idx_adicional = next((i for i, valor in enumerate(encabezados) if valor in alias_adicional), None)

    if idx_nombre is None or idx_precio is None:
        raise ValueError("El archivo debe tener columnas 'nombre' y 'precio'")

    productos = []
    errores = []

    for numero_fila, fila in enumerate(filas[1:], start=2):
        nombre = str(fila[idx_nombre]).strip() if idx_nombre < len(fila) else ""
        precio_raw = fila[idx_precio] if idx_precio < len(fila) else ""
        categoria = str(fila[idx_categoria]).strip() if idx_categoria is not None and idx_categoria < len(fila) else ""
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
            "nombre": nombre,
            "precio": precio,
            "categoria": categoria or "Panaderia",
            "es_adicional": adicional_raw in {"1", "si", "sí", "true", "x", "extra", "adicional"},
        })

    if not productos:
        raise ValueError("No se pudo importar ninguna fila valida")

    return productos, errores


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
    for p in productos:
        p["icono"] = icono(p["nombre"], p.get("categoria"))
        p["color"] = color_prod(p["nombre"])
    caja = obtener_resumen_caja_dia()
    return render_template("cajero_pos.html",
                           productos=productos,
                           categorias=categorias,
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
    pedidos = obtener_pedidos()
    # Enriquecer con items
    for p in pedidos:
        detalle = obtener_pedido(p["id"])
        p["items"] = detalle["items"] if detalle else []
        p["historial_estados"] = detalle.get("historial_estados", []) if detalle else []
        p["creado_en"] = detalle.get("creado_en", p.get("creado_en")) if detalle else p.get("creado_en")
        p["pagado_en"] = detalle.get("pagado_en", p.get("pagado_en")) if detalle else p.get("pagado_en")
        p["pagado_por"] = detalle.get("pagado_por", p.get("pagado_por")) if detalle else p.get("pagado_por")
        p["metodo_pago"] = detalle.get("metodo_pago", p.get("metodo_pago")) if detalle else p.get("metodo_pago")
        p["monto_recibido"] = detalle.get("monto_recibido", p.get("monto_recibido")) if detalle else p.get("monto_recibido")
        p["cambio"] = detalle.get("cambio", p.get("cambio")) if detalle else p.get("cambio")
    caja = obtener_resumen_caja_dia()
    return render_template("cajero_pedidos.html",
                           pedidos=pedidos,
                           caja=caja,
                           layout="cajero", active_page="pedidos")


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
                           layout="mesero", active_page="mesas")


@app.route("/mesero/pedidos")
@login_required
def mesero_pedidos():
    pedidos = obtener_pedidos()
    for p in pedidos:
        detalle = obtener_pedido(p["id"])
        p["items"] = detalle["items"] if detalle else []
    return render_template("mesero_pedidos.html",
                           pedidos=pedidos,
                           layout="mesero", active_page="pedidos")


# ── Panadero ──

@app.route("/panadero/pronostico")
@login_required
def panadero_pronostico():
    productos = obtener_productos(categoria="Panaderia")
    producto_default = productos[0] if productos else ""
    return render_template("panadero_pronostico.html",
                           productos=productos,
                           producto_default=producto_default,
                           layout="panadero", active_page="pronostico")


@app.route("/panadero/produccion", methods=["GET", "POST"])
@login_required
def panadero_produccion():
    productos = obtener_productos(categoria="Panaderia")
    if request.method == "POST":
        try:
            fecha = request.form["fecha"]
            producto = request.form["producto"]
            producido = int(request.form["producido"])
            vendido = int(request.form["vendido"])
            obs = request.form.get("observaciones", "")

            datetime.strptime(fecha, "%Y-%m-%d")

            if producido < 0 or vendido < 0:
                flash("Los valores no pueden ser negativos", "error")
            elif producto not in productos:
                flash("Solo puedes registrar produccion de productos de Panaderia", "error")
            else:
                ok = guardar_registro(fecha, producto, producido, vendido, obs)
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
                           hoy=hoy,
                           registros_recientes=registros_recientes,
                           layout="panadero", active_page="produccion")


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
    dias = int(request.args.get("dias", 30))
    productos = obtener_productos()
    registros = obtener_registros(
        producto if producto != "Todos" else None, dias=dias)

    for r in registros:
        r["icono"] = icono(r["producto"])

    return render_template("panadero_historial.html",
                           registros=registros,
                           productos=productos,
                           filtro_producto=producto,
                           filtro_dias=dias,
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
    pedidos = obtener_pedidos()
    for p in pedidos:
        detalle = obtener_pedido(p["id"])
        p["items"] = detalle["items"] if detalle else []
        p["historial_estados"] = detalle.get("historial_estados", []) if detalle else []
        p["creado_en"] = detalle.get("creado_en", p.get("creado_en")) if detalle else p.get("creado_en")
        p["pagado_en"] = detalle.get("pagado_en", p.get("pagado_en")) if detalle else p.get("pagado_en")
        p["pagado_por"] = detalle.get("pagado_por", p.get("pagado_por")) if detalle else p.get("pagado_por")
        p["metodo_pago"] = detalle.get("metodo_pago", p.get("metodo_pago")) if detalle else p.get("metodo_pago")
        p["monto_recibido"] = detalle.get("monto_recibido", p.get("monto_recibido")) if detalle else p.get("monto_recibido")
        p["cambio"] = detalle.get("cambio", p.get("cambio")) if detalle else p.get("cambio")
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
    productos = obtener_productos(categoria="Panaderia")
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
        try:
            resultado = calcular_pronostico(producto, fecha_objetivo=fecha_str)
            prediccion_semana.append({
                "fecha": fecha_str,
                "dia": dias_es.get(fecha.strftime("%A"), fecha.strftime("%A")),
                "sugerido": resultado.produccion_sugerida,
                "promedio": resultado.promedio_ventas,
                "estado": resultado.estado,
                "confianza": resultado.confianza,
                "modelo": resultado.modelo_usado,
            })
        except Exception:
            app.logger.exception("Error calculando pronostico semanal para %s", producto)
            prediccion_semana.append({
                "fecha": fecha_str,
                "dia": dias_es.get(fecha.strftime("%A"), fecha.strftime("%A")),
                "sugerido": 0,
                "promedio": 0,
                "estado": "alerta",
                "confianza": "poca",
                "modelo": "error",
            })

    historial = list(reversed(obtener_registros(producto, dias=dias)))
    serie_ventas_producto = obtener_serie_ventas_diarias(dias=dias, producto=producto)
    ranking_productos = obtener_resumen_productos_rango(dias=dias)

    total_producido = sum(int(r.get("producido", 0) or 0) for r in historial)
    total_vendido = sum(int(r.get("vendido", 0) or 0) for r in historial)
    total_sobrante = sum(max(int(r.get("sobrante", 0) or 0), 0) for r in historial)
    aprovechamiento = round((total_vendido / total_producido * 100), 1) if total_producido else 0
    tendencia = analizar_tendencia(historial)

    resumen = {
        "total_producido": total_producido,
        "total_vendido": total_vendido,
        "total_sobrante": total_sobrante,
        "aprovechamiento": aprovechamiento,
        "tendencia": tendencia,
        "sugerido_semana": sum(d["sugerido"] for d in prediccion_semana),
        "promedio_sugerido": round(sum(d["sugerido"] for d in prediccion_semana) / 7, 1),
    }

    # Compatibilidad extra con el contrato nuevo que ya habias empezado.
    resumen_dia = obtener_resumen_por_dia_semana(producto)
    prediccion_semanal = {
        dia: resumen_dia.get(dia, {}).get("promedio", 0)
        for dia in ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    }

    return jsonify({
        "producto": producto,
        "productos": productos,
        "prediccion_semana": prediccion_semana,
        "historial_producto": historial,
        "serie_ventas_producto": serie_ventas_producto,
        "ranking_productos": ranking_productos,
        "resumen": resumen,
        "prediccion_semanal": prediccion_semanal,
    })


@app.route("/api/pronostico/sugerencia")
@login_required
def api_pronostico_sugerencia():
    producto = request.args.get("producto", "").strip()
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d")).strip()
    productos_panaderia = set(obtener_productos(categoria="Panaderia"))

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


@app.route("/api/historial/dashboard")
def api_historial_dashboard():
    """API compatible con el dashboard contable/historico del frontend."""
    dias = int(request.args.get("dias", 30))
    producto = request.args.get("producto", "Todos")
    producto_filtro = None if producto in ("", "Todos") else producto

    totales = obtener_totales_ventas_rango(dias=dias, producto=producto_filtro)
    serie_diaria = obtener_serie_ventas_diarias(dias=dias, producto=producto_filtro)
    resumen_productos = obtener_resumen_productos_rango(dias=dias)
    if producto_filtro:
        resumen_productos = [r for r in resumen_productos if r.get("producto") == producto_filtro]

    ventas = obtener_ventas_rango(dias=dias, producto=producto_filtro)
    registros_operacion = obtener_registros(producto=producto_filtro, dias=dias)
    arqueos_periodo = obtener_arqueos_rango(dias=dias)
    movimientos_periodo = obtener_movimientos_caja_rango(dias=dias)
    medios_pago_db = obtener_resumen_medios_pago_rango(dias=dias, producto=producto_filtro)

    transacciones = int(totales.get("transacciones", 0) or 0)
    dinero = float(totales.get("dinero", 0) or 0)
    ticket_promedio = round(dinero / transacciones, 2) if transacciones else 0.0

    ventas_recientes = ventas[:25]

    horas = {f"{h:02d}:00": 0 for h in range(6, 22)}
    for v in ventas:
        hora = (v.get("hora") or "")[:2]
        if hora.isdigit():
            h = int(hora)
            if 6 <= h <= 21:
                key = f"{h:02d}:00"
                horas[key] += int(v.get("cantidad", 0) or 0)

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

    serie_pago_map = {}
    for venta in ventas:
        fecha = venta.get("fecha")
        if not fecha:
            continue
        bucket = serie_pago_map.setdefault(fecha, {
            "fecha": fecha,
            "efectivo": 0.0,
            "transferencia": 0.0,
            "transacciones": 0,
        })
        metodo = str(venta.get("metodo_pago", "efectivo") or "efectivo").strip().lower()
        total_venta = float(venta.get("total", 0) or 0)
        if metodo == "transferencia":
            bucket["transferencia"] += total_venta
        else:
            bucket["efectivo"] += total_venta

    grupos_por_fecha = {}
    for venta in ventas:
        fecha = venta.get("fecha")
        if not fecha:
            continue
        grupos_por_fecha.setdefault(fecha, set()).add(
            venta.get("venta_grupo") or f"legacy-{fecha}-{venta.get('hora', '')}-{venta.get('producto', '')}"
        )
    for fecha, grupos in grupos_por_fecha.items():
        bucket = serie_pago_map.setdefault(fecha, {
            "fecha": fecha,
            "efectivo": 0.0,
            "transferencia": 0.0,
            "transacciones": 0,
        })
        bucket["transacciones"] = len(grupos)

    serie_pago = [
        {
            "fecha": fecha,
            "efectivo": round(data["efectivo"], 2),
            "transferencia": round(data["transferencia"], 2),
            "transacciones": int(data["transacciones"]),
        }
        for fecha, data in sorted(serie_pago_map.items())
    ]

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

    # Compatibilidad adicional con payload resumido que habias empezado.
    resumen_simple = []
    resumen_aux = {}
    for r in registros_operacion:
        p = r.get("producto", "")
        if p not in resumen_aux:
            resumen_aux[p] = {"producto": p, "icono": icono(p), "producido": 0, "vendido": 0, "sobrante": 0, "dias": 0}
        resumen_aux[p]["producido"] += int(r.get("producido", 0) or 0)
        resumen_aux[p]["vendido"] += int(r.get("vendido", 0) or 0)
        resumen_aux[p]["sobrante"] += int(r.get("sobrante", 0) or 0)
        resumen_aux[p]["dias"] += 1
    for item in resumen_aux.values():
        item["aprovechamiento"] = round(
            (item["vendido"] / item["producido"] * 100) if item["producido"] > 0 else 0, 1
        )
        resumen_simple.append(item)

    return jsonify({
        "filtro_producto": producto,
        "dias": dias,
        "totales": {
            "panes": int(totales.get("panes", 0) or 0),
            "dinero": dinero,
            "transacciones": transacciones,
            "ticket_promedio": ticket_promedio,
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
        "registros": registros_operacion,
        "resumen": resumen_simple,
        "total_registros": len(registros_operacion),
    })


@app.route("/api/venta", methods=["POST"])
def api_venta():
    data = request.json
    if not data or "items" not in data:
        return jsonify({"ok": False, "error": "Sin datos"}), 400

    items_validacion = []
    for item in data["items"]:
        items_validacion.append({
            "producto": item.get("producto", ""),
            "cantidad": int(item.get("cantidad", 0) or 0),
            "modificaciones": [],
        })

    validacion = validar_items_contra_produccion_panaderia(items_validacion)
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

    items_venta = []
    try:
        for item in data["items"]:
            cantidad = int(item["cantidad"])
            precio = float(item["precio"])
            items_venta.append({
                "producto": item["producto"],
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
    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = cerrar_arqueo_caja(
        cerrado_por=usuario,
        monto_cierre=monto_cierre,
        notas_cierre=notas_cierre,
        codigo_verificacion=codigo_verificacion,
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


@app.route("/api/caja/reabrir", methods=["POST"])
@login_required
def api_reabrir_caja():
    data = request.get_json(silent=True) or {}
    codigo_verificacion = str(data.get("codigo_verificacion", "") or "").strip()
    motivo_reapertura = str(data.get("motivo_reapertura", "") or "").strip()
    usuario = session["usuario"]["nombre"] if "usuario" in session else ""
    resultado = reabrir_arqueo_caja(
        reabierto_por=usuario,
        codigo_verificacion=codigo_verificacion,
        motivo_reapertura=motivo_reapertura,
    )
    status = 200 if resultado.get("ok") else 400
    if resultado.get("ok"):
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
        resultado = guardar_catalogo_productos(productos)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception:
        app.logger.exception("Error importando catalogo de productos")
        return jsonify({"ok": False, "error": "No se pudo importar el catalogo"}), 500

    return jsonify({
        "ok": True,
        "creados": resultado["creados"],
        "actualizados": resultado["actualizados"],
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
    data = request.json
    ok = agregar_usuario(
        data.get("nombre", "").strip(),
        data.get("pin", "").strip(),
        data.get("rol", "cajero"),
    )
    return jsonify({"ok": ok})


@app.route("/api/usuario/<int:uid>", methods=["DELETE"])
@login_required
def api_eliminar_usuario(uid):
    ok = eliminar_usuario(uid)
    return jsonify({"ok": ok})


# ── API Pedidos ──

@app.route("/api/pedido", methods=["POST"])
@login_required
def api_crear_pedido():
    data = request.json
    if not data or "items" not in data or not data["items"]:
        return jsonify({"ok": False, "error": "Sin items"}), 400

    mesa_id = data.get("mesa_id")
    notas = data.get("notas", "")
    mesero = session["usuario"]["nombre"] if "usuario" in session else ""

    items = []
    for item in data["items"]:
        entry = {
            "producto": item["producto"],
            "cantidad": int(item["cantidad"]),
            "precio_unitario": float(item["precio"]),
            "notas": item.get("notas", ""),
        }
        # Procesar modificaciones (adicionales/exclusiones)
        if "modificaciones" in item:
            entry["modificaciones"] = []
            for mod in item["modificaciones"]:
                descripcion = str(mod.get("descripcion", "") or "").strip()
                tipo = mod.get("tipo", "adicional")
                cantidad = int(mod.get("cantidad", 1) or 0)
                if not descripcion:
                    continue
                if tipo == "adicional" and cantidad <= 0:
                    continue
                if tipo == "exclusion":
                    cantidad = 1
                entry["modificaciones"].append({
                    "tipo": tipo,
                    "descripcion": descripcion,
                    "cantidad": cantidad,
                    "precio_extra": float(mod.get("precio_extra", 0)),
                })
        items.append(entry)

    validacion = validar_items_contra_produccion_panaderia(items)
    if not validacion["ok"]:
        return jsonify({
            "ok": False,
            "error": validacion["error"],
            "faltantes": validacion["faltantes"],
        }), 400

    pedido_id = crear_pedido(mesa_id, mesero, items, notas)
    if pedido_id:
        return jsonify({"ok": True, "pedido_id": pedido_id})
    return jsonify({"ok": False, "error": "No se pudo crear el pedido"}), 500


@app.route("/api/pedido/<int:pedido_id>/estado", methods=["PUT"])
@login_required
def api_cambiar_estado(pedido_id):
    data = request.json
    nuevo_estado = data.get("estado", "")
    if nuevo_estado not in ("pendiente", "en_preparacion", "listo", "pagado", "cancelado"):
        return jsonify({"ok": False, "error": "Estado invalido"}), 400

    if nuevo_estado == "pagado":
        pedido = obtener_pedido(pedido_id)
        if not pedido:
            return jsonify({"ok": False, "error": "Pedido no encontrado"}), 404
        validacion = validar_items_contra_produccion_panaderia(
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
        resultado = pagar_pedido(
            pedido_id,
            registrado_por=usuario,
            metodo_pago=metodo_pago,
            monto_recibido=monto_recibido,
        )
        status = 200 if resultado.get("ok") else 400
        if resultado.get("ok"):
            resultado["pedido"] = obtener_pedido(pedido_id)
            resultado["caja"] = obtener_resumen_caja_dia()
        return jsonify(resultado), status
    else:
        usuario = session["usuario"]["nombre"] if "usuario" in session else ""
        ok = cambiar_estado_pedido(pedido_id, nuevo_estado, cambiado_por=usuario)
        if ok and nuevo_estado == "cancelado":
            registrar_audit(
                usuario=usuario,
                accion="cancelar_pedido",
                entidad="pedido",
                entidad_id=str(pedido_id),
                detalle=f"Pedido #{pedido_id} cancelado",
            )
    return jsonify({"ok": ok})


@app.route("/api/pedido/<int:pedido_id>")
@login_required
def api_obtener_pedido(pedido_id):
    pedido = obtener_pedido(pedido_id)
    if pedido:
        return jsonify(pedido)
    return jsonify({"error": "Pedido no encontrado"}), 404


@app.route("/api/pedidos")
@login_required
def api_obtener_pedidos():
    estado = request.args.get("estado")
    mesa_id = request.args.get("mesa_id", type=int)
    pedidos = obtener_pedidos(estado=estado, mesa_id=mesa_id)
    return jsonify(pedidos)


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
    productos_panaderia = obtener_productos(categoria="Panaderia")

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


@app.context_processor
def utility_processor():
    """Variables globales disponibles en todos los templates."""
    return {
        "ahora": datetime.now(),
        "icono": icono,
        "color_prod": color_prod,
    }


# ══════════════════════════════════════════════
# PUNTO DE ENTRADA
# ══════════════════════════════════════════════

def _iniciar_scheduler():
    """Inicia el scheduler de backups automáticos diarios."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        backup_hour = int(os.environ.get("BACKUP_AUTO_HOUR", "23"))
        retention_days = int(os.environ.get("BACKUP_RETENTION_DAYS", "30"))

        def _backup_diario():
            result = crear_backup(f"Backup automático diario - {datetime.now().strftime('%Y-%m-%d')}")
            if result["ok"]:
                limpiar_backups_antiguos(dias_retencion=retention_days)
                print(f"[BACKUP] Backup automático completado: {result['backup']['archivo']}")
            else:
                print(f"[BACKUP ERROR] {result.get('error', 'Error desconocido')}")

        scheduler = BackgroundScheduler(daemon=True)
        scheduler.add_job(_backup_diario, "cron", hour=backup_hour, minute=0)
        scheduler.start()
        print(f"  Backup automático programado a las {backup_hour:02d}:00")
        return scheduler
    except ImportError:
        print("  [AVISO] APScheduler no instalado. Backups automáticos desactivados.")
        return None
    except Exception as e:
        print(f"  [AVISO] No se pudo iniciar scheduler de backups: {e}")
        return None


# Inicializar BD y scheduler al cargar el módulo (para Gunicorn)
inicializar_base_de_datos()
_scheduler = _iniciar_scheduler()


if __name__ == "__main__":
    # Backup automatico al iniciar en modo desarrollo
    result = crear_backup("Backup automatico al iniciar")
    if result["ok"]:
        print("  Backup automatico creado")
    limpiar_backups_antiguos()
    ip = _get_local_ip()
    print()
    print("=" * 50)
    print("  PANADERIA - Sistema de Ventas y Pronostico")
    print("=" * 50)
    print(f"  Abrir en navegador: http://{ip}:5000")
    print(f"  QR clientes:        http://{ip}:5000/cliente/pedido")
    print()
    print("  ADVERTENCIA: Cambia los PINes por defecto en Configuracion > Usuarios")
    print("=" * 50)
    print()
    app.run(host="0.0.0.0", port=5000, debug=os.environ.get("FLASK_ENV") == "development")
