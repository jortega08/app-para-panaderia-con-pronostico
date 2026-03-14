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

import socket
from datetime import datetime
from functools import wraps

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, flash,
)

from data.database import (
    inicializar_base_de_datos,
    guardar_registro,
    obtener_registros,
    obtener_productos,
    obtener_productos_con_precio,
    agregar_producto,
    actualizar_precio,
    verificar_pin,
    obtener_usuarios,
    agregar_usuario,
    eliminar_usuario,
    registrar_venta,
    obtener_ventas_dia,
    obtener_resumen_ventas_dia,
    obtener_total_ventas_dia,
    obtener_vendido_dia_producto,
    obtener_mesas,
    agregar_mesa,
    eliminar_mesa,
    crear_pedido,
    obtener_pedidos,
    obtener_pedido,
    cambiar_estado_pedido,
    pagar_pedido,
    obtener_resumen_mesas,
    obtener_adicionales,
    agregar_adicional,
    actualizar_adicional,
    eliminar_adicional,
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
app.secret_key = "panaderia-secret-key-2024"

# ── Iconos y colores por producto ──
ICONOS = {
    "Pan Frances": "\U0001F956",
    "Pan Dulce": "\U0001F35E",
    "Croissant": "\U0001F950",
    "Integral": "\U0001F95E",
}
COLORES_PROD = {
    "Pan Frances": "#E8B44D",
    "Pan Dulce": "#E07A5F",
    "Croissant": "#81B29A",
    "Integral": "#9B8EA0",
}


def icono(nombre):
    return ICONOS.get(nombre, "\U0001F9C1")


def color_prod(nombre):
    return COLORES_PROD.get(nombre, "#B0BEC5")


# ── Decoradores ──

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "usuario" not in session:
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
        if not pin:
            flash("Escribe tu PIN", "error")
            return render_template("login.html")

        usuario = verificar_pin(pin)
        if usuario:
            session["usuario"] = usuario
            if usuario["rol"] == "cajero":
                return redirect(url_for("cajero_pos"))
            if usuario["rol"] == "mesero":
                return redirect(url_for("mesero_mesas"))
            return redirect(url_for("panadero_pronostico"))
        flash("PIN incorrecto", "error")
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
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])
    return render_template("cajero_pos.html",
                           productos=productos,
                           layout="cajero", active_page="pos")


@app.route("/cajero/ventas")
@login_required
def cajero_ventas():
    return render_template("dashboard_ventas.html",
                           layout="cajero", active_page="ventas")


@app.route("/cajero/pedidos")
@login_required
def cajero_pedidos():
    pedidos = obtener_pedidos()
    # Enriquecer con items
    for p in pedidos:
        detalle = obtener_pedido(p["id"])
        p["items"] = detalle["items"] if detalle else []
    return render_template("cajero_pedidos.html",
                           pedidos=pedidos,
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
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])
    adicionales = obtener_adicionales()
    mesas = obtener_mesas()
    mesa = next((m for m in mesas if m["id"] == mesa_id), None)
    if not mesa:
        flash("Mesa no encontrada", "error")
        return redirect(url_for("mesero_mesas"))
    return render_template("mesero_pedido.html",
                           mesa=mesa, productos=productos,
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
    productos = obtener_productos()
    datos = []
    for p in productos:
        try:
            r = calcular_pronostico(p)
            registros = obtener_registros(p, dias=7)
            ef = calcular_eficiencia(registros)
            tend = analizar_tendencia(registros)
            datos.append({
                "producto": p,
                "icono": icono(p),
                "color": color_prod(p),
                "sugerido": r.produccion_sugerida,
                "promedio": r.promedio_ventas,
                "dias": r.dias_historial,
                "estado": r.estado,
                "mensaje": r.mensaje,
                "confianza": r.confianza,
                "aprovechamiento": ef.get("tasa_aprovechamiento", 0) if ef else 0,
                "tendencia": tend,
            })
        except Exception:
            pass
    return render_template("panadero_pronostico.html",
                           pronosticos=datos,
                           layout="panadero", active_page="pronostico")


@app.route("/panadero/produccion", methods=["GET", "POST"])
@login_required
def panadero_produccion():
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
            else:
                ok = guardar_registro(fecha, producto, producido, vendido, obs)
                if ok:
                    flash(f"Registro guardado: {producto} - {fecha}", "success")
                else:
                    flash("No se pudo guardar", "error")
        except (ValueError, KeyError) as e:
            flash(f"Datos invalidos: {e}", "error")

    productos = obtener_productos()
    hoy = datetime.now().strftime("%Y-%m-%d")
    return render_template("panadero_produccion.html",
                           productos=productos, hoy=hoy,
                           layout="panadero", active_page="produccion")


@app.route("/panadero/ventas")
@login_required
def panadero_ventas():
    return render_template("dashboard_ventas.html",
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
    return render_template("panadero_operaciones.html",
                           stats=stats,
                           consumo=consumo,
                           insumos=insumos,
                           alertas_stock=alertas_stock,
                           mesas=mesas,
                           ventas_resumen=ventas_resumen,
                           ventas_total=ventas_total,
                           pedidos=pedidos,
                           layout="panadero", active_page="operaciones")


@app.route("/panadero/inventario")
@login_required
def panadero_inventario():
    insumos = obtener_insumos()
    productos = obtener_productos()
    recetas = {}
    for p in productos:
        recetas[p] = obtener_receta(p)
    alertas_stock = obtener_insumos_bajo_stock()
    return render_template("panadero_inventario.html",
                           insumos=insumos,
                           productos=productos,
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
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])

    usuarios = obtener_usuarios()
    local_ip = _get_local_ip()
    qr_url = f"http://{local_ip}:5000/cliente/pedido"

    return render_template("panadero_config.html",
                           productos=productos,
                           usuarios=usuarios,
                           qr_url=qr_url,
                           layout="panadero", active_page="config")


# ── Cliente (publico, sin login) ──

@app.route("/cliente/pedido")
def cliente_pedido():
    productos = obtener_productos_con_precio()
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])
    return render_template("cliente_pedido.html", productos=productos)


# ══════════════════════════════════════════════
# API JSON
# ══════════════════════════════════════════════

@app.route("/api/productos")
def api_productos():
    productos = obtener_productos_con_precio()
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])
    return jsonify(productos)


@app.route("/api/venta", methods=["POST"])
def api_venta():
    data = request.json
    if not data or "items" not in data:
        return jsonify({"ok": False, "error": "Sin datos"}), 400

    usuario = "Cliente"
    if "usuario" in session:
        usuario = session["usuario"]["nombre"]

    resultados = []
    for item in data["items"]:
        try:
            ok = registrar_venta(
                item["producto"],
                int(item["cantidad"]),
                float(item["precio"]),
                usuario,
            )
            resultados.append({"producto": item["producto"], "ok": ok})
        except Exception as e:
            resultados.append({"producto": item.get("producto", "?"),
                               "ok": False, "error": str(e)})

    return jsonify({
        "ok": all(r["ok"] for r in resultados),
        "detalle": resultados,
    })


@app.route("/api/ventas/hoy")
def api_ventas_hoy():
    try:
        return jsonify({
            "totales": obtener_total_ventas_dia(),
            "resumen": obtener_resumen_ventas_dia(),
            "ventas": obtener_ventas_dia(),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    ok = agregar_producto(nombre, precio)
    return jsonify({"ok": ok})


@app.route("/api/producto/precio", methods=["PUT"])
@login_required
def api_actualizar_precio():
    data = request.json
    nombre = data.get("nombre", "")
    precio = float(data.get("precio", 0))
    ok = actualizar_precio(nombre, precio)
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
                entry["modificaciones"].append({
                    "tipo": mod.get("tipo", "adicional"),
                    "descripcion": mod.get("descripcion", ""),
                    "cantidad": int(mod.get("cantidad", 1)),
                    "precio_extra": float(mod.get("precio_extra", 0)),
                })
        items.append(entry)

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
        usuario = session["usuario"]["nombre"] if "usuario" in session else ""
        ok = pagar_pedido(pedido_id, usuario)
    else:
        ok = cambiar_estado_pedido(pedido_id, nuevo_estado)
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
    return jsonify(obtener_adicionales())


@app.route("/api/adicional", methods=["POST"])
@login_required
def api_agregar_adicional():
    data = request.json
    nombre = data.get("nombre", "").strip()
    precio = float(data.get("precio", 0))
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    ok = agregar_adicional(nombre, precio)
    return jsonify({"ok": ok})


@app.route("/api/adicional/<int:aid>/precio", methods=["PUT"])
@login_required
def api_actualizar_adicional(aid):
    data = request.json
    precio = float(data.get("precio", 0))
    ok = actualizar_adicional(aid, precio)
    return jsonify({"ok": ok})


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


@app.route("/api/insumo/<int:iid>/stock", methods=["PUT"])
@login_required
def api_actualizar_stock(iid):
    data = request.json
    stock = float(data.get("stock", 0))
    ok = actualizar_stock(iid, stock)
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
    data = request.json
    ingredientes = data.get("ingredientes", [])
    ok = guardar_receta(producto, ingredientes)
    return jsonify({"ok": ok})


@app.route("/api/mesa", methods=["POST"])
@login_required
def api_agregar_mesa():
    data = request.json
    numero = int(data.get("numero", 0))
    nombre = data.get("nombre", "")
    if numero <= 0:
        return jsonify({"ok": False, "error": "Numero invalido"}), 400
    ok = agregar_mesa(numero, nombre)
    return jsonify({"ok": ok})


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

if __name__ == "__main__":
    inicializar_base_de_datos()
    # Backup automatico al iniciar
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
    print(f"  PIN Panadero: 1234  |  PIN Cajero: 0000  |  PIN Mesero: 1111")
    print("=" * 50)
    print()
    app.run(host="0.0.0.0", port=5000, debug=True)
