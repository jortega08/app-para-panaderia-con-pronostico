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
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    jsonify,
    flash,
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
    registrar_ventas_lote,
    obtener_ventas_dia,
    obtener_resumen_ventas_dia,
    obtener_total_ventas_dia,
    obtener_vendido_dia_producto,
    hay_produccion_dia,
    validar_venta_producto,
    obtener_ventas_rango,
    obtener_totales_ventas_rango,
    obtener_serie_ventas_diarias,
    obtener_resumen_productos_rango,
    obtener_ficha_tecnica,
    obtener_fichas_tecnicas,
    guardar_ficha_tecnica,
)
from logic.pronostico import (
    calcular_pronostico,
    calcular_eficiencia,
    analizar_tendencia,
)

app = Flask(__name__)
app.secret_key = "panaderia-secret-key-2024"

# Colores por producto (sin iconos tipo emoji)
COLORES_PROD = {
    "Pan Frances": "#E8B44D",
    "Pan Dulce": "#E07A5F",
    "Croissant": "#81B29A",
    "Integral": "#9B8EA0",
}


def icono(nombre):
    return ""


def color_prod(nombre):
    return COLORES_PROD.get(nombre, "#B0BEC5")


# Decoradores

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "usuario" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated


def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            usuario = session.get("usuario")
            if not usuario:
                if request.path.startswith("/api/"):
                    return jsonify({"ok": False, "error": "Debes iniciar sesion"}), 401
                return redirect(url_for("login"))

            rol_actual = usuario.get("rol")
            if rol_actual not in roles:
                if request.path.startswith("/api/"):
                    return jsonify({"ok": False, "error": "No autorizado para este recurso"}), 403

                flash("No tienes permiso para acceder a esta seccion.", "error")
                destino = "cajero_pos" if rol_actual == "cajero" else "panadero_pronostico"
                return redirect(url_for(destino))

            return f(*args, **kwargs)

        return decorated

    return decorator


@app.after_request
def _add_no_cache_headers(response):
    """Evita volver a paginas sensibles por cache del navegador."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ======================================================
# RUTAS DE PAGINAS
# ======================================================

@app.route("/")
def index():
    if "usuario" not in session:
        return redirect(url_for("login"))
    if session["usuario"]["rol"] == "cajero":
        return redirect(url_for("cajero_pos"))
    return redirect(url_for("panadero_pronostico"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if "usuario" in session:
        return redirect(url_for("index"))

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
            return redirect(url_for("panadero_pronostico"))

        flash("PIN incorrecto", "error")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# Cajero

@app.route("/cajero/pos")
@role_required("cajero")
def cajero_pos():
    productos = obtener_productos_con_precio()
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])
    return render_template(
        "cajero_pos.html", productos=productos, layout="cajero", active_page="pos"
    )


@app.route("/cajero/ventas")
@role_required("cajero")
def cajero_ventas():
    return render_template("dashboard_ventas.html", layout="cajero", active_page="ventas")


# Panadero

@app.route("/panadero/pronostico")
@role_required("panadero")
def panadero_pronostico():
    productos = obtener_productos()
    producto_default = productos[0] if productos else ""
    return render_template(
        "panadero_pronostico.html",
        productos=productos,
        producto_default=producto_default,
        layout="panadero",
        active_page="pronostico",
    )


@app.route("/panadero/produccion", methods=["GET", "POST"])
@role_required("panadero")
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
                usuario = session.get("usuario", {}).get("nombre", "")
                ok = guardar_registro(
                    fecha,
                    producto,
                    producido,
                    vendido,
                    obs,
                    registrado_por=usuario,
                )
                if ok:
                    flash(f"Registro guardado: {producto} - {fecha}", "success")
                else:
                    flash("No se pudo guardar", "error")
        except (ValueError, KeyError) as e:
            flash(f"Datos invalidos: {e}", "error")

    productos = obtener_productos()
    registros_recientes = obtener_registros(dias=30)
    hoy = datetime.now().strftime("%Y-%m-%d")
    return render_template(
        "panadero_produccion.html",
        productos=productos,
        registros_recientes=registros_recientes,
        hoy=hoy,
        layout="panadero",
        active_page="produccion",
    )


@app.route("/panadero/ventas")
@role_required("panadero")
def panadero_ventas():
    return render_template("dashboard_ventas.html", layout="panadero", active_page="ventas")


@app.route("/panadero/historial")
@role_required("panadero")
def panadero_historial():
    producto = request.args.get("producto", "Todos")
    dias = int(request.args.get("dias", 30))
    productos = obtener_productos()

    return render_template(
        "panadero_historial.html",
        productos=productos,
        filtro_producto=producto,
        filtro_dias=dias,
        layout="panadero",
        active_page="historial",
    )


@app.route("/panadero/estandarizacion", methods=["GET", "POST"])
@role_required("panadero")
def panadero_estandarizacion():
    productos = obtener_productos()
    if not productos:
        flash("No hay productos activos para configurar fichas tecnicas.", "error")
        return render_template(
            "panadero_estandarizacion.html",
            productos=[],
            producto_actual="",
            ficha={},
            fichas=[],
            layout="panadero",
            active_page="estandarizacion",
        )

    if request.method == "POST":
        producto = request.form.get("producto", "").strip()
        if producto not in productos:
            flash("Producto invalido en la ficha tecnica.", "error")
            return redirect(url_for("panadero_estandarizacion"))

        try:
            tiempo_amasado_min = max(int(request.form.get("tiempo_amasado_min", "0")), 0)
            tiempo_fermentacion_min = max(int(request.form.get("tiempo_fermentacion_min", "0")), 0)
            temperatura_horneado_c = max(int(request.form.get("temperatura_horneado_c", "0")), 0)
            tiempo_horneado_min = max(int(request.form.get("tiempo_horneado_min", "0")), 0)
        except ValueError:
            flash("Los tiempos y temperatura deben ser numeros enteros.", "error")
            return redirect(url_for("panadero_estandarizacion", producto=producto))

        ingredientes = request.form.get("ingredientes", "").strip()
        cantidades = request.form.get("cantidades", "").strip()
        pasos_proceso = request.form.get("pasos_proceso", "").strip()
        actualizado_por = session.get("usuario", {}).get("nombre", "")

        ok = guardar_ficha_tecnica(
            producto,
            ingredientes,
            cantidades,
            tiempo_amasado_min,
            tiempo_fermentacion_min,
            temperatura_horneado_c,
            tiempo_horneado_min,
            pasos_proceso,
            actualizado_por,
        )
        if ok:
            flash(f"Ficha tecnica actualizada para {producto}.", "success")
        else:
            flash("No se pudo guardar la ficha tecnica.", "error")

        return redirect(url_for("panadero_estandarizacion", producto=producto))

    producto_actual = request.args.get("producto", productos[0])
    if producto_actual not in productos:
        producto_actual = productos[0]

    ficha = obtener_ficha_tecnica(producto_actual)
    fichas = obtener_fichas_tecnicas()

    return render_template(
        "panadero_estandarizacion.html",
        productos=productos,
        producto_actual=producto_actual,
        ficha=ficha,
        fichas=fichas,
        layout="panadero",
        active_page="estandarizacion",
    )

@app.route("/panadero/config")
@role_required("panadero")
def panadero_config():
    productos = obtener_productos_con_precio()
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])

    usuarios = obtener_usuarios()
    local_ip = _get_local_ip()
    qr_url = f"http://{local_ip}:5000/cliente/pedido"

    return render_template(
        "panadero_config.html",
        productos=productos,
        usuarios=usuarios,
        qr_url=qr_url,
        layout="panadero",
        active_page="config",
    )


# Cliente (publico, sin login)

@app.route("/cliente/pedido")
def cliente_pedido():
    productos = obtener_productos_con_precio()
    for p in productos:
        p["icono"] = icono(p["nombre"])
        p["color"] = color_prod(p["nombre"])
    return render_template("cliente_pedido.html", productos=productos)


# ======================================================
# API JSON
# ======================================================

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
    if not data or "items" not in data or not data["items"]:
        return jsonify({"ok": False, "error": "Sin datos de venta"}), 400

    items = data["items"]
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    if not hay_produccion_dia(fecha_hoy):
        return jsonify(
            {
                "ok": False,
                "error": "No hay produccion registrada para hoy. Registra produccion antes de vender.",
            }
        ), 400

    usuario = "Cliente"
    if "usuario" in session:
        usuario = session["usuario"]["nombre"]

    validaciones = []
    cantidad_por_producto = {}

    for item in items:
        producto = item.get("producto", "").strip()

        try:
            cantidad = int(item.get("cantidad", 0))
            precio = float(item.get("precio", 0))
        except (TypeError, ValueError):
            cantidad = 0
            precio = -1

        if not producto or cantidad <= 0 or precio < 0:
            validaciones.append(
                {
                    "ok": False,
                    "producto": producto or "?",
                    "error": "Datos invalidos de producto en la venta.",
                }
            )
            continue

        cantidad_por_producto[producto] = cantidad_por_producto.get(producto, 0) + cantidad

    for producto, cantidad_total in cantidad_por_producto.items():
        validaciones.append(validar_venta_producto(fecha_hoy, producto, cantidad_total))

    errores = [v for v in validaciones if not v.get("ok")]
    if errores:
        return jsonify(
            {
                "ok": False,
                "error": errores[0].get("error", "No se pudo registrar la venta"),
                "detalle": errores,
            }
        ), 400

    ok = registrar_ventas_lote(items, usuario)
    if not ok:
        return jsonify(
            {
                "ok": False,
                "error": "No se pudo guardar la venta en la base de datos.",
            }
        ), 500

    return jsonify({"ok": True, "detalle": validaciones})


@app.route("/api/ventas/hoy")
@role_required("panadero", "cajero")
def api_ventas_hoy():
    try:
        return jsonify(
            {
                "totales": obtener_total_ventas_dia(),
                "resumen": obtener_resumen_ventas_dia(),
                "ventas": obtener_ventas_dia(),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ventas/vendido")
@role_required("panadero", "cajero")
def api_vendido_dia():
    fecha = request.args.get("fecha", datetime.now().strftime("%Y-%m-%d"))
    producto = request.args.get("producto", "")
    if producto:
        vendido = obtener_vendido_dia_producto(fecha, producto)
        return jsonify({"vendido": vendido})
    return jsonify({"vendido": 0})


@app.route("/api/pronostico/dashboard")
@role_required("panadero")
def api_pronostico_dashboard():
    productos = obtener_productos()
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

    historial = list(reversed(obtener_registros(producto, dias=dias)))
    serie_ventas_producto = obtener_serie_ventas_diarias(dias=dias, producto=producto)
    ranking_productos = obtener_resumen_productos_rango(dias=dias)

    total_producido = sum(r.get("producido", 0) for r in historial)
    total_vendido = sum(r.get("vendido", 0) for r in historial)
    total_sobrante = sum(r.get("sobrante", 0) for r in historial)
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

    return jsonify({
        "producto": producto,
        "productos": productos,
        "prediccion_semana": prediccion_semana,
        "historial_producto": historial,
        "serie_ventas_producto": serie_ventas_producto,
        "ranking_productos": ranking_productos,
        "resumen": resumen,
    })


@app.route("/api/historial/dashboard")
@role_required("panadero")
def api_historial_dashboard():
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

    transacciones = totales.get("transacciones", 0)
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
                horas[key] += v.get("cantidad", 0)

    serie_horaria = [{"hora": k, "panes": v} for k, v in horas.items()]

    total_producido = sum(max(int(r.get("producido", 0) or 0), 0) for r in registros_operacion)
    total_vendido = sum(max(int(r.get("vendido", 0) or 0), 0) for r in registros_operacion)
    total_sobrante = sum(max(int(r.get("sobrante", 0) or 0), 0) for r in registros_operacion)
    total_faltante = sum(max(int(r.get("faltante", 0) or 0), 0) for r in registros_operacion)

    aprovechamiento = round((total_vendido / total_producido) * 100, 1) if total_producido else 0.0
    desperdicio = round((total_sobrante / total_producido) * 100, 1) if total_producido else 0.0
    dias_con_quiebre = sum(1 for r in registros_operacion if (r.get("faltante", 0) or 0) > 0)

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
        por_fecha[fecha]["producido"] += max(int(r.get("producido", 0) or 0), 0)
        por_fecha[fecha]["vendido"] += max(int(r.get("vendido", 0) or 0), 0)
        por_fecha[fecha]["sobrante"] += max(int(r.get("sobrante", 0) or 0), 0)
        por_fecha[fecha]["faltante"] += max(int(r.get("faltante", 0) or 0), 0)

    serie_operativa = [por_fecha[k] for k in sorted(por_fecha.keys())]

    return jsonify({
        "filtro_producto": producto,
        "dias": dias,
        "totales": {
            "panes": totales.get("panes", 0),
            "dinero": dinero,
            "transacciones": transacciones,
            "ticket_promedio": ticket_promedio,
        },
        "serie_diaria": serie_diaria,
        "resumen_productos": resumen_productos,
        "ventas_recientes": ventas_recientes,
        "serie_horaria": serie_horaria,
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
@app.route("/api/producto", methods=["POST"])
@role_required("panadero")
def api_agregar_producto():
    data = request.json
    nombre = data.get("nombre", "").strip()
    precio = float(data.get("precio", 0))
    if not nombre:
        return jsonify({"ok": False, "error": "Nombre vacio"}), 400
    ok = agregar_producto(nombre, precio)
    return jsonify({"ok": ok})


@app.route("/api/producto/precio", methods=["PUT"])
@role_required("panadero")
def api_actualizar_precio():
    data = request.json
    nombre = data.get("nombre", "")
    precio = float(data.get("precio", 0))
    ok = actualizar_precio(nombre, precio)
    return jsonify({"ok": ok})


@app.route("/api/usuario", methods=["POST"])
@role_required("panadero")
def api_agregar_usuario():
    data = request.json
    ok = agregar_usuario(
        data.get("nombre", "").strip(),
        data.get("pin", "").strip(),
        data.get("rol", "cajero"),
    )
    return jsonify({"ok": ok})


@app.route("/api/usuario/<int:uid>", methods=["DELETE"])
@role_required("panadero")
def api_eliminar_usuario(uid):
    ok = eliminar_usuario(uid)
    return jsonify({"ok": ok})


# ======================================================
# UTILIDADES
# ======================================================


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


# ======================================================
# PUNTO DE ENTRADA
# ======================================================

if __name__ == "__main__":
    inicializar_base_de_datos()
    ip = _get_local_ip()
    print()
    print("=" * 50)
    print("  RICH - Sistema de Ventas y Pronostico")
    print("=" * 50)
    print(f"  Abrir en navegador: http://{ip}:5000")
    print(f"  QR clientes:        http://{ip}:5000/cliente/pedido")
    print("  PIN Panadero: 1234  |  PIN Cajero: 0000")
    print("=" * 50)
    print()
    app.run(host="0.0.0.0", port=5000, debug=True)
