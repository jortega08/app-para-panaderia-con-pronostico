"""
Blueprint de autenticacion: /, /login, /logout, /health, /ready,
/favicon.ico, /cambiar-password, /api/cambiar-password.
"""
from datetime import datetime

from flask import (
    Blueprint,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from app.responses import json_error
from app.web.decorators import login_required, roles_required
from app.web.utils import (
    _build_brand_context,
    _client_ip,
    _LOCKOUT_MINUTES,
    _MAX_ATTEMPTS,
    _nombre_usuario_actual,
    _panaderia_actual_id,
    _registrar_sesion,
    _sede_actual_id,
)
from data.database import (
    abrir_jornada_sede,
    activar_jornada_usuario,
    cambiar_password_usuario,
    cerrar_jornada_sede,
    diagnosticar_login_operativo_local,
    desactivar_jornada_usuario,
    get_connection,
    limpiar_login_attempts,
    listar_operativos_activos_por_pin,
    obtener_configuracion_login_operativo,
    obtener_estado_login_attempts,
    obtener_panaderia_por_codigo,
    obtener_terminal_lookup,
    obtener_usuarios_jornada,
    registrar_audit,
    registrar_login_exitoso,
    registrar_login_attempts_fallido,
    verificar_password,
    verificar_usuario_operativo_local,
)

auth_bp = Blueprint("auth", __name__)


def _redirect_post_login(usuario: dict):
    """Redirige al usuario a su seccion segun el rol, tras un login exitoso."""
    rol = str(usuario.get("rol") or "").strip()
    if usuario.get("must_change_password") and rol not in ("cajero", "mesero", "platform_superadmin"):
        return redirect(url_for("auth.cambiar_password"))
    if rol == "cajero":
        return redirect(url_for("cajero_pos"))
    if rol == "mesero":
        return redirect(url_for("mesero_mesas"))
    if rol == "platform_superadmin":
        return redirect(url_for("platform_panel"))
    return redirect(url_for("panadero_pronostico"))


def _login_context() -> dict:
    return {
        "login_operativo": obtener_configuracion_login_operativo(_panaderia_actual_id()),
    }


def _render_login():
    return render_template("login.html", **_login_context())


@auth_bp.route("/")
def index():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))
    rol = session["usuario"]["rol"]
    if rol == "cajero":
        return redirect(url_for("cajero_pos"))
    if rol == "mesero":
        return redirect(url_for("mesero_mesas"))
    if rol == "platform_superadmin":
        return redirect(url_for("platform_panel"))
    return redirect(url_for("panadero_pronostico"))


@auth_bp.route("/health")
def health_check():
    return jsonify({"status": "healthy"}), 200


@auth_bp.route("/favicon.ico")
def favicon():
    brand = getattr(g, "brand_context", _build_brand_context())
    return redirect(url_for("static", filename=brand.favicon_path, v="20260416-platform"))


@auth_bp.route("/ready")
def readiness_check():
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready"}), 200
    except Exception as e:
        return jsonify({"status": "not_ready", "error": str(e)}), 503


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        ip = _client_ip()
        scope_key = f"login:ip:{ip}"

        entry = obtener_estado_login_attempts(scope_key)
        locked_until = str(entry.get("locked_until", "") or "").strip()
        if locked_until:
            try:
                locked_until_dt = datetime.strptime(locked_until, "%Y-%m-%d %H:%M:%S")
                if locked_until_dt > datetime.now():
                    restante = max(1, int((locked_until_dt - datetime.now()).total_seconds() / 60) + 1)
                    flash(f"Demasiados intentos. Espera {restante} minuto(s).", "error")
                    return _render_login()
                limpiar_login_attempts([scope_key])
            except ValueError:
                limpiar_login_attempts([scope_key])

        modo = request.form.get("modo", "operativo").strip()
        usuario = None
        metodo = ""
        login_operativo = _login_context()["login_operativo"]
        terminal_context = getattr(g, "terminal_context", None)
        terminal_id = terminal_context.id if terminal_context is not None and getattr(terminal_context, "available", False) else None

        if modo == "operativo":
            pin = request.form.get("pin", "").strip()
            if not pin:
                flash("Escribe tu PIN.", "error")
                return _render_login()

            if login_operativo.get("requiere_username"):
                username_op = request.form.get("username_op", "").strip()
                if not username_op:
                    coincidencias = listar_operativos_activos_por_pin(login_operativo["panaderia_id"], pin)
                    if len(coincidencias) == 1:
                        usuario = coincidencias[0]
                        metodo = "pin_only_legacy"
                    elif len(coincidencias) > 1:
                        flash(
                            "Ese PIN coincide con mas de un usuario operativo activo. "
                            "Escribe tu username exacto o pide al administrador actualizarlo.",
                            "warning",
                        )
                        return _render_login()
                    else:
                        diagnostico = diagnosticar_login_operativo_local(
                            login_operativo["panaderia_id"],
                            pin,
                            requiere_username=False,
                        )
                        if diagnostico.get("status") == "jornada_cerrada":
                            flash("La jornada esta cerrada. Solicita a un administrador abrir la jornada.", "error")
                            return _render_login()
                        if diagnostico.get("status") == "pin_duplicado":
                            flash(
                                "Ese PIN coincide con mas de un usuario operativo activo. "
                                "Escribe tu username exacto o pide al administrador actualizarlo.",
                                "warning",
                            )
                            return _render_login()
                        intento = registrar_login_attempts_fallido(scope_key, _MAX_ATTEMPTS, _LOCKOUT_MINUTES)
                        restantes = max(0, _MAX_ATTEMPTS - int(intento.get("attempts", 0) or 0))
                        flash(f"PIN incorrecto. Intentos restantes: {restantes}", "error")
                        return _render_login()
                if not usuario:
                    usuario = verificar_usuario_operativo_local(login_operativo["panaderia_id"], username_op, pin)
                if not metodo:
                    metodo = "pin_username"
                if not usuario:
                    diagnostico = diagnosticar_login_operativo_local(
                        login_operativo["panaderia_id"],
                        pin,
                        username=username_op,
                        requiere_username=True,
                    )
                    if diagnostico.get("status") == "jornada_cerrada":
                        flash("La jornada está cerrada. Solicita a un administrador abrir la jornada.", "error")
                        return _render_login()
                    if diagnostico.get("status") == "identificador_duplicado":
                        flash(
                            "Ese nombre coincide con más de un usuario operativo activo. "
                            "Usa tu username exacto o pide al administrador actualizarlo.",
                            "warning",
                        )
                        return _render_login()
                    intento = registrar_login_attempts_fallido(scope_key, _MAX_ATTEMPTS, _LOCKOUT_MINUTES)
                    restantes = max(0, _MAX_ATTEMPTS - int(intento.get("attempts", 0) or 0))
                    flash(f"Usuario, nombre o PIN incorrecto. Intentos restantes: {restantes}", "error")
                    return _render_login()
            else:
                coincidencias = listar_operativos_activos_por_pin(login_operativo["panaderia_id"], pin)
                if len(coincidencias) == 1:
                    usuario = coincidencias[0]
                    metodo = "pin_only"
                elif len(coincidencias) > 1:
                    flash(
                        "Ese PIN coincide con mas de un usuario operativo activo. "
                        "Pide al administrador un reseteo de PIN.",
                        "warning",
                    )
                    return _render_login()
                else:
                    diagnostico = diagnosticar_login_operativo_local(
                        login_operativo["panaderia_id"],
                        pin,
                        requiere_username=False,
                    )
                    if diagnostico.get("status") == "jornada_cerrada":
                        flash("La jornada está cerrada. Solicita a un administrador abrir la jornada.", "error")
                        return _render_login()
                    if diagnostico.get("status") == "pin_duplicado":
                        flash(
                            "Ese PIN coincide con mas de un usuario operativo activo. "
                            "Pide al administrador un reseteo de PIN.",
                            "warning",
                        )
                        return _render_login()
                    intento = registrar_login_attempts_fallido(scope_key, _MAX_ATTEMPTS, _LOCKOUT_MINUTES)
                    restantes = max(0, _MAX_ATTEMPTS - int(intento.get("attempts", 0) or 0))
                    flash(f"PIN incorrecto. Intentos restantes: {restantes}", "error")
                    return _render_login()
        else:
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")

            if not password:
                flash("Escribe tu contrasena.", "error")
                return _render_login()

            if not username:
                flash("Escribe tu usuario, correo o nombre y tu contrasena.", "error")
                return _render_login()

            usuario = verificar_password(username, password)
            metodo = "password"

            if not usuario:
                intento = registrar_login_attempts_fallido(scope_key, _MAX_ATTEMPTS, _LOCKOUT_MINUTES)
                if str(intento.get("locked_until", "") or "").strip():
                    flash(f"Demasiados intentos fallidos. Espera {_LOCKOUT_MINUTES} minutos.", "error")
                else:
                    restantes = max(0, _MAX_ATTEMPTS - int(intento.get("attempts", 0) or 0))
                    flash(f"Usuario, correo, nombre o contrasena incorrectos. Intentos restantes: {restantes}", "error")
                return _render_login()

        limpiar_login_attempts([scope_key])
        if terminal_id:
            usuario = dict(usuario)
            usuario["terminal_id"] = terminal_id
        registrar_login_exitoso(int(usuario["id"]), terminal_id=terminal_id)
        _registrar_sesion(usuario)

        registrar_audit(
            usuario=usuario["nombre"],
            usuario_id=usuario.get("id"),
            panaderia_id=usuario.get("panaderia_id"),
            sede_id=usuario.get("sede_id"),
            ip=ip,
            user_agent=request.headers.get("User-Agent", ""),
            request_id=getattr(g, "request_id", ""),
            accion="login",
            entidad="usuario",
            entidad_id=str(usuario.get("id", "")),
            detalle=f"Login exitoso - rol: {usuario['rol']} - metodo: {metodo}",
            resultado="ok",
        )
        return _redirect_post_login(usuario)

    return _render_login()


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))


@auth_bp.route("/cambiar-password", methods=["GET"])
@login_required
def cambiar_password():
    return render_template("cambiar_password.html")


@auth_bp.route("/api/cambiar-password", methods=["POST"])
@login_required
def api_cambiar_password():
    data = request.get_json(silent=True) or {}
    nueva = str(data.get("nueva_password", "")).strip()
    confirmar = str(data.get("confirmar_password", "")).strip()
    usuario_id = session.get("usuario", {}).get("id")

    if not nueva or len(nueva) < 8:
        return json_error("La contrasena debe tener al menos 8 caracteres", 400)
    if nueva != confirmar:
        return json_error("Las contrasenas no coinciden", 400)

    ok = cambiar_password_usuario(usuario_id, nueva)
    if not ok:
        return json_error("No se pudo cambiar la contrasena", 500)

    usuario_sesion = session.get("usuario", {})
    usuario_sesion["must_change_password"] = 0
    session["usuario"] = usuario_sesion

    registrar_audit(
        usuario=usuario_sesion.get("nombre", ""),
        usuario_id=usuario_id,
        panaderia_id=usuario_sesion.get("panaderia_id"),
        sede_id=usuario_sesion.get("sede_id"),
        ip=_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="cambiar_password",
        entidad="usuario",
        entidad_id=str(usuario_id or ""),
        detalle="Cambio de contrasena exitoso",
        resultado="ok",
    )
    return jsonify({"ok": True, "data": {"redirect": url_for("panadero_pronostico")}})


@auth_bp.route("/api/terminal/lookup")
def api_terminal_lookup():
    """Retorna info publica de una terminal dado su codigo. Sin autenticacion requerida."""
    codigo = request.args.get("codigo", "").strip().upper()
    if not codigo:
        return jsonify({"ok": False, "error": "Falta el codigo de terminal"}), 400
    terminal = obtener_terminal_lookup(codigo)
    if not terminal:
        return jsonify({"ok": False, "error": "Terminal no reconocida o inactiva"}), 404
    return jsonify({
        "ok": True,
        "data": {
            "codigo": terminal["codigo"],
            "terminal_nombre": terminal["terminal_nombre"],
            "tipo": terminal["tipo"],
            "sede_nombre": terminal["sede_nombre"],
            "panaderia_nombre": terminal["panaderia_nombre"],
            "panaderia_codigo": terminal["panaderia_codigo"],
        },
    })


@auth_bp.route("/api/panaderia/lookup")
def api_panaderia_lookup():
    """Compatibilidad: devuelve nombre de panaderia por codigo corto."""
    codigo = request.args.get("codigo", "").strip()
    if not codigo:
        return jsonify({"ok": False, "error": "Falta el codigo"}), 400
    panaderia = obtener_panaderia_por_codigo(codigo)
    if not panaderia:
        return jsonify({"ok": False, "error": "Codigo no reconocido"}), 404
    return jsonify({"ok": True, "data": {"nombre": panaderia["nombre"], "codigo": panaderia["codigo"]}})


_ROLES_JORNADA = ("panadero", "tenant_admin", "platform_superadmin")


@auth_bp.route("/api/jornada/usuarios")
@login_required
@roles_required(*_ROLES_JORNADA)
def api_jornada_usuarios():
    """Lista cajeros/meseros de la panaderia activa con estado de jornada."""
    panaderia_id = _panaderia_actual_id()
    sede_id = _sede_actual_id()
    if not panaderia_id:
        return jsonify({"ok": False, "error": "Contexto de panaderia no disponible"}), 400
    usuarios = obtener_usuarios_jornada(panaderia_id, sede_id)
    return jsonify({"ok": True, "data": usuarios})


@auth_bp.route("/api/jornada/abrir", methods=["POST"])
@login_required
@roles_required(*_ROLES_JORNADA)
def api_jornada_abrir():
    """Abre jornada para todos los cajeros/meseros de la sede activa."""
    panaderia_id = _panaderia_actual_id()
    sede_id = _sede_actual_id()
    activado_por = _nombre_usuario_actual()
    if not panaderia_id or not sede_id:
        return jsonify({"ok": False, "error": "Contexto no disponible"}), 400
    cantidad = abrir_jornada_sede(panaderia_id, sede_id, activado_por)
    registrar_audit(
        usuario=activado_por,
        usuario_id=session.get("usuario", {}).get("id"),
        panaderia_id=panaderia_id,
        sede_id=sede_id,
        ip=_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="abrir_jornada",
        entidad="sede",
        entidad_id=str(sede_id),
        detalle=f"Jornada abierta: {cantidad} usuario(s) activados",
        resultado="ok",
    )
    return jsonify({"ok": True, "data": {"activados": cantidad}})


@auth_bp.route("/api/jornada/cerrar", methods=["POST"])
@login_required
@roles_required(*_ROLES_JORNADA)
def api_jornada_cerrar():
    """Cierra jornada para todos los cajeros/meseros de la sede activa."""
    panaderia_id = _panaderia_actual_id()
    sede_id = _sede_actual_id()
    if not panaderia_id or not sede_id:
        return jsonify({"ok": False, "error": "Contexto no disponible"}), 400
    cantidad = cerrar_jornada_sede(panaderia_id, sede_id)
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=session.get("usuario", {}).get("id"),
        panaderia_id=panaderia_id,
        sede_id=sede_id,
        ip=_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="cerrar_jornada",
        entidad="sede",
        entidad_id=str(sede_id),
        detalle=f"Jornada cerrada: {cantidad} usuario(s) desactivados",
        resultado="ok",
    )
    return jsonify({"ok": True, "data": {"desactivados": cantidad}})


@auth_bp.route("/api/jornada/usuario/<int:usuario_id>/activar", methods=["POST"])
@login_required
@roles_required(*_ROLES_JORNADA)
def api_jornada_activar_usuario(usuario_id: int):
    """Activa la jornada de un usuario individual."""
    panaderia_id = _panaderia_actual_id()
    sede_id = _sede_actual_id()
    if not panaderia_id:
        return jsonify({"ok": False, "error": "Contexto no disponible"}), 400
    activar_jornada_usuario(usuario_id, _nombre_usuario_actual(), panaderia_id)
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=session.get("usuario", {}).get("id"),
        panaderia_id=panaderia_id,
        sede_id=sede_id,
        ip=_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="activar_jornada_usuario",
        entidad="usuario",
        entidad_id=str(usuario_id),
        detalle="Jornada activada manualmente para usuario operativo",
        resultado="ok",
    )
    return jsonify({"ok": True})


@auth_bp.route("/api/jornada/usuario/<int:usuario_id>/desactivar", methods=["POST"])
@login_required
@roles_required(*_ROLES_JORNADA)
def api_jornada_desactivar_usuario(usuario_id: int):
    """Desactiva la jornada de un usuario individual."""
    panaderia_id = _panaderia_actual_id()
    sede_id = _sede_actual_id()
    if not panaderia_id:
        return jsonify({"ok": False, "error": "Contexto no disponible"}), 400
    desactivar_jornada_usuario(usuario_id, panaderia_id)
    registrar_audit(
        usuario=_nombre_usuario_actual(),
        usuario_id=session.get("usuario", {}).get("id"),
        panaderia_id=panaderia_id,
        sede_id=sede_id,
        ip=_client_ip(),
        user_agent=request.headers.get("User-Agent", ""),
        request_id=getattr(g, "request_id", ""),
        accion="desactivar_jornada_usuario",
        entidad="usuario",
        entidad_id=str(usuario_id),
        detalle="Jornada desactivada manualmente para usuario operativo",
        resultado="ok",
    )
    return jsonify({"ok": True})
