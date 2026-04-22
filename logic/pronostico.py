from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from data.database import (
    obtener_registros,
    obtener_serie_ventas_diarias,
    obtener_factor_dia_especial,
    obtener_ajuste_pronostico,
    obtener_demanda_comprometida_encargos,
)


# ── Configuración del modelo ──────────────────────────────────────────────────
BUFFER_SEGURIDAD = 0.10
UMBRAL_SOBREPRODUCCION = 0.15
DIAS_PROMEDIO_MOVIL = 7
DIAS_NIVEL_ALTO = 30

# Pesos del modelo mixto — deben sumar 1.0
PESO_DIA_SEMANA = 0.50   # Histórico del mismo día de semana
PESO_RECIENTE_7 = 0.30   # Últimos 7 días
PESO_TENDENCIA  = 0.20   # Componente de tendencia (promedio reciente * factor_tendencia)

AJUSTE_TENDENCIA = {
    "subiendo": 1.08,
    "bajando":  0.95,
    "estable":  1.00,
    "sin datos": 1.00,
}

TIPO_DIA = {
    "Lunes":     "laboral",
    "Martes":    "laboral",
    "Miercoles": "laboral",
    "Jueves":    "laboral",
    "Viernes":   "viernes",
    "Sabado":    "fin_semana",
    "Domingo":   "fin_semana",
}

VALORES_BASE_PRODUCTO = {
    "pan frances": 80,
    "pan dulce":   35,
    "croissant":   30,
    "integral":    25,
}

# Umbral de outlier: días cuya venta supera N desviaciones estándar se excluyen
OUTLIER_DESVIACIONES = 2.5


@dataclass
class ResultadoPronostico:
    producto: str
    produccion_sugerida: int   # Cuántos hornear (neto: demanda − stock − pendiente + encargos)
    venta_estimada: int        # Cuántos se esperan vender (demanda bruta del modelo)
    modelo_usado: str
    promedio_ventas: float
    dias_historial: int
    nivel_calidad: float       # 0-6 basado en CV del patrón de ventas
    estado: str                # "bien" | "alerta" | "problema"
    mensaje: str
    confianza: str             # "poca" | "media" | "buena"
    demanda_comprometida: int = 0
    detalles: dict = field(default_factory=dict)

    @property
    def demanda_estimada(self) -> int:
        return int(self.venta_estimada or 0)

    @demanda_estimada.setter
    def demanda_estimada(self, valor: int) -> None:
        self.venta_estimada = int(valor or 0)

    @property
    def produccion_recomendada(self) -> int:
        return int(self.produccion_sugerida or 0)

    @produccion_recomendada.setter
    def produccion_recomendada(self, valor: int) -> None:
        self.produccion_sugerida = int(valor or 0)


# ── Función principal ─────────────────────────────────────────────────────────

def _nombre_dia_semana_es(fecha: str) -> str:
    dia_obj = datetime.strptime(fecha, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miercoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sabado", "Sunday": "Domingo",
    }
    return dias_es.get(dia_obj, dia_obj)


def obtener_historial_pronostico(
    producto: str,
    dias: int | None = 30,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    dias_consulta = max(int(dias or 3650), 1)
    registros_base = obtener_registros(
        producto, dias=dias_consulta,
        fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
    )
    ventas_base = obtener_serie_ventas_diarias(
        dias=dias_consulta, producto=producto,
        fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
    )

    historial_por_fecha: dict[str, dict] = {}

    for registro in registros_base:
        fecha = str(registro.get("fecha", "") or "").strip()
        if not fecha:
            continue
        producido = int(registro.get("producido", 0) or 0)
        vendido_manual = int(registro.get("vendido", 0) or 0)
        sobrante_inicial = int(registro.get("sobrante_inicial", 0) or 0)
        sobrante_operativo = max(sobrante_inicial + producido - vendido_manual, 0)
        historial_por_fecha[fecha] = {
            "fecha": fecha,
            "dia_semana": str(registro.get("dia_semana", "") or "").strip() or _nombre_dia_semana_es(fecha),
            "producto": producto,
            "producido": producido,
            "vendido": vendido_manual,
            "vendido_manual": vendido_manual,
            "vendido_real": vendido_manual,
            "sobrante_inicial": sobrante_inicial,
            "sobrante": sobrante_operativo,
            "sobrante_total": sobrante_operativo,
            "observaciones": str(registro.get("observaciones", "") or "").strip(),
        }

    for venta in ventas_base:
        fecha = str(venta.get("fecha", "") or "").strip()
        if not fecha:
            continue
        vendido_real = int(venta.get("panes", 0) or 0)
        registro = historial_por_fecha.get(fecha)
        if registro is None:
            historial_por_fecha[fecha] = {
                "fecha": fecha,
                "dia_semana": _nombre_dia_semana_es(fecha),
                "producto": producto,
                "producido": 0,
                "vendido": vendido_real,
                "vendido_manual": 0,
                "vendido_real": vendido_real,
                "sobrante_inicial": 0,
                "sobrante": 0,
                "sobrante_total": 0,
                "observaciones": "Ventas reales sin registro diario de produccion",
            }
            continue
        registro["vendido_real"] = vendido_real
        registro["vendido"] = max(int(registro.get("vendido_manual", 0) or 0), vendido_real)
        registro["sobrante"] = max(
            int(registro.get("sobrante_inicial", 0) or 0)
            + int(registro.get("producido", 0) or 0)
            - int(registro["vendido"] or 0),
            0,
        )
        registro["sobrante_total"] = registro["sobrante"]

    return sorted(
        historial_por_fecha.values(),
        key=lambda item: str(item.get("fecha", "") or ""),
        reverse=True,
    )


def obtener_resumen_pronostico_por_dia_semana(
    producto: str,
    dias: int | None = 90,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> dict:
    historial = obtener_historial_pronostico(
        producto, dias=dias,
        fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
    )
    apoyo = ""
    agrupado: dict[str, list[int]] = defaultdict(list)
    for registro in historial:
        dia = str(registro.get("dia_semana", "") or "").strip()
        if not dia:
            continue
        agrupado[dia].append(int(registro.get("vendido", 0) or 0))

    apoyo = apoyo.replace("Â·", "|").replace("·", "|")

    return {
        dia: {
            "promedio": round(sum(valores) / len(valores), 1) if valores else 0.0,
            "muestras": len(valores),
            "registros": len(valores),
        }
        for dia, valores in agrupado.items()
    }


def calcular_pronostico(
    producto: str,
    fecha_objetivo: Optional[str] = None,
    buffer: float = BUFFER_SEGURIDAD,
    stock_actual: Optional[int] = None,
    produccion_pendiente: Optional[int] = None,
    pedidos_registrados: int = 0,
    encargos_confirmados: int = 0,
) -> ResultadoPronostico:
    """
    Calcula cuántos panes hornear de un producto.

    Selecciona el modelo según datos disponibles.
    Con datos suficientes usa blend 50/30/20: mismo día + últimos 7d + componente tendencia.
    Ajusta por stock actual, producción pendiente, pedidos y encargos confirmados.

    encargos_confirmados: unidades ya comprometidas via encargos (con_anticipo/programado/listo)
    """
    if fecha_objetivo is None:
        fecha_objetivo = datetime.now().strftime("%Y-%m-%d")

    dias_disponibles = len(obtener_historial_pronostico(producto, dias=None))

    ajuste_manual = obtener_ajuste_pronostico(fecha_objetivo, producto)

    # El ajuste manual se aplica al resultado final, despues de calcular demanda
    # estimada, demanda comprometida y produccion recomendada del sistema.

    stock_actual_val = max(int(stock_actual or 0), 0)
    produccion_pendiente_val = max(int(produccion_pendiente or 0), 0)

    if dias_disponibles < 7:
        resultado = _modelo_inicial(producto, dias_disponibles, buffer, fecha_objetivo)
    elif dias_disponibles < DIAS_NIVEL_ALTO:
        resultado = _modelo_promedio_semanal(producto, dias_disponibles, buffer, fecha_objetivo)
    else:
        resultado = _modelo_mixto(producto, dias_disponibles, fecha_objetivo, buffer)

    # ── Ajuste por días especiales ───────────────────────────────────────────
    factor_especial = obtener_factor_dia_especial(fecha_objetivo)
    if factor_especial != 1.0:
        venta_ajustada = resultado.venta_estimada * factor_especial
        resultado.venta_estimada = _redondear_produccion(venta_ajustada)
        resultado.detalles["factor_dia_especial"] = round(factor_especial, 2)
        resultado.mensaje += f" Factor día especial: {factor_especial:.0%}."

    # ── Bug 3 fix: pedidos_registrados + encargos siempre se aplican ────────
    # pedidos comprometidos = registrados en caja + encargos confirmados
    comprometidos = max(int(pedidos_registrados or 0), 0) + max(int(encargos_confirmados or 0), 0)
    demanda_total_operativa = max(int(resultado.venta_estimada or 0), 0) + comprometidos
    cobertura_actual = stock_actual_val + produccion_pendiente_val

    # Bug 4 fix: separar venta_estimada del neto de producción
    # hornear = demanda esperada − disponible ya (stock + pendiente) + comprometidos ya vendidos
    hornear = max(resultado.venta_estimada - stock_actual_val - produccion_pendiente_val + comprometidos, 0)
    resultado.demanda_comprometida = comprometidos
    resultado.produccion_recomendada = _redondear_produccion(hornear * (1 + buffer))

    if ajuste_manual is not None and ajuste_manual.get("ajustado") is not None:
        produccion_sistema = int(resultado.produccion_recomendada or 0)
        ajustado = _redondear_produccion(float(ajuste_manual.get("ajustado") or 0))
        resultado.produccion_recomendada = ajustado
        resultado.detalles["produccion_recomendada_sistema"] = produccion_sistema
        resultado.detalles["ajuste_manual"] = ajuste_manual
        resultado.detalles["ajuste_motivo"] = ajuste_manual.get("motivo", "")
        resultado.mensaje += (
            f" Ajuste manual aplicado al resultado final: {ajustado} unidades."
            f" Motivo: {ajuste_manual.get('motivo', '') or 'Sin motivo'}."
        )

    resultado.detalles["demanda_estimada"] = resultado.demanda_estimada
    resultado.detalles["demanda_comprometida"] = resultado.demanda_comprometida
    resultado.detalles["produccion_recomendada"] = resultado.produccion_recomendada
    resultado.detalles["venta_estimada_modelo"] = resultado.venta_estimada
    resultado.detalles["stock_actual"] = stock_actual_val
    resultado.detalles["stock_actual_disponible"] = stock_actual is not None
    resultado.detalles["produccion_pendiente"] = produccion_pendiente_val
    resultado.detalles["produccion_pendiente_disponible"] = produccion_pendiente is not None
    resultado.detalles["pedidos_registrados"] = pedidos_registrados
    resultado.detalles["encargos_confirmados"] = encargos_confirmados
    resultado.detalles["comprometidos_total"] = comprometidos
    resultado.detalles["demanda_total_operativa"] = demanda_total_operativa
    resultado.detalles["cobertura_actual"] = cobertura_actual
    resultado.detalles["cobertura_total_planificada"] = cobertura_actual + int(resultado.produccion_recomendada or 0)
    resultado.detalles["margen_planificado"] = (
        resultado.detalles["cobertura_total_planificada"] - demanda_total_operativa
    )
    resultado.detalles["hornear_neto"] = hornear

    return resultado


def generar_lectura_operativa(
    resultado: Optional[ResultadoPronostico],
    *,
    producto_seleccionado: bool = True,
) -> dict:
    if not producto_seleccionado:
        return {
            "titulo": "Selecciona un producto",
            "mensaje": "Elige un producto y actualiza el pronostico para ver una lectura operativa util.",
            "lineas": [],
            "nivel": "neutro",
            "estado": "sin_producto",
            "badge": "Sin producto",
            "apoyo": "Sin producto seleccionado.",
            "riesgo_quiebre": "sin_datos",
            "riesgo_sobrante": "sin_datos",
            "urgencia": "sin_datos",
            "accion": "",
        }

    if resultado is None:
        return {
            "titulo": "Sin datos suficientes",
            "mensaje": "Todavia no hay una base confiable para resumir la operacion de hoy.",
            "lineas": [],
            "nivel": "neutro",
            "estado": "sin_datos",
            "badge": "Sin datos",
            "apoyo": "No fue posible construir la lectura operativa.",
            "riesgo_quiebre": "sin_datos",
            "riesgo_sobrante": "sin_datos",
            "urgencia": "sin_datos",
            "accion": "",
        }

    detalles = getattr(resultado, "detalles", {}) or {}
    metricas = detalles.get("metricas", {}) or {}

    demanda_estimada = max(int(resultado.demanda_estimada or 0), 0)
    demanda_comprometida = max(int(resultado.demanda_comprometida or 0), 0)
    produccion_recomendada = max(int(resultado.produccion_recomendada or 0), 0)
    demanda_total = max(int(detalles.get("demanda_total_operativa", demanda_estimada + demanda_comprometida) or 0), 0)
    stock_actual = max(int(detalles.get("stock_actual", 0) or 0), 0)
    produccion_pendiente = max(int(detalles.get("produccion_pendiente", 0) or 0), 0)
    cobertura_actual = max(int(detalles.get("cobertura_actual", stock_actual + produccion_pendiente) or 0), 0)
    cobertura_planificada = max(
        int(detalles.get("cobertura_total_planificada", cobertura_actual + produccion_recomendada) or 0),
        0,
    )
    margen_planificado = int(detalles.get("margen_planificado", cobertura_planificada - demanda_total) or 0)
    faltante_actual = max(demanda_total - cobertura_actual, 0)

    stock_disponible = bool(detalles.get("stock_actual_disponible"))
    pendiente_disponible = bool(detalles.get("produccion_pendiente_disponible"))

    tasa_sobrante = float(metricas.get("tasa_sobrante", detalles.get("tasa_sobrante", 0)) or 0)
    tasa_quiebre = float(metricas.get("tasa_quiebre", 0) or 0)
    tasa_tope = float(metricas.get("tasa_tope", 0) or 0)

    share_comprometida = (demanda_comprometida / demanda_total) if demanda_total > 0 else 0.0
    encargos_altos = demanda_comprometida > 0 and (
        share_comprometida >= 0.35 or demanda_comprometida >= max(8, math.ceil(demanda_estimada * 0.25))
    )
    confianza_baja = (
        str(resultado.confianza or "").lower() == "poca"
        or float(resultado.nivel_calidad or 0) < 2.5
        or int(resultado.dias_historial or 0) < 7
    )

    riesgo_quiebre = "bajo"
    if demanda_total <= 0:
        riesgo_quiebre = "bajo"
    elif cobertura_planificada < demanda_total:
        riesgo_quiebre = "alto"
    elif demanda_comprometida > 0 and cobertura_actual < demanda_comprometida:
        riesgo_quiebre = "alto"
    elif faltante_actual >= max(6, math.ceil(demanda_total * 0.25)):
        riesgo_quiebre = "alto"
    elif faltante_actual > 0 or tasa_quiebre >= 20 or tasa_tope >= 40:
        riesgo_quiebre = "medio"
    elif confianza_baja and margen_planificado <= max(2, math.ceil(demanda_total * 0.08)):
        riesgo_quiebre = "medio"

    exceso_actual = max(cobertura_actual - demanda_total, 0)
    exceso_planificado = max(cobertura_planificada - demanda_total, 0)
    riesgo_sobrante = "bajo"
    if cobertura_planificada > 0 and demanda_total <= 0:
        riesgo_sobrante = "alto"
    elif exceso_planificado >= max(8, math.ceil(demanda_total * 0.22)) or tasa_sobrante >= 18:
        riesgo_sobrante = "alto"
    elif exceso_planificado >= max(4, math.ceil(demanda_total * 0.12)) or exceso_actual >= max(4, math.ceil(demanda_total * 0.12)) or tasa_sobrante >= 10:
        riesgo_sobrante = "medio"

    urgencia = "baja"
    if riesgo_quiebre == "alto":
        urgencia = "alta"
    elif riesgo_quiebre == "medio" or produccion_recomendada > 0 or encargos_altos or confianza_baja:
        urgencia = "media"
    if produccion_recomendada == 0 and cobertura_actual >= demanda_total and riesgo_sobrante == "bajo":
        urgencia = "baja"

    nivel = "bien"
    estado = "estable"
    titulo = "Cobertura controlada"
    badge = "Operacion estable"

    if riesgo_quiebre == "alto":
        nivel = "riesgo"
        estado = "quiebre"
        titulo = "Produce temprano"
        badge = "Riesgo de quiebre"
    elif riesgo_sobrante == "alto":
        nivel = "alerta"
        estado = "sobrante"
        titulo = "Conviene ser prudente"
        badge = "Riesgo de sobrante"
    elif produccion_recomendada == 0 and cobertura_actual >= demanda_total:
        nivel = "bien"
        estado = "sin_producir"
        titulo = "No hace falta producir mas"
        badge = "Cobertura suficiente"
    elif confianza_baja:
        nivel = "alerta"
        estado = "revision_manual"
        titulo = "Revisa antes de un lote grande"
        badge = "Confianza baja"
    elif encargos_altos:
        nivel = "alerta"
        estado = "encargos_presion"
        titulo = "Los encargos mandan hoy"
        badge = "Encargos altos"

    lineas: list[str] = []

    linea_base = f"Demanda estimada {demanda_estimada} u"
    if demanda_comprometida > 0:
        linea_base += f" + {demanda_comprometida} u comprometidas por encargos"
    linea_base += f". Produccion recomendada {produccion_recomendada} u."
    lineas.append(linea_base)

    if stock_disponible or pendiente_disponible:
        partes_cobertura = []
        if stock_disponible:
            partes_cobertura.append(f"stock actual {stock_actual} u")
        if pendiente_disponible and produccion_pendiente > 0:
            partes_cobertura.append(f"produccion pendiente {produccion_pendiente} u")
        cobertura_label = ", ".join(partes_cobertura) if partes_cobertura else f"cobertura actual {cobertura_actual} u"
        cobertura_texto = cobertura_label[:1].upper() + cobertura_label[1:] if cobertura_label else "Cobertura actual"
        if produccion_recomendada == 0 and cobertura_actual >= demanda_total:
            lineas.append(f"{cobertura_texto} cubre la demanda de hoy sin producir adicional.")
        elif faltante_actual > 0:
            lineas.append(f"{cobertura_texto} deja un faltante de {faltante_actual} u antes del siguiente lote.")
        else:
            lineas.append(f"{cobertura_texto} deja la cobertura encaminada para hoy.")
    elif demanda_total <= 0 and produccion_recomendada == 0:
        lineas.append("Hoy no aparece una demanda operativa que justifique producir adicional.")
    elif demanda_comprometida > 0 and encargos_altos:
        lineas.append("Los encargos pesan fuerte en la jornada y conviene asegurar cobertura temprano.")

    if riesgo_quiebre == "alto":
        accion = "Prioriza este producto en la primera hornada y produce temprano."
        lineas.append("Riesgo alto de quiebre y urgencia alta. Prioriza este producto en la primera hornada.")
    elif riesgo_quiebre == "medio":
        accion = "Programa un lote temprano y revisa la salida antes del mediodia."
        lineas.append("Riesgo medio de quiebre. Revisa la salida en la manana antes de abrir otro lote.")
    elif riesgo_sobrante == "alto":
        accion = "Evita un lote grande y produce por tandas cortas."
        lineas.append("Riesgo alto de sobrante. Evita un lote grande y produce por tandas cortas.")
    elif riesgo_sobrante == "medio":
        accion = "Avanza con prudencia y valida la salida real antes de repetir lote."
        lineas.append("Riesgo medio de sobrante. Conviene avanzar con prudencia y validar salida real.")
    elif produccion_recomendada == 0 and cobertura_actual >= demanda_total:
        accion = "No produzcas adicional por ahora; monitorea la venta real."
        lineas.append("No conviene producir adicional por ahora; monitorea la venta real y reacciona solo si acelera.")
    elif encargos_altos:
        accion = "Programa el lote temprano para no presionar la operacion."
        lineas.append("Urgencia media por encargos. Programa el lote temprano para no presionar la operacion.")
    else:
        accion = "Produce el lote sugerido y monitorea la salida durante la manana."
        lineas.append("Riesgo bajo de quiebre y riesgo controlado de sobrante con el plan actual.")

    if confianza_baja:
        lineas.append("Hay variabilidad reciente o poco historial confiable. Revisa manualmente antes de ampliar el lote.")

    lineas = lineas[:4]

    apoyo = f"Quiebre {riesgo_quiebre} · Sobrante {riesgo_sobrante} · Urgencia {urgencia}"
    mensaje = " ".join(lineas)
    if margen_planificado > 0 and riesgo_sobrante == "bajo" and nivel == "bien":
        apoyo += f" · Margen final {margen_planificado} u"

    return {
        "titulo": titulo,
        "mensaje": mensaje,
        "lineas": lineas,
        "nivel": nivel,
        "estado": estado,
        "badge": badge,
        "apoyo": apoyo,
        "riesgo_quiebre": riesgo_quiebre,
        "riesgo_sobrante": riesgo_sobrante,
        "urgencia": urgencia,
        "accion": accion,
    }


# ── Modelos internos ──────────────────────────────────────────────────────────

def _modelo_inicial(producto: str, dias: int, buffer: float, fecha: str) -> ResultadoPronostico:
    """Pocos datos (<7 días). Estimación base con ajuste por tendencia."""
    registros = obtener_historial_pronostico(producto, dias=30)
    registros_limpios = _filtrar_outliers(registros)

    if registros_limpios:
        promedio = sum(r["vendido"] for r in registros_limpios) / len(registros_limpios)
        origen = "historial_reciente"
    else:
        promedio = _valor_base_inicial(producto)
        origen = "valor_base"

    tendencia = analizar_tendencia(registros_limpios)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)
    venta_estimada = _redondear_produccion(promedio * factor_tendencia)

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=venta_estimada,
        venta_estimada=venta_estimada,
        modelo_usado="estimacion_inicial",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_calidad=_calcular_calidad(registros_limpios),
        estado="alerta",
        mensaje=f"Apenas {dias} días de datos. Estimación inicial con ajuste por tendencia.",
        confianza="poca",
        detalles={
            "origen_base": origen,
            "tendencia": tendencia,
            "factor_tendencia": factor_tendencia,
        },
    )


def _modelo_promedio_semanal(producto: str, dias: int, buffer: float, fecha: str) -> ResultadoPronostico:
    """7-29 días de datos. Promedio reciente ajustado por sobrante y tendencia."""
    registros_recientes = obtener_historial_pronostico(producto, dias=DIAS_PROMEDIO_MOVIL)
    registros_limpios = _filtrar_outliers(registros_recientes)

    if not registros_limpios:
        return _modelo_inicial(producto, dias, buffer, fecha)

    promedio = sum(r["vendido"] for r in registros_limpios) / len(registros_limpios)

    total_producido = sum(r["producido"] for r in registros_limpios)
    total_sobrante = sum(max(r["sobrante"], 0) for r in registros_limpios)
    tasa_sobrante = (total_sobrante / total_producido) if total_producido > 0 else 0

    if tasa_sobrante > UMBRAL_SOBREPRODUCCION:
        factor_ajuste = 0.90
        estado = "problema"
        msg_base = "Está sobrando mucho pan. Se redujo la sugerencia."
    elif tasa_sobrante > 0.08:
        factor_ajuste = 0.95
        estado = "alerta"
        msg_base = "Hay sobrante moderado. Se aplicó ajuste ligero."
    else:
        factor_ajuste = 1.0
        estado = "bien"
        msg_base = "Producción equilibrada según la última semana."

    tendencia = analizar_tendencia(registros_limpios)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)
    venta_estimada = _redondear_produccion(promedio * factor_ajuste * factor_tendencia)

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=venta_estimada,
        venta_estimada=venta_estimada,
        modelo_usado="promedio_semanal",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_calidad=_calcular_calidad(registros_limpios),
        estado=estado,
        mensaje=f"{msg_base} Tendencia: {tendencia}.",
        confianza="media",
        detalles={
            "tasa_sobrante": round(tasa_sobrante * 100, 1),
            "factor_ajuste": factor_ajuste,
            "tendencia": tendencia,
            "factor_tendencia": factor_tendencia,
            "metricas": _metricas_operativas(registros_limpios),
        },
    )


def _modelo_mixto(
    producto: str,
    dias: int,
    fecha_objetivo: str,
    buffer: float,
) -> ResultadoPronostico:
    """
    Modelo maduro (≥30 días): blend 50/30/20.
    - 50%: Histórico del mismo día de semana
    - 30%: Últimos 7 días
    - 20%: Componente tendencia (promedio reciente × factor_tendencia)
    Los tres pesos suman 1.0 correctamente.
    """
    resumen_dia = obtener_resumen_pronostico_por_dia_semana(producto, dias=90)
    registros_90 = obtener_historial_pronostico(producto, dias=90)
    registros_recientes = obtener_historial_pronostico(producto, dias=DIAS_PROMEDIO_MOVIL)

    def _conteo_resumen(dia_nombre: str) -> int:
        base = resumen_dia.get(dia_nombre, {})
        return int(base.get("registros", base.get("muestras", 0)) or 0)

    dia_obj = datetime.strptime(fecha_objetivo, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miercoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sabado", "Sunday": "Domingo",
    }
    dia_objetivo_es = dias_es.get(dia_obj, dia_obj)
    tipo_dia_obj = TIPO_DIA.get(dia_objetivo_es, "laboral")

    # ── Componente 1: Histórico del mismo día (50%) ───────────────────────────
    if dia_objetivo_es in resumen_dia and _conteo_resumen(dia_objetivo_es) >= 2:
        promedio_dia = resumen_dia[dia_objetivo_es]["promedio"]
    else:
        dias_mismo_tipo = [
            resumen_dia[d]["promedio"]
            for d in resumen_dia
            if TIPO_DIA.get(d) == tipo_dia_obj and _conteo_resumen(d) >= 1
        ]
        if dias_mismo_tipo:
            promedio_dia = sum(dias_mismo_tipo) / len(dias_mismo_tipo)
        else:
            promedios = [v["promedio"] for v in resumen_dia.values()]
            promedio_dia = sum(promedios) / len(promedios) if promedios else _valor_base_inicial(producto)

    # ── Componente 2: Últimos 7 días (30%) ───────────────────────────────────
    registros_limpios_recientes = _filtrar_outliers(registros_recientes)
    if registros_limpios_recientes:
        promedio_reciente = sum(r["vendido"] for r in registros_limpios_recientes) / len(registros_limpios_recientes)
    else:
        promedio_reciente = promedio_dia

    # ── Componente 3: Tendencia (20%) — Bug 5 fix: componente aditivo real ───
    # La tendencia proyecta el promedio reciente con el factor de tendencia.
    # Esto representa "hacia dónde va la demanda", no un simple multiplicador.
    registros_14 = obtener_historial_pronostico(producto, dias=14)
    tendencia = analizar_tendencia(registros_14)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)
    promedio_tendencia = promedio_reciente * factor_tendencia  # señal de tendencia

    # ── Blend final (pesos suman 1.0) ─────────────────────────────────────────
    promedio_combinado = (
        PESO_DIA_SEMANA * promedio_dia +
        PESO_RECIENTE_7  * promedio_reciente +
        PESO_TENDENCIA   * promedio_tendencia
    )
    venta_estimada = _redondear_produccion(promedio_combinado)

    # ── Calidad y estado ─────────────────────────────────────────────────────
    registros_90_limpios = _filtrar_outliers(registros_90)
    calidad = _calcular_calidad(registros_90_limpios)
    if calidad >= 4.0:
        estado = "bien"
        msg = "Pronóstico confiable: combina patrón semanal y recencia."
    elif calidad >= 2.5:
        estado = "alerta"
        msg = "Pronóstico útil, pero conviene vigilar variaciones recientes."
    else:
        estado = "problema"
        msg = "Alta variación detectada. Revisa datos y realiza ajustes manuales."

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=venta_estimada,
        venta_estimada=venta_estimada,
        modelo_usado="mixto_dia_semana_recencia",
        promedio_ventas=round(promedio_combinado, 1),
        dias_historial=dias,
        nivel_calidad=calidad,
        estado=estado,
        mensaje=f"{msg} Tendencia: {tendencia}. Tipo de día: {tipo_dia_obj}.",
        confianza="buena",
        detalles={
            "promedio_dia_semana": round(promedio_dia, 1),
            "promedio_reciente_7d": round(promedio_reciente, 1),
            "promedio_tendencia": round(promedio_tendencia, 1),
            "pesos": {"dia_semana": PESO_DIA_SEMANA, "reciente_7d": PESO_RECIENTE_7, "tendencia": PESO_TENDENCIA},
            "dia_objetivo": dia_objetivo_es,
            "tipo_dia": tipo_dia_obj,
            "tendencia": tendencia,
            "factor_tendencia": factor_tendencia,
            "metricas": _metricas_operativas(registros_90_limpios),
        },
    )


# ── Utilidades de calidad y análisis ─────────────────────────────────────────

def _filtrar_outliers(registros: list[dict]) -> list[dict]:
    """Excluye días atípicos cuyas ventas se desvían más de N desviaciones estándar."""
    if len(registros) < 5:
        return registros

    ventas = [r["vendido"] for r in registros if r.get("producido", 0) > 0]
    if not ventas:
        return registros

    media = sum(ventas) / len(ventas)
    varianza = sum((v - media) ** 2 for v in ventas) / len(ventas)
    desv = varianza ** 0.5

    if desv < 1:
        return registros

    umbral_max = media + OUTLIER_DESVIACIONES * desv
    umbral_min = max(0, media - OUTLIER_DESVIACIONES * desv)
    filtrados = [r for r in registros if umbral_min <= r["vendido"] <= umbral_max]
    return filtrados if len(filtrados) >= 3 else registros


def _metricas_operativas(registros: list[dict]) -> dict:
    """Métricas operativas entendibles para el negocio."""
    if not registros:
        return {"mae": 0.0, "tasa_sobrante": 0.0, "tasa_quiebre": 0.0, "tasa_tope": 0.0}

    difs_abs = [abs(r.get("producido", 0) - r.get("vendido", 0)) for r in registros]
    mae = sum(difs_abs) / len(difs_abs) if difs_abs else 0.0

    total_producido = sum(max(r.get("producido", 0), 0) for r in registros)
    total_sobrante = sum(max(r.get("sobrante", 0), 0) for r in registros)
    tasa_sobrante = (total_sobrante / total_producido * 100) if total_producido > 0 else 0.0

    dias_quiebre = dias_tope = 0
    for r in registros:
        producido = r.get("producido", 0)
        vendido = r.get("vendido", 0)
        if producido <= 0:
            continue
        if vendido > producido:
            dias_quiebre += 1
        if vendido == producido:
            dias_tope += 1

    total = len(registros)
    return {
        "mae": round(mae, 1),
        "tasa_sobrante": round(tasa_sobrante, 1),
        "tasa_quiebre": round(dias_quiebre / total * 100, 1) if total else 0.0,
        "tasa_tope": round(dias_tope / total * 100, 1) if total else 0.0,
    }


def _calcular_calidad(registros: list[dict]) -> float:
    """
    Bug 6 fix: nivel de calidad basado en CV del patrón de ventas (estabilidad),
    no en sobrante/producido que es una decisión de producción externa al modelo.

    CV bajo = patrón estable = mayor confianza en el pronóstico.
    Bonus por volumen de datos.
    """
    if len(registros) < 3:
        return 0.0

    ventas = [r.get("vendido", 0) for r in registros if r.get("vendido", 0) >= 0]
    if not ventas:
        return 0.0

    n = len(ventas)
    promedio = sum(ventas) / n
    if promedio <= 0:
        return 1.0

    varianza = sum((v - promedio) ** 2 for v in ventas) / n
    desv = varianza ** 0.5
    cv = desv / promedio  # coeficiente de variación: 0 = perfecto, >1 = muy inestable

    # CV < 0.15 → patrón casi constante (6.0)
    # CV 0.15-0.35 → patrón estable (4.0-5.5)
    # CV 0.35-0.65 → variación moderada (2.0-4.0)
    # CV > 0.65 → alta variación (< 2.0)
    score = 6.0 - min(cv * 8.0, 5.5)

    # Bonus por volumen de datos (hasta +0.5 con 30+ días)
    bonus_datos = min(n / 30.0, 1.0) * 0.5

    return round(max(0.5, min(6.0, score + bonus_datos)), 2)


def analizar_tendencia(registros: list[dict]) -> str:
    """Detecta tendencia usando primera mitad vs segunda mitad cronológica."""
    if len(registros) < 5:
        return "sin datos"

    ventas_cronologicas = [r["vendido"] for r in reversed(registros)]
    mitad = len(ventas_cronologicas) // 2
    tramo_antiguo = ventas_cronologicas[:mitad]
    tramo_reciente = ventas_cronologicas[mitad:]

    if not tramo_antiguo or not tramo_reciente:
        return "sin datos"

    prom_antiguo = sum(tramo_antiguo) / len(tramo_antiguo)
    prom_reciente = sum(tramo_reciente) / len(tramo_reciente)

    diferencia_pct = ((prom_reciente - prom_antiguo) / prom_antiguo) if prom_antiguo else 0

    if diferencia_pct > 0.06:
        return "subiendo"
    if diferencia_pct < -0.06:
        return "bajando"
    return "estable"


def calcular_eficiencia(registros: list[dict]) -> dict:
    """Métricas de eficiencia para el dashboard."""
    if not registros:
        return {}

    total_producido = sum(r["producido"] for r in registros)
    total_vendido = sum(r["vendido"] for r in registros)
    total_sobrante = sum(r["sobrante"] for r in registros)
    aprovechamiento = (total_vendido / total_producido * 100) if total_producido > 0 else 0

    return {
        "total_producido": total_producido,
        "total_vendido": total_vendido,
        "total_sobrante": total_sobrante,
        "tasa_aprovechamiento": round(aprovechamiento, 1),
    }


def _redondear_produccion(valor: float) -> int:
    """Bug 2 fix: permite sugerir 0 cuando la demanda es realmente 0 o negativa."""
    if valor <= 0:
        return 0
    return int(math.ceil(valor))


def _valor_base_inicial(producto: str) -> float:
    """Valor base contextual para productos sin historial."""
    nombre = producto.strip().lower()
    if nombre in VALORES_BASE_PRODUCTO:
        return float(VALORES_BASE_PRODUCTO[nombre])
    registros_producto = obtener_historial_pronostico(producto, dias=30)
    if registros_producto:
        return round(sum(r["vendido"] for r in registros_producto) / len(registros_producto), 1)
    return 30.0


# ── Backtesting rolling-window ────────────────────────────────────────────────

def _forecast_sobre_registros(registros_previos: list[dict], fecha: str) -> int:
    """
    Calcula un pronóstico usando solo los registros dados (sin llamar a la BD).
    Versión interna para backtesting.
    """
    if not registros_previos:
        return 0

    n = len(registros_previos)
    dia_es = _nombre_dia_semana_es(fecha)

    if n < 7:
        ventas = [r["vendido"] for r in registros_previos]
        promedio = sum(ventas) / len(ventas) if ventas else 0
        tendencia = analizar_tendencia(registros_previos)
        factor = AJUSTE_TENDENCIA.get(tendencia, 1.0)
        return _redondear_produccion(promedio * factor)

    # Promedio del mismo día de semana
    del_mismo_dia = [r["vendido"] for r in registros_previos if r.get("dia_semana") == dia_es]
    tipo = TIPO_DIA.get(dia_es, "laboral")
    del_mismo_tipo = [r["vendido"] for r in registros_previos
                      if TIPO_DIA.get(r.get("dia_semana", ""), "laboral") == tipo]

    if del_mismo_dia:
        promedio_dia = sum(del_mismo_dia) / len(del_mismo_dia)
    elif del_mismo_tipo:
        promedio_dia = sum(del_mismo_tipo) / len(del_mismo_tipo)
    else:
        todos = [r["vendido"] for r in registros_previos]
        promedio_dia = sum(todos) / len(todos) if todos else 0

    # Promedio últimos 7 registros
    recientes = registros_previos[-7:]
    promedio_reciente = sum(r["vendido"] for r in recientes) / len(recientes) if recientes else promedio_dia

    # Tendencia
    tendencia = analizar_tendencia(registros_previos[-14:] if len(registros_previos) >= 14 else registros_previos)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)
    promedio_tendencia = promedio_reciente * factor_tendencia

    combinado = (
        PESO_DIA_SEMANA * promedio_dia +
        PESO_RECIENTE_7  * promedio_reciente +
        PESO_TENDENCIA   * promedio_tendencia
    )
    return _redondear_produccion(combinado)


def calcular_backtesting(
    producto: str,
    ventana_entrenamiento: int = 21,
    max_evaluaciones: int = 20,
) -> dict:
    """
    Rolling-window backtesting: para cada día evaluado, usa solo historia previa
    (ventana_entrenamiento días) para calcular el pronóstico, luego compara con
    la venta real.

    Devuelve MAPE, MAE y la serie de evaluaciones por día.
    """
    historial = obtener_historial_pronostico(producto, dias=90)
    if not historial:
        return {"ok": False, "error": "Sin historial disponible", "evaluaciones": []}

    # Ordenar cronológicamente
    cronologico = sorted(historial, key=lambda r: r["fecha"])
    if len(cronologico) < ventana_entrenamiento + 3:
        return {
            "ok": False,
            "error": f"Se necesitan al menos {ventana_entrenamiento + 3} días para backtesting.",
            "evaluaciones": [],
        }

    # Ventana deslizante sobre los últimos días
    candidatos = cronologico[ventana_entrenamiento:]  # solo desde donde hay suficiente historia
    a_evaluar = candidatos[-max_evaluaciones:] if len(candidatos) > max_evaluaciones else candidatos

    evaluaciones = []
    for punto in a_evaluar:
        fecha = punto["fecha"]
        vendido_real = int(punto.get("vendido", 0) or 0)

        # Historia estrictamente anterior a esta fecha
        historia_previa = [r for r in cronologico if r["fecha"] < fecha]
        historia_recortada = historia_previa[-ventana_entrenamiento:]
        if len(historia_recortada) < 5:
            continue

        forecast = _forecast_sobre_registros(historia_recortada, fecha)
        error = forecast - vendido_real
        error_abs = abs(error)
        error_pct = round(error_abs / vendido_real * 100, 1) if vendido_real > 0 else None

        evaluaciones.append({
            "fecha": fecha,
            "dia_semana": punto.get("dia_semana", _nombre_dia_semana_es(fecha)),
            "pronostico": forecast,
            "vendido_real": vendido_real,
            "error": error,
            "error_abs": error_abs,
            "error_pct": error_pct,
            "dentro_10pct": error_pct is not None and error_pct <= 10.0,
            "dentro_20pct": error_pct is not None and error_pct <= 20.0,
        })

    if not evaluaciones:
        return {"ok": False, "error": "No se pudo evaluar ningún día.", "evaluaciones": []}

    errores_validos = [e["error_pct"] for e in evaluaciones if e["error_pct"] is not None]
    mape = round(sum(errores_validos) / len(errores_validos), 1) if errores_validos else 0.0
    mae = round(sum(e["error_abs"] for e in evaluaciones) / len(evaluaciones), 1)
    hits_10 = sum(1 for e in evaluaciones if e.get("dentro_10pct"))
    hits_20 = sum(1 for e in evaluaciones if e.get("dentro_20pct"))
    n = len(evaluaciones)

    return {
        "ok": True,
        "producto": producto,
        "n": n,
        "ventana_entrenamiento": ventana_entrenamiento,
        "mape": mape,
        "mae": mae,
        "hit_rate_10pct": round(hits_10 / n * 100, 1) if n else 0.0,
        "hit_rate_20pct": round(hits_20 / n * 100, 1) if n else 0.0,
        "evaluaciones": evaluaciones,
    }


# ── Encargos overlay ──────────────────────────────────────────────────────────

def obtener_encargos_confirmados_para_fecha(producto: str, fecha: str) -> int:
    """
    Devuelve las unidades ya comprometidas via encargos (con_anticipo/programado/listo)
    para el producto en la fecha dada.
    Importación lazy para evitar ciclos y carga solo cuando se necesita.
    """
    try:
        return obtener_demanda_comprometida_encargos(
            producto=producto,
            fecha_entrega=fecha,
            estados=("confirmado", "programado"),
        )
    except Exception:
        return 0
