from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Optional

from data.database import (
    obtener_registros,
    obtener_resumen_por_dia_semana,
    contar_registros,
    obtener_factor_dia_especial,
    obtener_ajuste_pronostico,
)


# ── Configuración del modelo ──────────────────────────────────────────────────
BUFFER_SEGURIDAD = 0.10
UMBRAL_SOBREPRODUCCION = 0.15
DIAS_PROMEDIO_MOVIL = 7
DIAS_NIVEL_ALTO = 30

# Pesos del modelo mixto (Fase 2: mejorado)
PESO_DIA_SEMANA = 0.50       # Histórico del mismo día de semana
PESO_RECIENTE_7 = 0.30       # Últimos 7 días
PESO_TENDENCIA = 0.20        # Ajuste de tendencia reciente (últimas 2 semanas)

AJUSTE_TENDENCIA = {
    "subiendo": 1.08,
    "bajando": 0.95,
    "estable": 1.00,
    "sin datos": 1.00,
}

# Agrupación de días por tipo operativo (panadería tiene patrones distintos)
TIPO_DIA = {
    "Lunes":    "laboral",    # L-J: demanda similar, moderada
    "Martes":   "laboral",
    "Miercoles":"laboral",
    "Jueves":   "laboral",
    "Viernes":  "viernes",    # Viernes: repunte antes del fin de semana
    "Sabado":   "fin_semana", # S: alto
    "Domingo":  "fin_semana", # D: alto/bajo según zona
}

VALORES_BASE_PRODUCTO = {
    "pan frances": 80,
    "pan dulce": 35,
    "croissant": 30,
    "integral": 25,
}

# Umbral de outlier: días cuya venta supera N desviaciones estándar se excluyen
OUTLIER_DESVIACIONES = 2.5


@dataclass
class ResultadoPronostico:
    """Resultado del pronóstico de producción."""

    producto: str
    produccion_sugerida: int       # Cuántos hornear
    venta_estimada: int            # Cuántos se esperan vender
    modelo_usado: str
    promedio_ventas: float
    dias_historial: int
    nivel_calidad: float           # 0-6
    estado: str                    # "bien" | "alerta" | "problema"
    mensaje: str
    confianza: str                 # "poca" | "media" | "buena"
    detalles: dict


# ── Función principal ─────────────────────────────────────────────────────────

def calcular_pronostico(
    producto: str,
    fecha_objetivo: Optional[str] = None,
    buffer: float = BUFFER_SEGURIDAD,
    stock_actual: int = 0,
    produccion_pendiente: int = 0,
    pedidos_registrados: int = 0,
) -> ResultadoPronostico:
    """
    Calcula cuántos panes hornear de un producto.

    Selecciona el modelo según datos disponibles.
    Con datos suficientes usa: 50% mismo día + 30% últimos 7 días + 20% tendencia.
    Ajusta por stock actual, producción pendiente y días especiales.

    Args:
        producto: nombre del producto
        fecha_objetivo: fecha para la que se pronostica (default: hoy)
        buffer: margen de seguridad adicional (default 10%)
        stock_actual: unidades disponibles en inventario
        produccion_pendiente: unidades ya en producción o pendientes de hornear
        pedidos_registrados: pedidos ya confirmados para ese día
    """
    if fecha_objetivo is None:
        fecha_objetivo = datetime.now().strftime("%Y-%m-%d")

    dias_disponibles = contar_registros(producto)

    # ── Verificar ajuste manual del panadero ─────────────────────────────────
    ajuste_manual = obtener_ajuste_pronostico(fecha_objetivo, producto)

    if dias_disponibles < 7:
        resultado = _modelo_inicial(producto, dias_disponibles, buffer, fecha_objetivo)
    elif dias_disponibles < DIAS_NIVEL_ALTO:
        resultado = _modelo_promedio_semanal(producto, dias_disponibles, buffer, fecha_objetivo)
    else:
        resultado = _modelo_mixto(producto, dias_disponibles, fecha_objetivo, buffer)

    # ── Ajuste por días especiales (festivos, eventos) ───────────────────────
    factor_especial = obtener_factor_dia_especial(fecha_objetivo)
    if factor_especial != 1.0:
        venta_ajustada = resultado.venta_estimada * factor_especial
        resultado.venta_estimada = _redondear_produccion(venta_ajustada)
        resultado.detalles["factor_dia_especial"] = round(factor_especial, 2)
        resultado.mensaje += f" Factor día especial: {factor_especial:.0%}."

    # ── Fórmula stock-aware: cuánto hornear = venta - stock - pendiente ──────
    hornear = max(resultado.venta_estimada - stock_actual - produccion_pendiente + pedidos_registrados, 0)
    if stock_actual > 0 or produccion_pendiente > 0:
        resultado.produccion_sugerida = _redondear_produccion(hornear * (1 + buffer))
        resultado.detalles["stock_actual"] = stock_actual
        resultado.detalles["produccion_pendiente"] = produccion_pendiente
        resultado.detalles["pedidos_registrados"] = pedidos_registrados
        resultado.detalles["hornear_calculado"] = hornear
    else:
        resultado.produccion_sugerida = _redondear_produccion(resultado.venta_estimada * (1 + buffer))

    # ── Si hay ajuste manual, reflejarlo en los detalles ─────────────────────
    if ajuste_manual:
        resultado.detalles["ajuste_manual"] = ajuste_manual
        resultado.detalles["ajuste_motivo"] = ajuste_manual.get("motivo", "")

    return resultado


# ── Modelos internos ──────────────────────────────────────────────────────────

def _modelo_inicial(producto: str, dias: int, buffer: float, fecha: str) -> ResultadoPronostico:
    """Pocos datos (<7 días). Estimación base con ajuste por tendencia."""
    registros = obtener_registros(producto, dias=30)
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
    registros_recientes = obtener_registros(producto, dias=DIAS_PROMEDIO_MOVIL)
    registros_limpios = _filtrar_outliers(registros_recientes)

    if not registros_limpios:
        return _modelo_inicial(producto, dias, buffer, fecha)

    promedio = sum(r["vendido"] for r in registros_limpios) / len(registros_limpios)

    # Ajuste por tasa de sobrante
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
    Modelo maduro (≥30 días):
    50% histórico del mismo día de semana + 30% últimos 7 días + 20% tendencia reciente.
    Separa por tipo de día (laboral/viernes/fin_semana).
    Excluye outliers.
    """
    resumen_dia = obtener_resumen_por_dia_semana(producto)
    registros_90 = obtener_registros(producto, dias=90)
    registros_recientes = obtener_registros(producto, dias=DIAS_PROMEDIO_MOVIL)

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

    # ── Base 1: Histórico del mismo día (50%) ──────────────────────────────
    if dia_objetivo_es in resumen_dia and _conteo_resumen(dia_objetivo_es) >= 2:
        promedio_dia = resumen_dia[dia_objetivo_es]["promedio"]
    else:
        # Fallback: promedio del mismo tipo de día (ej: todos los días laborales)
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

    # ── Base 2: Últimos 7 días (30%) ──────────────────────────────────────
    registros_limpios_recientes = _filtrar_outliers(registros_recientes)
    if registros_limpios_recientes:
        promedio_reciente = sum(r["vendido"] for r in registros_limpios_recientes) / len(registros_limpios_recientes)
    else:
        promedio_reciente = promedio_dia

    # ── Base 3: Tendencia de las últimas 2 semanas (20%) ─────────────────
    registros_14 = obtener_registros(producto, dias=14)
    tendencia = analizar_tendencia(registros_14)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)

    # ── Combinar ──────────────────────────────────────────────────────────
    promedio_combinado = (
        PESO_DIA_SEMANA * promedio_dia +
        PESO_RECIENTE_7 * promedio_reciente
    )
    # El 20% de tendencia se aplica como factor multiplicador
    venta_estimada = _redondear_produccion(promedio_combinado * factor_tendencia)

    # ── Calidad y estado ─────────────────────────────────────────────────
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
            "dia_objetivo": dia_objetivo_es,
            "tipo_dia": tipo_dia_obj,
            "tendencia": tendencia,
            "factor_tendencia": factor_tendencia,
            "metricas": _metricas_operativas(registros_90_limpios),
        },
    )


# ── Utilidades de calidad y análisis ─────────────────────────────────────────

def _filtrar_outliers(registros: list[dict]) -> list[dict]:
    """
    Excluye días atípicos cuyas ventas se desvían más de N desviaciones estándar.
    Esto evita que un día de cierre parcial o sobredemanda inusual distorsione el pronóstico.
    """
    if len(registros) < 5:
        return registros  # Sin suficientes datos, no filtrar

    ventas = [r["vendido"] for r in registros if r.get("producido", 0) > 0]
    if not ventas:
        return registros

    media = sum(ventas) / len(ventas)
    varianza = sum((v - media) ** 2 for v in ventas) / len(ventas)
    desv = varianza ** 0.5

    if desv < 1:
        return registros  # Muy consistente, sin outliers

    umbral_max = media + OUTLIER_DESVIACIONES * desv
    umbral_min = max(0, media - OUTLIER_DESVIACIONES * desv)

    filtrados = [r for r in registros if umbral_min <= r["vendido"] <= umbral_max]
    return filtrados if len(filtrados) >= 3 else registros


def _metricas_operativas(registros: list[dict]) -> dict:
    """Métricas simples y entendibles para el negocio."""
    if not registros:
        return {"mae": 0.0, "tasa_sobrante": 0.0, "tasa_quiebre": 0.0, "tasa_tope": 0.0}

    difs_abs = [abs(r.get("producido", 0) - r.get("vendido", 0)) for r in registros]
    mae = sum(difs_abs) / len(difs_abs) if difs_abs else 0.0

    total_producido = sum(max(r.get("producido", 0), 0) for r in registros)
    total_sobrante = sum(max(r.get("sobrante", 0), 0) for r in registros)
    tasa_sobrante = (total_sobrante / total_producido * 100) if total_producido > 0 else 0.0

    dias_quiebre = 0
    dias_tope = 0
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
    tasa_quiebre = (dias_quiebre / total * 100) if total else 0.0
    tasa_tope = (dias_tope / total * 100) if total else 0.0

    return {
        "mae": round(mae, 1),
        "tasa_sobrante": round(tasa_sobrante, 1),
        "tasa_quiebre": round(tasa_quiebre, 1),
        "tasa_tope": round(tasa_tope, 1),
    }


def _calcular_calidad(registros: list[dict]) -> float:
    """
    Índice 0-6 basado en métricas operativas.
    Usa percentiles de los propios datos en vez de constantes fijas.
    """
    if len(registros) < 3:
        return 0.0

    m = _metricas_operativas(registros)
    ventas = [r.get("vendido", 0) for r in registros]
    promedio = sum(ventas) / len(ventas) if ventas else 1

    # Normalizar MAE como % del promedio (más justo que dividir por constante fija)
    mae_pct = (m["mae"] / promedio * 100) if promedio > 0 else 0

    score = 6.0
    score -= min(mae_pct / 20.0, 2.0)           # Hasta -2 por desajuste relativo alto
    score -= min(m["tasa_sobrante"] / 20.0, 2.0) # Hasta -2 por desperdicio
    score -= min(m["tasa_quiebre"] / 10.0, 2.0)  # Hasta -2 por quiebres de stock

    return round(max(0.5, min(6.0, score)), 2)


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
    """Redondeo conservador hacia arriba para no quedarse corto."""
    if valor <= 0:
        return 1
    return int(math.ceil(valor))


def _valor_base_inicial(producto: str) -> float:
    """Valor base contextual para productos sin historial."""
    nombre = producto.strip().lower()
    if nombre in VALORES_BASE_PRODUCTO:
        return float(VALORES_BASE_PRODUCTO[nombre])

    registros_generales = obtener_registros(None, dias=30)
    if registros_generales:
        promedio_general = sum(r["vendido"] for r in registros_generales) / len(registros_generales)
        return round(promedio_general, 1)

    return 30.0
