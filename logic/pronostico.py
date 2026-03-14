from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from data.database import (
    obtener_registros,
    obtener_resumen_por_dia_semana,
    contar_registros,
)


# Configuracion del modelo
BUFFER_SEGURIDAD = 0.10
UMBRAL_SOBREPRODUCCION = 0.15
DIAS_PROMEDIO_MOVIL = 7
DIAS_NIVEL_ALTO = 30
PESO_DIA_SEMANA = 0.65
PESO_RECIENTE = 0.35

AJUSTE_TENDENCIA = {
    "subiendo": 1.08,
    "bajando": 0.95,
    "estable": 1.00,
    "sin datos": 1.00,
}

VALORES_BASE_PRODUCTO = {
    "pan frances": 80,
    "pan dulce": 35,
    "croissant": 30,
    "integral": 25,
}


@dataclass
class ResultadoPronostico:
    """Resultado del pronostico."""

    producto: str
    produccion_sugerida: int
    modelo_usado: str
    promedio_ventas: float
    dias_historial: int
    nivel_calidad: float       # 0-6, que tan estable es la produccion
    estado: str                # "bien" | "alerta" | "problema"
    mensaje: str
    confianza: str             # "poca" | "media" | "buena"
    detalles: dict


def calcular_pronostico(
    producto: str,
    fecha_objetivo: Optional[str] = None,
    buffer: float = BUFFER_SEGURIDAD,
) -> ResultadoPronostico:
    """
    Calcula cuantos panes hornear de un producto.
    Selecciona automaticamente el modelo segun datos disponibles.
    """
    if fecha_objetivo is None:
        fecha_objetivo = datetime.now().strftime("%Y-%m-%d")

    dias_disponibles = contar_registros(producto)

    if dias_disponibles < 7:
        return _modelo_inicial(producto, dias_disponibles, buffer)
    if dias_disponibles < DIAS_NIVEL_ALTO:
        return _modelo_promedio_semanal(producto, dias_disponibles, buffer)
    return _modelo_por_dia(producto, dias_disponibles, fecha_objetivo, buffer)

# Modelos internos

def _modelo_inicial(producto: str, dias: int, buffer: float) -> ResultadoPronostico:
    """Pocos datos. Sugiere cantidad base + ajuste suave por tendencia."""
    registros = obtener_registros(producto, dias=30)

    if registros:
        promedio = sum(r["vendido"] for r in registros) / len(registros)
        origen = "historial_reciente"
    else:
        promedio = _valor_base_inicial(producto)
        origen = "valor_base"

    tendencia = analizar_tendencia(registros)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)
    sugerida = _redondear_produccion(promedio * (1 + buffer) * factor_tendencia)

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="estimacion_inicial",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_calidad=_calcular_calidad(registros),
        estado="alerta",
        mensaje=(
            f"Apenas {dias} dias de datos. Se uso una estimacion inicial "
            "con ajuste por tendencia."
        ),
        confianza="poca",
        detalles={
            "origen_base": origen,
            "tendencia": tendencia,
            "factor_tendencia": factor_tendencia,
        },
    )


def _modelo_promedio_semanal(producto: str, dias: int, buffer: float) -> ResultadoPronostico:
    """Promedio reciente con ajuste por sobrantes y tendencia."""
    registros = obtener_registros(producto, dias=DIAS_PROMEDIO_MOVIL)
    ventas = [r["vendido"] for r in registros]

    if not ventas:
        return _modelo_inicial(producto, dias, buffer)

    promedio = sum(ventas) / len(ventas)
    sobrantes = [max(r["sobrante"], 0) for r in registros]
    total_producido = sum(r["producido"] for r in registros)
    tasa_sobrante = (sum(sobrantes) / total_producido) if total_producido > 0 else 0

    factor_ajuste = 1.0
    if tasa_sobrante > UMBRAL_SOBREPRODUCCION:
        factor_ajuste = 0.90
        estado = "problema"
        msg_base = "Esta sobrando mucho pan. Se redujo la sugerencia."
    elif tasa_sobrante > 0.08:
        factor_ajuste = 0.95
        estado = "alerta"
        msg_base = "Hay sobrante moderado. Se aplico ajuste ligero."
    else:
        estado = "bien"
        msg_base = "Produccion equilibrada segun la ultima semana."

    tendencia = analizar_tendencia(registros)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)

    sugerida = _redondear_produccion(
        promedio * (1 + buffer) * factor_ajuste * factor_tendencia
    )

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="promedio_semanal",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_calidad=_calcular_calidad(registros),
        estado=estado,
        mensaje=f"{msg_base} Tendencia actual: {tendencia}.",
        confianza="media",
        detalles={
            "tasa_sobrante": round(tasa_sobrante * 100, 1),
            "factor_ajuste": factor_ajuste,
            "tendencia": tendencia,
            "factor_tendencia": factor_tendencia,
            "metricas": _metricas_operativas(registros),
        },
    )


def _modelo_por_dia(
    producto: str,
    dias: int,
    fecha_objetivo: str,
    buffer: float,
) -> ResultadoPronostico:
    """Combina patron por dia de semana + recencia + tendencia."""
    resumen = obtener_resumen_por_dia_semana(producto)
    registros_90 = obtener_registros(producto, dias=90)
    registros_recientes = obtener_registros(producto, dias=DIAS_PROMEDIO_MOVIL)

    dia_obj = datetime.strptime(fecha_objetivo, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes",
        "Tuesday": "Martes",
        "Wednesday": "Miercoles",
        "Thursday": "Jueves",
        "Friday": "Viernes",
        "Saturday": "Sabado",
        "Sunday": "Domingo",
    }
    dia_objetivo_es = dias_es.get(dia_obj, dia_obj)

    if dia_objetivo_es in resumen:
        promedio_dia = resumen[dia_objetivo_es]["promedio"]
    else:
        promedios = [v["promedio"] for v in resumen.values()]
        promedio_dia = sum(promedios) / len(promedios) if promedios else _valor_base_inicial(producto)

    if registros_recientes:
        promedio_reciente = sum(r["vendido"] for r in registros_recientes) / len(registros_recientes)
    else:
        promedio_reciente = promedio_dia

    promedio_combinado = (PESO_DIA_SEMANA * promedio_dia) + (PESO_RECIENTE * promedio_reciente)

    tendencia = analizar_tendencia(registros_90)
    factor_tendencia = AJUSTE_TENDENCIA.get(tendencia, 1.0)
    sugerida = _redondear_produccion(promedio_combinado * (1 + buffer) * factor_tendencia)

    calidad = _calcular_calidad(registros_90)
    if calidad >= 4.0:
        estado = "bien"
        msg = "Pronostico confiable: combina patron semanal y recencia."
    elif calidad >= 2.5:
        estado = "alerta"
        msg = "Pronostico util, pero conviene vigilar variaciones recientes."
    else:
        estado = "problema"
        msg = "Alta variacion detectada. Revisa datos y ajustes manuales."

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="mixto_dia_semana_recencia",
        promedio_ventas=round(promedio_combinado, 1),
        dias_historial=dias,
        nivel_calidad=calidad,
        estado=estado,
        mensaje=f"{msg} Tendencia: {tendencia}.",
        confianza="buena",
        detalles={
            "promedio_dia_semana": round(promedio_dia, 1),
            "promedio_reciente": round(promedio_reciente, 1),
            "dia_objetivo": dia_objetivo_es,
            "tendencia": tendencia,
            "factor_tendencia": factor_tendencia,
            "metricas": _metricas_operativas(registros_90),
        },
    )


# Utilidades de calidad

def _metricas_operativas(registros: list[dict]) -> dict:
    """Metricas simples y entendibles para negocio."""
    if not registros:
        return {
            "mae": 0.0,
            "tasa_sobrante": 0.0,
            "tasa_quiebre": 0.0,
            "tasa_tope": 0.0,
        }

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
    Indice 0-6 basado en metricas operativas:
    - MAE (desajuste promedio entre producido y vendido)
    - tasa de sobrante
    - tasa de quiebre (solo cuando vendido > producido)
    """
    if len(registros) < 3:
        return 0.0

    m = _metricas_operativas(registros)

    score = 6.0
    score -= min(m["mae"] / 15.0, 2.0)
    score -= min(m["tasa_sobrante"] / 20.0, 2.0)
    score -= min(m["tasa_quiebre"] / 10.0, 2.0)

    score = max(0.5, min(6.0, score))
    return round(score, 2)


def analizar_tendencia(registros: list[dict]) -> str:
    """Detecta tendencia de ventas usando primera mitad vs segunda mitad cronologica."""
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
    """Metricas de eficiencia para el dashboard."""
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
    """Redondeo conservador hacia arriba para evitar quedarse corto."""
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
