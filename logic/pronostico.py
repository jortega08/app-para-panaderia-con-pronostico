"""
pronostico.py
-------------
Motor de pronostico adaptativo para produccion de pan.

Niveles de modelo (seleccion automatica segun datos disponibles):
  - Nivel 1: Estimacion inicial (0-6 dias de historial)
  - Nivel 2: Promedio de la ultima semana (7-29 dias)
  - Nivel 3: Promedio por dia de la semana (30+ dias)
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from data.database import (
    obtener_registros,
    obtener_resumen_por_dia_semana,
    contar_registros,
)


# ──────────────────────────────────────────────
# Configuracion del modelo
# ──────────────────────────────────────────────
BUFFER_SEGURIDAD = 0.10       # 10% extra por seguridad
UMBRAL_SOBREPRODUCCION = 0.15 # 15% sobrante = mucho
DIAS_PROMEDIO_MOVIL = 7
DIAS_NIVEL_ALTO = 30


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


def calcular_pronostico(producto: str,
                        fecha_objetivo: Optional[str] = None,
                        buffer: float = BUFFER_SEGURIDAD
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
    elif dias_disponibles < DIAS_NIVEL_ALTO:
        return _modelo_promedio_semanal(producto, dias_disponibles, buffer)
    else:
        return _modelo_por_dia(producto, dias_disponibles,
                               fecha_objetivo, buffer)


# ──────────────────────────────────────────────
# Modelos internos
# ──────────────────────────────────────────────

def _modelo_inicial(producto: str, dias: int,
                    buffer: float) -> ResultadoPronostico:
    """
    Pocos datos. Sugiere cantidad conservadora.
    """
    registros = obtener_registros(producto, dias=30)
    if registros:
        promedio = sum(r["vendido"] for r in registros) / len(registros)
        sugerida = int(promedio * (1 + buffer))
    else:
        promedio = 50
        sugerida = 50

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="estimacion_inicial",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_calidad=_calcular_calidad(registros),
        estado="alerta",
        mensaje=f"Apenas {dias} dias de datos. Este numero es una estimacion.",
        confianza="poca",
        detalles={"registros": registros}
    )


def _modelo_promedio_semanal(producto: str, dias: int,
                              buffer: float) -> ResultadoPronostico:
    """
    Promedio de los ultimos 7 dias con ajuste automatico.
    """
    registros = obtener_registros(producto, dias=DIAS_PROMEDIO_MOVIL)
    ventas = [r["vendido"] for r in registros]

    if not ventas:
        return _modelo_inicial(producto, dias, buffer)

    promedio = sum(ventas) / len(ventas)
    sobrantes = [r["sobrante"] for r in registros]
    total_producido = sum(r["producido"] for r in registros)
    tasa_sobrante = (sum(sobrantes) / total_producido
                     if total_producido > 0 else 0)

    # Ajuste automatico si sobra mucho
    factor_ajuste = 1.0
    if tasa_sobrante > UMBRAL_SOBREPRODUCCION:
        factor_ajuste = 0.90
        estado = "problema"
        msg = "Esta sobrando mucho pan. Se redujo la cantidad sugerida."
    elif tasa_sobrante > 0.08:
        factor_ajuste = 0.95
        estado = "alerta"
        msg = "Sobra un poco de pan. Se ajusto ligeramente."
    else:
        estado = "bien"
        msg = "La produccion esta bien equilibrada."

    sugerida = int(promedio * (1 + buffer) * factor_ajuste)
    calidad = _calcular_calidad(registros)

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="promedio_semanal",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_calidad=calidad,
        estado=estado,
        mensaje=msg,
        confianza="media",
        detalles={
            "tasa_sobrante": round(tasa_sobrante * 100, 1),
            "factor_ajuste": factor_ajuste,
            "registros": registros,
        }
    )


def _modelo_por_dia(producto: str, dias: int,
                     fecha_objetivo: str,
                     buffer: float) -> ResultadoPronostico:
    """
    Promedio historico por dia de la semana.
    Mas preciso porque captura patrones (ej: los sabados se vende mas).
    """
    resumen = obtener_resumen_por_dia_semana(producto)
    registros = obtener_registros(producto, dias=90)

    dia_obj = datetime.strptime(fecha_objetivo, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miercoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sabado", "Sunday": "Domingo"
    }
    dia_objetivo_es = dias_es.get(dia_obj, dia_obj)

    if dia_objetivo_es in resumen:
        promedio = resumen[dia_objetivo_es]["promedio"]
    else:
        todos = [v["promedio"] for v in resumen.values()]
        promedio = sum(todos) / len(todos) if todos else 50

    sugerida = int(promedio * (1 + buffer))
    calidad = _calcular_calidad(registros)

    if calidad >= 3.0:
        estado = "bien"
        msg = "Pronostico confiable. La produccion ha sido estable."
    elif calidad >= 2.0:
        estado = "alerta"
        msg = "El pronostico es aceptable, pero hay algo de variacion."
    else:
        estado = "problema"
        msg = "Hay mucha variacion en las ventas. Revisa los datos."

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="por_dia_semana",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_calidad=calidad,
        estado=estado,
        mensaje=msg,
        confianza="buena",
        detalles={
            "resumen_semanal": resumen,
            "dia_objetivo": dia_objetivo_es,
            "registros": registros,
        }
    )


# ──────────────────────────────────────────────
# Utilidades de calidad
# ──────────────────────────────────────────────

def _calcular_calidad(registros: list[dict]) -> float:
    """
    Calcula un indice de calidad (0-6) basado en que tan bien
    se ajusta la produccion a la demanda.
    Mas alto = mas estable = mejor.
    """
    if len(registros) < 3:
        return 0.0

    defectos = 0
    for r in registros:
        prod = r.get("producido", 0)
        if prod == 0:
            continue
        sobrante_pct = r.get("sobrante", 0) / prod
        if sobrante_pct > UMBRAL_SOBREPRODUCCION or r.get("vendido") == prod:
            defectos += 1

    total = len(registros)
    dpmo = (defectos / total) * 1_000_000

    tabla = [
        (3.4,      6.0),
        (233,      5.0),
        (6_210,    4.0),
        (66_807,   3.0),
        (308_538,  2.0),
        (690_000,  1.0),
    ]
    for limite, nivel in tabla:
        if dpmo <= limite:
            return nivel
    return 0.5


def analizar_tendencia(registros: list[dict]) -> str:
    """
    Analiza si las ventas van subiendo, bajando o estan estables.
    """
    if len(registros) < 5:
        return "sin datos"

    mitad = len(registros) // 2
    primera = [r["vendido"] for r in registros[mitad:]]
    segunda = [r["vendido"] for r in registros[:mitad]]

    prom_primera = sum(primera) / len(primera)
    prom_segunda = sum(segunda) / len(segunda)

    diferencia_pct = (prom_segunda - prom_primera) / prom_primera if prom_primera else 0

    if diferencia_pct > 0.05:
        return "subiendo"
    elif diferencia_pct < -0.05:
        return "bajando"
    else:
        return "estable"


def calcular_eficiencia(registros: list[dict]) -> dict:
    """Metricas de eficiencia para el dashboard."""
    if not registros:
        return {}

    total_producido = sum(r["producido"] for r in registros)
    total_vendido   = sum(r["vendido"]   for r in registros)
    total_sobrante  = sum(r["sobrante"]  for r in registros)

    aprovechamiento = (total_vendido / total_producido * 100
                       if total_producido > 0 else 0)

    return {
        "total_producido":      total_producido,
        "total_vendido":        total_vendido,
        "total_sobrante":       total_sobrante,
        "tasa_aprovechamiento": round(aprovechamiento, 1),
    }
