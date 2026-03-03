"""
pronostico.py
-------------
Capa lógica: motor de pronóstico adaptativo.

Niveles de modelo (se seleccionan automáticamente según datos disponibles):
  - Nivel 1: Regla base fija (0–6 días de historial)
  - Nivel 2: Promedio móvil + buffer de seguridad (7–29 días)
  - Nivel 3: Promedio por día de semana + ajuste por tendencia (30+ días)

Cálculo de Nivel Sigma integrado (DMAIC - Controlar).
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
# Constantes de configuración del modelo
# ──────────────────────────────────────────────
BUFFER_SEGURIDAD_DEFAULT = 0.10   # 10% por encima del promedio
UMBRAL_SOBREPRODUCCION   = 0.15   # 15% sobrante = alerta roja
UMBRAL_DESABASTO         = 0.05   # ventas == producción en <5% del tiempo = ok
DIAS_PROMEDIO_MOVIL      = 7      # ventana de promedio móvil
DIAS_NIVEL_MEDIO         = 30     # umbral para escalar a nivel 3


@dataclass
class ResultadoPronostico:
    """Resultado estructurado del motor de pronóstico."""
    producto: str
    produccion_sugerida: int
    modelo_usado: str         # "base" | "promedio_movil" | "por_dia_semana"
    promedio_ventas: float
    dias_historial: int
    nivel_sigma: float
    estado: str               # "optimal" | "warning" | "danger"
    mensaje_estado: str
    confianza: str            # "baja" | "media" | "alta"
    detalles: dict            # datos adicionales para gráficas


def calcular_pronostico(producto: str,
                        fecha_objetivo: Optional[str] = None,
                        buffer: float = BUFFER_SEGURIDAD_DEFAULT
                        ) -> ResultadoPronostico:
    """
    Punto de entrada principal del motor de pronóstico.
    Selecciona automáticamente el modelo según datos disponibles.
    """
    if fecha_objetivo is None:
        fecha_objetivo = datetime.now().strftime("%Y-%m-%d")

    dias_disponibles = contar_registros(producto)

    if dias_disponibles < 7:
        return _modelo_base(producto, dias_disponibles, buffer)
    elif dias_disponibles < DIAS_NIVEL_MEDIO:
        return _modelo_promedio_movil(producto, dias_disponibles, buffer)
    else:
        return _modelo_por_dia_semana(producto, dias_disponibles,
                                      fecha_objetivo, buffer)


# ──────────────────────────────────────────────
# Modelos internos
# ──────────────────────────────────────────────

def _modelo_base(producto: str, dias: int,
                 buffer: float) -> ResultadoPronostico:
    """
    Nivel 1: Sin datos suficientes.
    Sugiere una cantidad conservadora basada en los pocos días disponibles
    o una producción mínima por defecto.
    """
    registros = obtener_registros(producto, dias=30)
    if registros:
        promedio = sum(r["vendido"] for r in registros) / len(registros)
        sugerida = int(promedio * (1 + buffer))
    else:
        promedio = 50  # valor mínimo de arranque
        sugerida = 50

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="base",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_sigma=_calcular_sigma(registros),
        estado="warning",
        mensaje_estado=f"⚠️ Solo {dias} días de datos. Pronóstico estimado.",
        confianza="baja",
        detalles={"registros": registros}
    )


def _modelo_promedio_movil(producto: str, dias: int,
                            buffer: float) -> ResultadoPronostico:
    """
    Nivel 2: Promedio móvil de los últimos N días con buffer ajustable.
    Incluye reglas de ajuste automático por sobrante.
    """
    registros = obtener_registros(producto, dias=DIAS_PROMEDIO_MOVIL)
    ventas = [r["vendido"] for r in registros]

    if not ventas:
        return _modelo_base(producto, dias, buffer)

    promedio = sum(ventas) / len(ventas)
    sobrantes = [r["sobrante"] for r in registros]
    tasa_sobrante = (sum(sobrantes) / sum(r["producido"] for r in registros)
                     if sum(r["producido"] for r in registros) > 0 else 0)

    # Ajuste automático: reducir si hay sobreproducción sistemática
    factor_ajuste = 1.0
    if tasa_sobrante > UMBRAL_SOBREPRODUCCION:
        factor_ajuste = 0.90  # reducir 10%
        estado, msg = "danger", "🔴 Sobreproducción detectada. Reduciendo 10%."
    elif tasa_sobrante > 0.08:
        factor_ajuste = 0.95
        estado, msg = "warning", "🟡 Sobrante moderado. Ajuste leve aplicado."
    else:
        estado, msg = "optimal", "🟢 Producción en rango óptimo."

    sugerida = int(promedio * (1 + buffer) * factor_ajuste)
    sigma = _calcular_sigma(registros)

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="promedio_movil",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_sigma=sigma,
        estado=estado,
        mensaje_estado=msg,
        confianza="media",
        detalles={
            "tasa_sobrante": round(tasa_sobrante * 100, 1),
            "factor_ajuste": factor_ajuste,
            "registros": registros,
        }
    )


def _modelo_por_dia_semana(producto: str, dias: int,
                            fecha_objetivo: str,
                            buffer: float) -> ResultadoPronostico:
    """
    Nivel 3: Promedio histórico por día de la semana.
    Más preciso porque captura patrones semanales reales.
    """
    resumen = obtener_resumen_por_dia_semana(producto)
    registros = obtener_registros(producto, dias=90)

    dia_obj = datetime.strptime(fecha_objetivo, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sábado", "Sunday": "Domingo"
    }
    dia_objetivo_es = dias_es.get(dia_obj, dia_obj)

    if dia_objetivo_es in resumen:
        promedio = resumen[dia_objetivo_es]["promedio"]
    else:
        # Fallback: promedio general
        todos = [v["promedio"] for v in resumen.values()]
        promedio = sum(todos) / len(todos) if todos else 50

    sugerida = int(promedio * (1 + buffer))
    sigma = _calcular_sigma(registros)

    if sigma >= 3.0:
        estado, msg = "optimal", f"🟢 Nivel Sigma {sigma:.1f}. Proceso controlado."
    elif sigma >= 2.0:
        estado, msg = "warning", f"🟡 Nivel Sigma {sigma:.1f}. Proceso mejorable."
    else:
        estado, msg = "danger", f"🔴 Nivel Sigma {sigma:.1f}. Alta variabilidad."

    return ResultadoPronostico(
        producto=producto,
        produccion_sugerida=sugerida,
        modelo_usado="por_dia_semana",
        promedio_ventas=round(promedio, 1),
        dias_historial=dias,
        nivel_sigma=sigma,
        estado=estado,
        mensaje_estado=msg,
        confianza="alta",
        detalles={
            "resumen_semanal": resumen,
            "dia_objetivo": dia_objetivo_es,
            "registros": registros,
        }
    )


# ──────────────────────────────────────────────
# Utilidades de calidad (Lean Six Sigma)
# ──────────────────────────────────────────────

def _calcular_sigma(registros: list[dict]) -> float:
    """
    Calcula el nivel Sigma aproximado basado en la tasa de defectos.
    Defecto = día con sobrante > 15% O con ventas == producción (desabasto).
    Usa la tabla DPMO → Sigma simplificada.
    """
    if len(registros) < 3:
        return 0.0

    defectos = 0
    for r in registros:
        prod = r.get("producido", 0)
        if prod == 0:
            continue
        sobrante_pct = r.get("sobrante", 0) / prod
        # Sobreproducción o desabasto = defecto
        if sobrante_pct > UMBRAL_SOBREPRODUCCION or r.get("vendido") == prod:
            defectos += 1

    total = len(registros)
    dpmo = (defectos / total) * 1_000_000

    # Tabla DPMO → Sigma (simplificada, con desplazamiento 1.5σ)
    tabla = [
        (3.4,      6.0),
        (233,      5.0),
        (6_210,    4.0),
        (66_807,   3.0),
        (308_538,  2.0),
        (690_000,  1.0),
    ]
    for limite, sigma in tabla:
        if dpmo <= limite:
            return sigma
    return 0.5


def analizar_tendencia(registros: list[dict]) -> str:
    """
    Analiza si las ventas tienen tendencia creciente, decreciente o estable.
    Retorna: "creciente" | "decreciente" | "estable"
    """
    if len(registros) < 5:
        return "sin datos"

    # Dividir en dos mitades y comparar promedios
    mitad = len(registros) // 2
    primera = [r["vendido"] for r in registros[mitad:]]
    segunda = [r["vendido"] for r in registros[:mitad]]

    prom_primera = sum(primera) / len(primera)
    prom_segunda = sum(segunda) / len(segunda)

    diferencia_pct = (prom_segunda - prom_primera) / prom_primera if prom_primera else 0

    if diferencia_pct > 0.05:
        return "creciente"
    elif diferencia_pct < -0.05:
        return "decreciente"
    else:
        return "estable"


def calcular_eficiencia(registros: list[dict]) -> dict:
    """
    Métricas de eficiencia del proceso para el dashboard.
    """
    if not registros:
        return {}

    total_producido = sum(r["producido"] for r in registros)
    total_vendido   = sum(r["vendido"]   for r in registros)
    total_sobrante  = sum(r["sobrante"]  for r in registros)

    tasa_aprovechamiento = (total_vendido / total_producido * 100
                            if total_producido > 0 else 0)

    return {
        "total_producido":      total_producido,
        "total_vendido":        total_vendido,
        "total_sobrante":       total_sobrante,
        "tasa_aprovechamiento": round(tasa_aprovechamiento, 1),
        "perdida_estimada":     total_sobrante,  # unidades perdidas
    }
