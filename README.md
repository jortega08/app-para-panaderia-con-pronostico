# 🍞 Panadería Lean — Sistema de Pronóstico DMAIC

Sistema de gestión de producción y pronóstico de demanda para panaderías,
basado en la metodología **Lean Six Sigma (DMAIC)**.

---

## 📁 Estructura del proyecto

```
panaderia_app/
│
├── app.py                  ← Punto de entrada (interfaz gráfica)
├── seed_demo.py            ← Genera datos de prueba (ejecutar 1 vez)
│
├── data/
│   ├── __init__.py
│   └── database.py         ← Capa de datos (SQLite)
│
├── logic/
│   ├── __init__.py
│   └── pronostico.py       ← Motor de pronóstico adaptativo
│
└── README.md
```

---

## ⚙️ Instalación

### Requisitos
- Python 3.10 o superior
- Tkinter (incluido en Python estándar)
- matplotlib (opcional, para gráficas)

### 1. Instalar dependencias

```bash
pip install matplotlib
```

> Tkinter y SQLite vienen incluidos con Python. No necesitas instalarlos.

### 2. (Opcional) Generar datos de demostración

```bash
python seed_demo.py
```

Esto crea 45 días de historial simulado para que puedas explorar todas las funciones.

### 3. Ejecutar la aplicación

```bash
python app.py
```

---

## 🔬 Modelos de pronóstico

El sistema selecciona automáticamente el modelo según los datos disponibles:

| Historial disponible | Modelo usado           | Confianza |
|---------------------|------------------------|-----------|
| < 7 días            | Regla base conservadora | Baja      |
| 7 – 29 días         | Promedio móvil + buffer | Media     |
| 30+ días            | Promedio por día semana | Alta      |

---

## 📊 Nivel Sigma (DMAIC - Controlar)

El sistema calcula el nivel Sigma automáticamente:

| Nivel Sigma | Significado                        |
|------------|-------------------------------------|
| ≥ 3.0σ     | 🟢 Proceso controlado               |
| ≥ 2.0σ     | 🟡 Proceso mejorable                |
| < 2.0σ     | 🔴 Alta variabilidad, requiere acción|

---

## 🗺️ Relación con DMAIC

| Fase      | Función en el sistema                          |
|-----------|------------------------------------------------|
| Definir   | Identificar sobreproducción y desabasto        |
| Medir     | Registro diario digital (base de datos SQLite) |
| Analizar  | Pareto de sobrantes, tendencias por producto   |
| Mejorar   | Ajuste automático de producción sugerida       |
| Controlar | Alertas, nivel Sigma, dashboard de estado      |

---

## 🚀 Futuras mejoras sugeridas

- [ ] Exportar reportes a Excel/PDF
- [ ] Modelo ARIMA para series de tiempo largas (con `statsmodels`)
- [ ] Integración con clima (API) para ajuste por temporada
- [ ] Notificaciones por WhatsApp/Email al inicio del día
- [ ] Versión web con Flask o FastAPI + React
- [ ] Soporte para múltiples sucursales
