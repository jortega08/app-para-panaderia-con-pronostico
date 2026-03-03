# Panaderia - Sistema de Ventas y Pronostico

Sistema sencillo para panaderias que combina punto de venta con pronostico
de produccion. Disenado para ser facil de usar por personas de cualquier edad.

---

## Estructura del proyecto

```
panaderia_app/
|
├── app.py                  <- Interfaz grafica (punto de entrada)
├── seed_demo.py            <- Genera datos de prueba (ejecutar 1 vez)
|
├── data/
|   ├── __init__.py
|   └── database.py         <- Base de datos SQLite
|
├── logic/
|   ├── __init__.py
|   └── pronostico.py       <- Motor de pronostico
|
└── README.md
```

---

## Instalacion

### Requisitos
- Python 3.10 o superior
- Tkinter (ya viene con Python)
- matplotlib (opcional, para graficas)

### 1. Instalar dependencias opcionales

```bash
pip install matplotlib
```

### 2. (Opcional) Generar datos de demostracion

```bash
python seed_demo.py
```

Crea 45 dias de historial con ventas simuladas.

### 3. Ejecutar

```bash
python app.py
```

---

## Roles

| Rol       | PIN default | Que puede hacer                                      |
|-----------|-------------|------------------------------------------------------|
| Panadero  | 1234        | Ver pronosticos, registrar produccion, configurar     |
| Cajero    | 0000        | Registrar ventas, ver resumen del dia                 |

Los PINs y usuarios se pueden cambiar desde Configuracion (rol panadero).

---

## Funciones principales

### Cajero
- **Registrar Venta**: Selecciona producto, cantidad, y registra. Muestra total.
- **Ventas de Hoy**: Resumen de todas las ventas del dia.

### Panadero
- **Cuantos Hornear**: Pronostico por producto con colores de estado.
- **Registrar Produccion**: Cuantos panes se hornearon (vendido se auto-llena desde ventas del cajero).
- **Ventas de Hoy**: Ver lo que registro el cajero.
- **Historial**: Tabla de produccion por dias.
- **Configuracion**: Productos, precios y usuarios.

---

## Pronostico

El sistema calcula automaticamente cuantos panes hornear:

| Datos disponibles | Metodo                  | Confianza |
|-------------------|-------------------------|-----------|
| < 7 dias          | Estimacion inicial      | Poca      |
| 7 - 29 dias       | Promedio de la semana   | Media     |
| 30+ dias          | Promedio por dia        | Buena     |

---

## Futuras mejoras

- [ ] QR en caja para que el cliente registre su pedido
- [ ] Exportar reportes a Excel/PDF
- [ ] Notificaciones al inicio del dia
- [ ] Version web
