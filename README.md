# Panaderia - Sistema de Ventas y Pronostico

Aplicacion web ligera para panaderias. Combina punto de venta con
pronostico de produccion. Disenada para ser facil de usar.

## Instalacion

```bash
pip install flask
python seed_demo.py   # Datos de prueba (opcional)
python app.py         # Iniciar servidor
```

Abrir en el navegador: `http://localhost:5000`

## Roles

| Rol       | PIN  | Acceso                                           |
|-----------|------|--------------------------------------------------|
| Panadero  | 1234 | Pronosticos, produccion, ventas, historial, config |
| Cajero    | 0000 | Punto de venta con carrito, ventas del dia        |
| Cliente   | ---  | Registrar compra via QR (sin login)               |

## Funciones

### Cajero - Punto de Venta
- Carrito multi-producto con cantidad editable
- Precios automaticos desde configuracion
- Registro de venta con un click

### Panadero - Pronostico
- Grafica de produccion sugerida vs promedio
- Tarjetas por producto con estado (bien/alerta/problema)
- Pronostico adaptativo de 3 niveles

### Panadero - Configuracion
- Productos con precios editables
- Usuarios con PIN y roles
- Codigo QR para clientes

### Cliente - QR
- Pagina mobile-first accesible via QR
- Seleccionar productos y cantidades
- Registrar compra sin necesidad de cajero

## Arquitectura

```
app.py                  Flask (rutas + API)
data/database.py        SQLite
logic/pronostico.py     Motor de pronostico
templates/              HTML (Jinja2)
static/style.css        Estilos
```

## Tecnologias
- Python + Flask (backend)
- SQLite (base de datos)
- Chart.js (graficas)
- HTML/CSS/JS (frontend, sin frameworks)
