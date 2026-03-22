# Panaderia Richs - Sistema de ventas, produccion y pronostico

Aplicacion web desarrollada en **Python + Flask + SQLite** para apoyar la
operacion diaria de una panaderia. El sistema integra:

- punto de venta para cajero
- toma de pedidos por mesa para mesero
- registro de produccion diaria para panaderia
- inventario de insumos
- recetas y productos compuestos
- pronostico de produccion para productos de categoria `Panaderia`
- control de caja con apertura, movimientos, cierre y reapertura
- dashboards operativos, contables e historicos

## Tecnologias

- Python
- Flask
- SQLite
- HTML + Jinja2
- JavaScript
- CSS
- Chart.js

## Ejecucion

1. Inicializar datos de prueba si se requiere:

```bash
python seed_demo.py
```

2. Ejecutar la aplicacion:

```bash
python app.py
```

## Usuarios por defecto

- `Admin` - PIN `1234` - rol `panadero`
- `Cajero` - PIN `0000` - rol `cajero`
- `Mesero` - PIN `1111` - rol `mesero`

Si estos usuarios ya fueron modificados desde configuracion, se deben usar los
datos actualizados.

## Modulos principales

### Panadero / Administrador

- Pronostico de produccion
- Registro de produccion
- Ventas de hoy
- Operaciones
- Historial
- Inventario
- Respaldos
- Configuracion

### Cajero

- Punto de venta
- Pedidos
- Ventas de hoy
- Apertura, movimientos, cierre y reapertura de caja

### Mesero

- Mesas
- Toma de pedido por mesa
- Seguimiento de pedidos

## Reglas clave del sistema

- Solo los productos de categoria `Panaderia` participan en produccion y
  pronostico.
- No se pueden vender o comprometer productos de panaderia si no existe
  produccion suficiente registrada para el dia.
- Los pedidos pueden pasar por estados como `pendiente`, `en preparacion`,
  `listo` y `pagado`.
- Los adicionales se configuran desde administracion y pueden descontar insumos
  o productos base del inventario.
- La caja debe abrirse antes de cobrar.
- El cierre y la reapertura de caja requieren codigo de verificacion.

## Documentacion

Consulta el manual de usuario completo en:

- [MANUAL_USUARIO.md](C:/Users/mondr/OneDrive/Documentos/VisualStudioCode/PuntoVentaPanaderia/app-para-panaderia-con-pronostico/MANUAL_USUARIO.md)
