---
name: Bakery POS Project Overview
description: Arquitectura completa, módulos, base de datos, API y decisiones de diseño del sistema POS para panaderías
type: project
updatedAt: 2026-04-22
originSessionId: 66a0e190-e2f2-47b8-91a6-362e01bd66d3
---
Flask monolith POS + multi-tenant SaaS para panaderías (Colombia). SQLite dev / PostgreSQL prod (Railway). Monolito principal en `app.py` (~248 KB) + blueprints en `app/`. BD centralizada en `data/database.py` (~600 KB).

---

## Stack Tecnológico

| Capa | Tecnología |
|------|-----------|
| Backend | Flask 3.0+, Gunicorn, APScheduler 3.10+ |
| Base de datos | SQLite (dev) / PostgreSQL 12+ (prod) via `db_adapter` |
| Frontend | Jinja2, HTML5, CSS3, JavaScript vanilla |
| Visualización | ECharts 5.5.1 (bundle local `static/js/charts/echarts.min.js` + CDN fallback) |
| Contenedores | Docker, Docker Compose, Nginx reverse proxy |
| Deploy | Railway.app (PaaS), WSGI via `wsgi.py` |

**ECharts helper:** `static/js/charts/echarts-helpers.js` — gestiona ciclo de vida de instancias (`init/dispose/resizeAll`), `AppCharts.grid()` con paddings predeterminados. `ensureECharts()` usa CDN como fallback si la carga local falló.

---

## Estructura de Directorios

```
app-para-panaderia-con-pronostico/
├── app/                        # Módulos Flask (blueprints y servicios)
│   ├── context.py              # Data classes: TenantContext, SedeContext, TerminalContext, SubscriptionContext, BrandContext
│   ├── tenant_service.py       # TenantService: resolve_by_id/slug/domain/default, validación de suscripciones
│   ├── security.py             # Constantes de roles, manejo CSRF
│   ├── logging_utils.py        # Configuración de logs y request IDs para trazabilidad
│   ├── responses.py            # Builders de respuestas JSON de error estandarizadas
│   └── web/
│       ├── auth.py             # Blueprint: login (PIN/password), logout, health, jornada API
│       ├── decorators.py       # @login_required, @roles_required, @admin_required, @tenant_scope_required, @sede_scope_required
│       └── utils.py            # Contexto de usuario, CSRF tokens, brand context, filtros Jinja2
│
├── data/
│   ├── database.py             # ~100 funciones DB, migraciones fases 0–8 idempotentes
│   └── db_adapter.py           # Abstracción SQLite/PostgreSQL (conexión, parámetros, upsert)
│
├── logic/
│   └── pronostico.py           # Motor de pronóstico v2 (mezcla ponderada + anomalías + backtesting)
│
├── static/
│   ├── brand/                  # Logos y branding por tenant
│   ├── css/                    # Hojas de estilo (layout, pages, components)
│   └── js/
│       ├── charts/             # echarts.min.js (1 MB), echarts-helpers.js
│       ├── core/               # Utilidades de seguridad JS
│       └── pages/              # Scripts específicos por página
│
├── templates/
│   ├── base.html               # Layout maestro (nav, estilos, scripts)
│   ├── login.html
│   ├── error.html
│   ├── components/             # _chart_card, _kpi_card, _table_card, _pagination, _help_tooltip_trigger
│   ├── cajero_*.html           # POS, ventas, pedidos, encargos
│   ├── mesero_*.html           # Mesas, pedido por mesa, lista de pedidos
│   ├── panadero_*.html         # Pronóstico, producción, ventas, historial, cartera, documentos,
│   │                           # operaciones, inventario, jornada, backups, config, audit,
│   │                           # estandarización, cierre
│   ├── dashboard_ventas.html
│   ├── comanda_print.html      # Ticket de cocina
│   ├── factura_print.html      # Factura/recibo
│   ├── cliente_historial.html
│   └── platform_panel.html     # Panel superadmin de plataforma
│
├── app.py                      # Aplicación Flask principal (~248 KB, monolito de rutas)
├── backup.py                   # Sistema backup SQLite (API nativa + WAL) / PostgreSQL (pg_dump)
├── jobs_runner.py              # Runner de jobs: modo `once` o `daemon` con poll interval configurable
├── seed_demo.py                # Datos de demo para desarrollo
├── wsgi.py                     # Entry point WSGI
├── Dockerfile
├── docker-compose.yml          # Producción (web + nginx + PostgreSQL externo)
├── docker-compose.demo.yml     # Demo (web + nginx + PostgreSQL local)
├── gunicorn.conf.py
├── nginx.conf
├── railway.toml
└── requirements.txt            # Flask, psycopg2-binary, python-dotenv, gunicorn, APScheduler
```

---

## Multi-tenancy

`panaderias → sedes → terminales` con `tenant_memberships` como fuente de rol/panaderia_id/sede_id.

- `TenantService` resuelve contexto en `before_request` (por id, slug, dominio o `resolve_default()`).
- `SubscriptionContext` expone plan (`free/starter/pro/enterprise`), límites y `is_active`.
- `TerminalContext` asocia terminales físicas (caja, mesero, kiosko, cocina) a la sesión.
- `_apply_tenant_scope()` helper interno: añade `WHERE panaderia_id = ?` a cualquier query.
- `resolve_default()` solo para no autenticados; autenticados siempre por `session["usuario"]["panaderia_id"]`.

**Límites del plan `free`:** 1 sede, 5 usuarios, 50 productos. Planes superiores expanden los límites.

---

## Roles y Acceso

| Rol | Acceso |
|-----|--------|
| `platform_superadmin` | Bypasa checks de tenant, sede y suscripción |
| `tenant_admin` | Gestión de panadería: usuarios, config, sedes |
| `panadero` | Dashboards, pronóstico, producción, inventario, historial, cierre |
| `cajero` | POS, ventas, encargos, caja |
| `mesero` | Mesas, pedidos de mesa |

---

## Seguridad de Sesión

- **Login dual:** PIN operativo (cajero/mesero) + usuario/password para admin.
- **Rate limiting:** tracking de intentos fallidos por IP en `login_attempts`; lockout tras N fallos.
- **`session_version`** (int en `usuarios`) — se incrementa al revocar acceso.  
  `before_request` compara `session["usuario"]["session_version"]` contra DB en cada request (excepto login/logout/static). Si difieren → `session.clear()` + redirect/401.
- **Cookies:** HTTPOnly, SameSite=Lax, Secure en producción. CSRF tokens en formularios.
- **Jornada:** `usuarios.jornada_activa` controla si cajero/mesero puede iniciar sesión operativa.
- **Audit log:** Todas las acciones sensibles quedan en `audit_log` (usuario, IP, timestamp, entidad, resultado).

---

## Base de Datos — Tablas Principales

### Multi-tenant
| Tabla | Descripción |
|-------|-------------|
| `panaderias` | Organización (slug, nombre, activa, dominio_custom) |
| `sedes` | Sucursal (panaderia_id, slug, nombre, codigo, activa) |
| `terminales` | Dispositivo POS (sede_id, tipo, codigo, last_seen_at) |
| `tenant_subscriptions` | Plan (panaderia_id, plan, estado, max_sedes, max_usuarios, max_productos) |
| `tenant_branding` | Marca visual (logo_path, favicon_path, colors) |
| `tenant_memberships` | Relación usuario-tenant-sede-rol |

### Usuarios y Seguridad
| Tabla | Descripción |
|-------|-------------|
| `usuarios` | Usuarios (pin_hash, password_hash, rol, jornada_activa, session_version, last_login_at) |
| `login_attempts` | Intentos fallidos (scope_key, attempts, locked_until) |

### Catálogo
| Tabla | Descripción |
|-------|-------------|
| `productos` | Catálogo (nombre, precio, categoria, es_panaderia, stock_minimo, surtido_tipo) |
| `categorias_producto` | Categorías activas por tenant |
| `adicionales` | Extras/toppings opcionales |

### Ventas y Pedidos
| Tabla | Descripción |
|-------|-------------|
| `ventas` | Transacciones de venta directa (venta_grupo, metodo_pago, monto_recibido, cambio) |
| `pedidos` | Órdenes de mesa (estado, mesa_id, mesero, total, metodo_pago) |
| `pedido_items` | Ítems del pedido |
| `pedido_item_modificaciones` | Personalizaciones (adicionales, exclusiones) |
| `pedido_estado_historial` | Historial de cambios de estado |
| `mesas` | Mesas físicas (numero, nombre, activa, eliminada) |

### Encargos (Pre-pedidos)
| Tabla | Descripción |
|-------|-------------|
| `encargos` | Pre-pedido con fecha de entrega (cliente, empresa, estado, total) |
| `encargo_items` | Ítems del encargo |

### Inventario y Recetas
| Tabla | Descripción |
|-------|-------------|
| `insumos` | Ingredientes (unidad, stock, stock_minimo) |
| `inventario_sede` | Stock por sucursal (insumo_id, sede_id, stock) |
| `recetas` | Ingredientes por producto (cantidad, unidad_receta) |
| `receta_fichas` | Fichas técnicas (rendimiento, tiempo_preparacion, tiempo_amasado) |
| `producto_componentes` | Desglose de componentes de producto |
| `adicional_insumos` | Ingredientes requeridos por extra |

### Producción y Pronóstico
| Tabla | Descripción |
|-------|-------------|
| `registros_diarios` | Log diario (fecha, dia_semana, producido, vendido, sobrante_inicial) |
| `ajustes_pronostico` | Ajustes manuales al forecast (tipo, valor, razon) |
| `dias_especiales` | Factores para días especiales (factor, tipo, descripcion) |
| `mermas` | Registros de desperdicio/merma |
| `alertas` | Alertas por umbral de producto |

### Caja y Contabilidad
| Tabla | Descripción |
|-------|-------------|
| `arqueos_caja` | Apertura/cierre de caja (monto_apertura, monto_cierre, diferencia, efectivo_esperado) |
| `movimientos_caja` | Transacciones (tipo, concepto, monto, registrado_por) |

### Sistema
| Tabla | Descripción |
|-------|-------------|
| `audit_log` | Trazabilidad completa (accion, entidad, entidad_id, ip, resultado) |
| `configuracion_sistema` | Configuración clave-valor por tenant |

---

## API Endpoints Principales

### Autenticación y Sistema (`app/web/auth.py`)
```
GET/POST /login              Login (PIN o password)
GET      /logout
GET      /health             Liveness check
GET      /ready              Readiness check (incluye DB)
GET/POST /cambiar-password
POST     /api/cambiar-password
GET      /api/terminal/lookup   Info de terminal por código (público)
GET      /api/panaderia/lookup  Info de panadería por código (público)
GET/POST /api/jornada/*         Apertura/cierre de jornada, activar usuarios
```

### Cajero
```
GET  /cajero/pos
GET  /cajero/ventas
GET  /cajero/pedidos
GET  /cajero/pedido/<id>/editar
POST /api/caja/abrir | /api/caja/cerrar
POST /api/caja/movimiento
```

### Mesero
```
GET /mesero/mesas
GET /mesero/pedido/<mesa_id>
GET /mesero/pedidos
```

### Panadero (Dashboards)
```
GET /panadero/pronostico       Pronóstico de demanda
GET /panadero/produccion       Planificación y registro de lotes
GET /panadero/ventas           Dashboard de ventas
GET /panadero/historial        Tendencias históricas
GET /panadero/cartera          Cuentas por cobrar
GET /panadero/documentos       Facturas/documentos
GET /panadero/operaciones      Operaciones y ajustes
GET /panadero/inventario       Stock de ingredientes
GET /panadero/jornada          Gestión de jornada
GET /panadero/backups          Backup/restore
GET /panadero/config           Configuración
GET /panadero/audit            Log de auditoría
GET /panadero/estandarizacion  Estandarización de recetas
GET /panadero/cierre           Cierre diario
```

### API JSON (selección)
```
GET  /api/productos
POST /api/producto | PUT /api/producto/<id> | DELETE /api/producto/<id>
POST /api/productos/importar
GET  /api/pronostico/dashboard
GET  /api/pronostico/sugerencia
GET  /api/produccion/contexto | /api/produccion/contexto-masivo
POST /api/produccion/validar-insumos
POST /api/produccion/lotes-masivos
POST /api/produccion/descartar
GET  /api/inventario/proyeccion-insumos
GET  /api/historial/dashboard
POST /api/venta
GET  /api/ventas/hoy
POST /api/pedido
POST /api/mesa/<id>/unir-cuentas
POST /api/usuario | PUT /api/usuario/<id>
POST /api/usuario/<id>/reset-pin
POST /api/config/codigo-caja
```

**Formato de respuesta:** `{"ok": true/false, "data": {...}, "error": "..."}`  
**Paginación:** `?page=1&size=50` con validación de límite.

---

## Motor de Pronóstico (`logic/pronostico.py`)

**Modelo v2 — mezcla ponderada de 3 componentes:**
```
Pronóstico = 50% (promedio histórico por día de semana)
           + 30% (media móvil últimos 7 días)
           + 20% (tendencia ajustada)
           + buffer de seguridad 10%
           + overlay de encargos confirmados
```

**Constantes:**
- `BUFFER_SEGURIDAD = 0.10`
- `DIAS_PROMEDIO_MOVIL = 7`
- `DIAS_NIVEL_ALTO = 30` (ventana de estabilidad)
- `OUTLIER_DESVIACIONES = 2.5` (umbral detección de anomalías)

**Funciones clave:**
| Función | Descripción |
|---------|-------------|
| `calcular_pronostico()` | Motor principal de predicción |
| `calcular_eficiencia()` | Métricas de precisión del forecast |
| `analizar_tendencia()` | Dirección y magnitud de tendencia |
| `obtener_historial_pronostico()` | Datos históricos para visualización |
| `obtener_resumen_pronostico_por_dia_semana()` | Patrones semanales |
| `calcular_backtesting()` | Validación: ventana deslizante → MAPE, MAE, hit_rate_10%, hit_rate_20% |
| `obtener_encargos_confirmados_para_fecha()` | Overlay de demanda comprometida |

**Manejo especial:**
- División por cero: fallback a 0.0 cuando `suma_pesos == 0`
- Calidad calculada con CV de la serie de ventas (no sobrante/producido)
- `_redondear_produccion()` retorna 0 si `valor <= 0` (no fuerza mínimo 1)
- Días especiales: factores configurables por tenant/sede/fecha

---

## Módulos de Funcionalidad

### Gestión de Caja
- Ciclo de vida: Apertura → Movimientos → Cierre con cuadre
- Diferencia esperado vs real; código de verificación opcional
- Soporte multi-método: efectivo, tarjeta, transferencia

### Gestión de Pedidos (Restaurant)
- Estados: `pendiente → en_preparacion → listo → pagado → cancelado`
- Historial de estados con timestamp
- Modificaciones de ítems (adicionales, exclusiones)
- División de cuentas entre métodos de pago
- Impresión de comanda para cocina

### Encargos (Pre-pedidos)
- Fecha de entrega separada de la fecha de creación
- Estados: `pendiente → listo → entregado → cancelado`
- Pago al momento de entrega soportado
- Se integran como overlay en el motor de pronóstico

### Backup y Recuperación
- **SQLite:** API nativa con soporte WAL
- **PostgreSQL:** `pg_dump` con rotación automática (retención 30 días)
- Jobs runner: modo `once` (cron/systemd) o `daemon` (polling por `JOBS_POLL_SECONDS`)
- UI de backup en `/panadero/backups`

### Inventario y Recetas
- Stock por sede (`inventario_sede`)
- Recetas mapeadas a insumos con cantidades y unidades
- Proyección de insumos necesarios para producción planificada
- Alertas por stock mínimo
- Registro de mermas con razón

### Análisis e Historial
- KPIs: aprovechamiento (vendido/producido %), desperdicio %, días con quiebre
- Evolución diaria con dataZoom (slider temporal cuando periodo > 20 días)
- Diferencias de cierre de caja por día (`chartDifCaja`)
- Dashboard de ventas: ticket promedio, ventas por hora (24 barras)
- Exportación CSV de ventas e inventario

---

## Releases Completados

### Release 4 — Pronóstico v2 (2026-04-19/21)
- 6 bugs corregidos en `logic/pronostico.py` (división por cero, calidad, redondeo, blend, encargos, backtesting)
- Panel backtesting en `panadero_pronostico.html`: KPIs MAPE/MAE/hit_rate, gráfico ECharts, tabla de evaluaciones

### Release 5 — Dashboards v2 (2026-04-21)
- `dashboard_ventas.html`: KPI ticket promedio + gráfico ventas por hora
- `panadero_historial.html`: KPIs aprovechamiento/desperdicio/quiebres + gráfico diferencias de caja + dataZoom

### Pendiente Release 5
- Drilldown interactivo: click en barra de `chartEvolucionDiaria` filtra tabla al día
- `echarts-helpers.js`: agregar `heatmapOption`, `stackedBarOption`, `waterfallOption`

### Release 6 — Hardening (próximo)
- pytest suite (cobertura rutas API críticas)
- APScheduler externo para backup jobs
- Paginación estándar `?page=1&size=50` en todos los endpoints de listado

---

## Decisiones de Diseño

- **Encargos como overlay:** `obtener_encargos_confirmados_para_fecha` suma demanda comprometida sobre el forecast base — no modifica el modelo, se muestra en capa separada.
- **`session_version` check:** Verificación en cada request garantiza revocación inmediata de sesiones sin depender de expiración de cookie.
- **Monolito intencional:** `app.py` y `data/database.py` se mantienen monolíticos para simplicidad de deploy; blueprints en `app/web/` para código nuevo.
- **Migraciones idempotentes:** `data/database.py` ejecuta `CREATE TABLE IF NOT EXISTS` y `ALTER TABLE ... ADD COLUMN` con verificación; safe para redeploys.
- **`_apply_tenant_scope`:** Toda función DB que accede a datos de panadería debe pasar por este helper — garantiza aislamiento multi-tenant.
