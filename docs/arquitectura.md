# Evaluación de Arquitectura — PuntoVenta Panadería SaaS

> Estado al: 2026-04-19  
> Versión del sistema: multi-tenant SaaS (Fases 0–7 implementadas)

---

## Índice

1. [¿Qué es el sistema?](#1-qué-es-el-sistema)
2. [Stack tecnológico](#2-stack-tecnológico)
3. [Estructura de módulos](#3-estructura-de-módulos)
4. [Cómo se comunican las partes](#4-cómo-se-comunican-las-partes)
5. [Flujo completo de una request](#5-flujo-completo-de-una-request)
6. [Modelo de datos](#6-modelo-de-datos)
7. [Roles y permisos](#7-roles-y-permisos)
8. [Reglas de negocio](#8-reglas-de-negocio)
9. [Multi-tenancy](#9-multi-tenancy)
10. [Autenticación y sesiones](#10-autenticación-y-sesiones)
11. [Seguridad](#11-seguridad)
12. [Algoritmo de pronóstico](#12-algoritmo-de-pronóstico)
13. [Despliegue e infraestructura](#13-despliegue-e-infraestructura)
14. [Puntos de extensión y deuda técnica](#14-puntos-de-extensión-y-deuda-técnica)

---

## 1. ¿Qué es el sistema?

**PuntoVenta Panadería** es una aplicación web SaaS B2B diseñada para gestionar panaderías artesanales. Corre como una aplicación Flask monolítica multi-tenant y actualmente soporta una panadería con múltiples sedes, con infraestructura lista para escalar a N tenants independientes.

### Qué hace

| Módulo | Función |
|--------|---------|
| **Punto de venta (POS)** | Registro de ventas individuales y en lote, arqueo de caja, métodos de pago múltiples |
| **Pedidos & Mesas** | Creación de pedidos por mesa, asignación a mesero, división de cuenta, estados del pedido |
| **Encargos** | Pedidos especiales con fecha de entrega, seguimiento por estado |
| **Pronóstico de producción** | Sugerencia diaria de cuánto producir por producto usando promedio móvil ponderado + tendencia |
| **Registro de producción** | Ingreso de producido/vendido/sobrante por día, descarte de sobrante |
| **Inventario & Insumos** | Control de stock de insumos, alertas de stock bajo, recetas con fichas técnicas |
| **Usuarios & Jornada** | Alta/baja de cajeros y meseros, activación de jornada diaria por sede |
| **Backup** | Backup automático a las 23:00, respaldo manual, restauración, compatible con SQLite y PostgreSQL |
| **Auditoría** | Registro inmutable de acciones críticas con trazabilidad completa |
| **Panel de Plataforma** | Vista de todas las panaderías, gestión de planes de suscripción y estado operativo |

---

## 2. Stack tecnológico

| Capa | Tecnología | Versión / Notas |
|------|-----------|----------------|
| Lenguaje | Python | 3.11 |
| Framework web | Flask | Con blueprints y `g` request context |
| WSGI server | Gunicorn | `sync` workers, sin async |
| Reverse proxy | Nginx | Alpine, puerto 80 → 5000 |
| Base de datos principal | SQLite (dev) / PostgreSQL (prod) | Adaptador `db_adapter.py` transparente |
| ORM / Queries | SQL directo | Sin ORM — queries crudas con `sqlite3` / `psycopg2` |
| Templating | Jinja2 | Vía Flask; sin SSR frameworks modernos |
| Gráficas | ECharts 5.5 | CDN con fallback a cdnjs |
| CSS | Custom | 4 archivos (core, layout, components, pages) + style.css global |
| JavaScript | Vanilla JS | Sin frameworks front-end (React, Vue, etc.) |
| Containerización | Docker | `python:3.11-slim`, usuario no-root |
| Orquestación | Docker Compose | 2 servicios: web + nginx |
| Autenticación | Sesiones Flask firmadas | `session` cookie `HttpOnly`, `SameSite=Lax` |
| CSRF | Token en sesión + header `X-CSRF-Token` | Validado en `before_request` |

---

## 3. Estructura de módulos

```
app-para-panaderia-con-pronostico/
│
├── app.py                    # Aplicación Flask principal (>4000 líneas)
│                             # Todas las rutas, before/after_request,
│                             # helpers internos, context global de Jinja
│
├── wsgi.py                   # Entry point de Gunicorn → importa app de app.py
│
├── backup.py                 # Sistema de backup (SQLite nativo / pg_dump)
│
├── app/                      # Módulos core de la aplicación
│   ├── context.py            # Dataclasses de request context (frozen)
│   │   ├── TenantContext
│   │   ├── SedeContext
│   │   ├── SubscriptionContext
│   │   ├── TerminalContext
│   │   └── BrandContext
│   │
│   ├── tenant_service.py     # Resolución y validación del tenant activo
│   │   └── TenantService     # 14 métodos estáticos + 3 excepciones
│   │
│   ├── security.py           # Constantes de roles, CSRF helpers
│   ├── responses.py          # json_error(), wants_json_response()
│   ├── logging_utils.py      # configure_app_logging(), generate_request_id()
│   │
│   └── web/
│       ├── auth.py           # Blueprint "auth" — login, logout, jornada API
│       ├── decorators.py     # @login_required, @roles_required, @tenant_scope_required,
│       │                     # @sede_scope_required, @admin_required
│       └── utils.py          # Helpers de sesión, iconos, CSRF, rate limiting
│
├── data/
│   ├── database.py           # Toda la capa de datos (~3800 líneas)
│   │   ├── 33 tablas SQLite/PostgreSQL
│   │   ├── 140+ funciones públicas
│   │   ├── Sistema de migraciones idempotentes (_migrar_*)
│   │   └── Multi-tenant scope por hilo (_tenant_scope, _apply_tenant_scope)
│   └── db_adapter.py         # Abstracción SQLite ↔ PostgreSQL (get_database_info)
│
├── logic/
│   └── pronostico.py         # Algoritmo de pronóstico de producción
│                             # (promedio móvil ponderado + tendencia + días especiales)
│
├── templates/                # 28 templates Jinja2
│   ├── base.html             # Layout base heredado por todos
│   ├── login.html
│   ├── platform_panel.html   # Panel platform_superadmin
│   ├── cajero_*.html         # Vistas del cajero (3)
│   ├── mesero_*.html         # Vistas del mesero (3)
│   ├── panadero_*.html       # Vistas del panadero/admin (12)
│   └── components/           # Componentes reutilizables (_kpi_card, _chart_card, etc.)
│
├── static/
│   ├── style.css             # CSS global
│   ├── css/                  # core / layout / components / pages
│   ├── js/
│   │   ├── core/security.js  # CSRF y helpers de seguridad front-end
│   │   └── charts/echarts-helpers.js
│   └── brand/                # Logo SVG (richs-logo.svg)
│
├── backups/                  # Directorio de archivos de backup
├── docs/                     # Documentación
├── Dockerfile
├── docker-compose.yml
├── gunicorn.conf.py
└── requirements.txt
```

---

## 4. Cómo se comunican las partes

### Diagrama de capas

```
┌─────────────────────────────────────────────────────┐
│                   CLIENTE (Browser)                  │
│  HTML/CSS/JS vanilla + ECharts + fetch() para APIs  │
└────────────────────┬────────────────────────────────┘
                     │ HTTP
                     ▼
┌─────────────────────────────────────────────────────┐
│                   NGINX (puerto 80)                  │
│  Reverse proxy → localhost:5000                      │
│  Static files servidos directamente (si configura)   │
└────────────────────┬────────────────────────────────┘
                     │ HTTP (interno)
                     ▼
┌─────────────────────────────────────────────────────┐
│            GUNICORN (puerto 5000)                    │
│  Workers síncronos (min(cpu*2+1, 4))                │
│  Timeout 120s, preload_app=True                      │
└────────────────────┬────────────────────────────────┘
                     │ WSGI
                     ▼
┌─────────────────────────────────────────────────────┐
│              FLASK (app.py + blueprints)             │
│                                                      │
│  before_request:                                     │
│    1. generate_request_id                            │
│    2. _resolver_contextos_request()                  │
│       → TenantService.resolve_*()                   │
│       → g.tenant/sede/brand/subscription/terminal   │
│    3. Guards: tenant suspendido (503), plan vencido  │
│    4. CSRF validation                                │
│                                                      │
│  Routing → @app.route / blueprint auth_bp            │
│  Decoradores: @login_required, @roles_required, etc. │
│                                                      │
│  after_request:                                      │
│    → Security headers (X-Frame-Options, HSTS, etc.) │
└────────────────────┬────────────────────────────────┘
                     │ Python function calls
                     ▼
┌────────────────────────────────────┐
│        data/database.py            │
│  • _tenant_scope() por hilo        │
│  • _apply_tenant_scope() en queries│
│  • get_connection() → SQLite/PG    │
└────────────────────┬───────────────┘
                     │ SQL
                     ▼
┌────────────────────────────────────┐
│     SQLite (dev) / PostgreSQL (prod)│
│     33 tablas, schema auto-migrate  │
└────────────────────────────────────┘
```

### Comunicación Front ↔ Back

Existen dos patrones en la misma app:

**1. Server-Side Rendering (SSR) — vistas HTML:**
```
GET /panadero/pronostico
  → @login_required → @roles_required
  → Python fetches data from DB
  → render_template("panadero_pronostico.html", **ctx)
  → HTML completo al cliente
```

**2. API JSON — operaciones interactivas:**
```
POST /api/venta
  Headers: X-CSRF-Token: <token>
  Body: { "producto": "Pan Francés", "cantidad": 5, ... }
  → @login_required
  → registrar_venta(...)
  → { "ok": true }
```

Todos los endpoints API retornan `{"ok": true|false, "data": ..., "error": "..."}`.  
El CSRF token se envía en el header `X-CSRF-Token` (obtenido del meta tag en base.html).

---

## 5. Flujo completo de una request

### Request autenticada: `POST /api/venta`

```
1. Browser → nginx → gunicorn → Flask

2. before_request():
   a. g.request_id = generate_request_id()  # UUID para trazabilidad
   b. _resolver_contextos_request():
      - session["usuario"]["panaderia_id"] → TenantService.resolve_by_id()
      - session["usuario"]["sede_id"]      → TenantService.resolve_sede_by_id()
      - session["usuario"]["terminal_id"]  → TenantService.resolve_terminal_by_id()
      - TenantService.get_subscription()   → SubscriptionContext
      - TenantService.get_branding()       → BrandContext
   c. g.tenant_context, g.sede_context, g.brand_context,
      g.subscription_context, g.terminal_context = ...
   d. set_query_context(tenant_id, sede_id)  # hilo local para DB queries
   e. TenantService.touch_terminal()         # actualiza last_seen_at
   f. Guards (solo si usuario autenticado y no platform_superadmin):
      - if not tenant_context.is_active → abort(503)
      - if subscription_context.is_expired → abort(402)
   g. CSRF check (POST/PUT/PATCH/DELETE): X-CSRF-Token == session["_csrf_token"]

3. @login_required:
   - Verifica "usuario" en session
   - Verifica edad de sesión (SESSION_LIFETIME_HOURS, default 8h)
   - Verifica inactividad (SESSION_LIFETIME_HOURS desde _last_activity_ts)
   - Actualiza session["_last_activity_ts"]

4. @roles_required (si aplica):
   - _rol_usuario_actual() ∈ roles_permitidos

5. View function: registrar_venta(...)
   - Llama data/database.py → _tenant_scope() para panaderia_id/sede_id
   - INSERT INTO ventas (..., panaderia_id=?, sede_id=?)
   - return {"ok": True}

6. after_request():
   - X-Request-Id, X-Content-Type-Options, X-Frame-Options,
     X-XSS-Protection, Referrer-Policy, [HSTS si HTTPS]

7. JSON response → gunicorn → nginx → Browser
```

### Login operativo (cajero/mesero): `POST /login` (modo=operativo)

```
1. Formulario: codigo_panaderia + username_op + pin + terminal_codigo?

2. verificar_pin_operativo(codigo_panaderia, username, pin):
   - JOIN panaderias + usuarios por código y username
   - Valida pin (hash bcrypt)
   - _enriquecer_con_membresia() → membership_id autoritativo
   - if not membership_id → return None (membresía inactiva = deniega)

3. Si usuario válido:
   - _registrar_sesion(usuario)
   - if terminal_codigo → obtener_terminal_por_codigo(sede_id, codigo)
     - if activa → session["usuario"]["terminal_id"] = terminal.id

4. registrar_audit(accion="login", resultado="ok")

5. _redirect_post_login() → /cajero/pos o /mesero/mesas
```

---

## 6. Modelo de datos

### Tablas por dominio

#### Plataforma / Multi-tenant

| Tabla | Rol | Columnas clave |
|-------|-----|----------------|
| `panaderias` | Tenant raíz | `slug`, `activa`, `estado_operativo`, `codigo` |
| `sedes` | Sub-unidad del tenant | `panaderia_id FK`, `slug`, `codigo` UNIQUE por tenant |
| `tenant_memberships` | Relación usuario↔tenant | `usuario_id FK`, `panaderia_id FK`, `sede_id FK`, `rol`, `activa` |
| `tenant_subscriptions` | Plan comercial | `panaderia_id UNIQUE FK`, `plan`, `estado`, `fecha_vencimiento`, `max_*` |
| `terminales` | POS físico por sede | `sede_id FK`, `codigo` UNIQUE por sede, `tipo`, `last_seen_at` |
| `tenant_branding` | Marca visual | `panaderia_id FK`, colores, rutas de logo |

#### Usuarios y seguridad

| Tabla | Rol | Columnas clave |
|-------|-----|----------------|
| `usuarios` | Identidad | `username`, `email`, `password_hash`, `pin`, `rol`, `panaderia_id FK`, `sede_id FK` |
| `login_attempts` | Rate limiting | `scope_key`, `attempt_count`, `locked_until` |
| `audit_log` | Trazabilidad | `usuario`, `accion`, `entidad`, `entidad_id`, `detalle`, `panaderia_id`, `sede_id` |

#### Operaciones

| Tabla | Rol | Columnas clave |
|-------|-----|----------------|
| `productos` | Catálogo | `nombre`, `precio`, `categoria`, `es_panaderia`, `stock_minimo`, `panaderia_id` |
| `ventas` | Transacciones | `venta_grupo`, `referencia_tipo`, `metodo_pago`, `panaderia_id`, `sede_id` |
| `pedidos` | Mesa + estado | `mesa_id FK`, `mesero`, `estado`, `panaderia_id`, `sede_id` |
| `pedido_items` | Líneas de pedido | `pedido_id FK`, `producto`, `cantidad`, `precio_unitario` |
| `pedido_item_modificaciones` | Adicionales / exclusiones | `item_id FK`, `tipo`, `nombre`, `precio` |
| `mesas` | Física de sala | `numero`, `capacidad`, `ubicacion`, `panaderia_id`, `sede_id` |
| `encargos` | Pedidos futuros | `fecha_entrega`, `cliente_nombre`, `estado`, `panaderia_id`, `sede_id` |
| `arqueos_caja` | Control de caja | `abierto_por`, `efectivo_real`, `diferencia`, `panaderia_id`, `sede_id` |
| `movimientos_caja` | Entradas/salidas | `tipo`, `concepto`, `monto`, `panaderia_id`, `sede_id` |

#### Producción e inventario

| Tabla | Rol | Columnas clave |
|-------|-----|----------------|
| `registros_diarios` | Producción diaria | `fecha`, `producto`, `producido`, `vendido`, `sobrante`, `panaderia_id` |
| `insumos` | Materias primas | `nombre`, `stock`, `stock_minimo`, `unidad`, `panaderia_id` |
| `recetas` | Proceso de elaboración | `producto`, `rendimiento`, `tiempos`, `temperatura`, `pasos` |
| `receta_fichas` | Ficha técnica extendida | `panaderia_id` |
| `producto_componentes` | Insumos por producto | `producto_base`, `insumo FK`, `cantidad_requerida`, `unidad_receta` |
| `adicionales` | Extras del menú | `nombre`, `precio`, `panaderia_id`, `sede_id` |
| `mermas` | Pérdidas registradas | `fecha`, `producto`, `cantidad`, `razon` |
| `dias_especiales` | Factores de demanda | `fecha`, `nombre`, `factor_demanda` |
| `ajustes_pronostico` | Override manual | `fecha`, `producto`, `ajustado` |

### Claves de aislamiento multi-tenant

Toda tabla operativa tiene columnas `panaderia_id` y/o `sede_id`. Las queries siempre pasan por `_apply_tenant_scope()`:

```python
# Automático en todas las queries
filtros = ["activa = 1"]
params  = []
_apply_tenant_scope(filtros, params)
# → agrega "AND panaderia_id = ?" con el id del hilo actual
```

---

## 7. Roles y permisos

### Jerarquía de roles

```
platform_superadmin          ← Nivel de plataforma (sin tenant fijo)
    │
    └── tenant_admin         ← Admin de una panadería
            │
            ├── panadero     ← Operador de producción / supervisor
            │
            ├── cajero       ← Punto de venta (login con PIN)
            │
            └── mesero       ← Gestión de mesas (login con PIN)
```

### Matriz de acceso por rol

| Módulo / Vista | platform_superadmin | tenant_admin | panadero | cajero | mesero |
|----------------|:-------------------:|:------------:|:--------:|:------:|:------:|
| Panel de plataforma | ✓ | — | — | — | — |
| Pronóstico producción | ✓ (bypass) | ✓ | ✓ | — | — |
| Producción / Historial | ✓ | ✓ | ✓ | — | — |
| POS (Caja) | ✓ | ✓ | — | ✓ | — |
| Ventas del cajero | ✓ | ✓ | — | ✓ | — |
| Pedidos cajero | ✓ | ✓ | — | ✓ | — |
| Mesas (mesero) | — | ✓ | — | — | ✓ |
| Pedidos mesero | — | ✓ | — | — | ✓ |
| Inventario / Insumos | ✓ | ✓ | ✓ | — | — |
| Configuración | ✓ | ✓ | — | — | — |
| Usuarios | ✓ | ✓ | ✓ | — | — |
| Jornada (abrir/cerrar) | ✓ | ✓ | ✓ | — | — |
| Backups | ✓ | ✓ | ✓ | — | — |
| Audit log | ✓ | ✓ | — | — | — |
| Cambiar plan suscripción | ✓ | — | — | — | — |
| Suspender/activar tenant | ✓ | — | — | — | — |

### Constantes en `app/security.py`

```python
PLATFORM_ADMIN_ROLE = "platform_superadmin"
TENANT_ADMIN_ROLE   = "tenant_admin"
OPERATIONAL_ROLES   = {"panadero", "cajero", "mesero"}
VALID_ROLES         = {PLATFORM_ADMIN_ROLE, TENANT_ADMIN_ROLE, *OPERATIONAL_ROLES}
ADMIN_ROLES         = {PLATFORM_ADMIN_ROLE, TENANT_ADMIN_ROLE, "panadero"}
```

### Comportamiento especial de `platform_superadmin`

- No tiene `panaderia_id` ni `sede_id` en su registro de usuario
- No requiere `tenant_memberships` activa
- Bypasa los guards de `abort(503)` (tenant suspendido) y `abort(402)` (plan vencido)
- Bypasa `@tenant_scope_required` y `@sede_scope_required`
- No se le fuerza cambio de contraseña
- Bypasa los límites de plan en creación de productos y usuarios

---

## 8. Reglas de negocio

### Autenticación y acceso

- **Login admin** (tenant_admin, panadero): usuario/email + contraseña (bcrypt, mín. 8 caracteres)
- **Login operativo** (cajero, mesero): código de panadería + username + PIN de 4 dígitos
- **Bloqueo por intentos**: 5 intentos fallidos → bloqueo de 5 minutos (configurable via env)
- **Sesión**: expira a las 8 horas de login O por inactividad de 8 horas (configurable)
- **Jornada**: cajeros y meseros deben tener `jornada_activa = 1` para poder hacer login operativo; un admin debe abrirla explícitamente

### Producción y stock

- Un producto puede tener `stock_minimo`; si `vendido_hoy >= producido_hoy - stock_minimo`, se genera alerta
- El "sobrante" del día anterior se usa como `sobrante_inicial` del registro siguiente
- El descarte de sobrante genera un movimiento que reduce el stock disponible
- Los insumos se descuentan automáticamente al registrar producción (por la receta asociada)

### Pedidos

- Un pedido pasa por estados: `pendiente → listo → pagado` o `pendiente → cancelado`
- El historial de estados queda en `pedido_estado_historial`
- Un mesero solo puede ver sus propios pedidos (filtro `_pedido_visible_para_usuario`)
- Se puede dividir un pedido (`dividir_pedido_y_cobrar`) — crea un pedido nuevo con los ítems seleccionados
- Se pueden unificar pedidos de una misma mesa (`unificar_pedidos`)

### Encargos

- Son pedidos futuros con `fecha_entrega`; no afectan stock inmediatamente
- Estados: `pendiente → confirmado → listo → entregado → cancelado`

### Caja

- Solo puede haber un arqueo abierto por sede por día
- El cierre registra `efectivo_real` vs `efectivo_esperado`, calcula diferencia
- Los movimientos de caja (entradas/salidas) ajustan el balance

### Límites de plan

| Plan | Sedes | Usuarios | Productos |
|------|------:|--------:|----------:|
| free | 1 | 5 | 50 |
| starter | 1 | 10 | 100 |
| pro | 3 | 20 | 500 |
| enterprise | 999 | 999 | 999 |

- Los límites se verifican al **crear** (no al listar). Si el DB ya tiene más datos de los que permite el plan (caso de downgrade), los existentes no se borran, pero no se pueden agregar nuevos.
- `platform_superadmin` nunca tiene límites aplicados.

### Días especiales y pronóstico

- Un `dia_especial` tiene un `factor_demanda` (p.ej. 1.5 para Navidad)
- Este factor multiplica la sugerencia base del pronóstico
- El ajuste manual (`ajustes_pronostico`) permite al panadero sobreescribir la sugerencia

---

## 9. Multi-tenancy

### Modelo de aislamiento

El sistema usa un **discriminador de columna** (`panaderia_id` / `sede_id`) en todas las tablas operativas. No hay bases de datos separadas por tenant.

### Resolución del tenant por request

En `before_request`, `_resolver_contextos_request()` sigue esta cadena:

```
1. ¿Hay usuario en sesión con panaderia_id?
   → TenantService.resolve_by_id(session_panaderia_id)

2. ¿No hay usuario en sesión?
   → Intenta resolución por dominio del host: TenantService.resolve_by_domain(host)

3. Fallback:
   → TenantService.resolve_default() (primera panadería activa en DB)

4. Sede:
   → TenantService.resolve_sede_by_id(session_sede_id) || resolve_sede_default(tenant_id)

5. Terminal (opcional, login operativo):
   → TenantService.resolve_terminal_by_id(session_terminal_id) si existe en sesión
```

Resultado: `g.tenant_context`, `g.sede_context`, `g.subscription_context`, `g.terminal_context`, `g.brand_context` disponibles en toda request.

### Membresías (`tenant_memberships`)

La tabla separa la **identidad** del usuario de su **pertenencia** al tenant:

- Un usuario puede tener membresías en múltiples panaderías (multi-tenant futuro)
- Al hacer login, `_enriquecer_con_membresia()` sobrescribe `rol`, `panaderia_id` y `sede_id` con los valores de la membresía activa
- Si un usuario no tiene membresía activa → login denegado (aunque la contraseña sea correcta)

### Suscripciones y suspensión

| Condición | Código HTTP | Quién puede acceder |
|-----------|:-----------:|---------------------|
| Tenant `estado_operativo != 'activa\|prueba'` | 503 | Solo `platform_superadmin` |
| Suscripción `is_expired` | 402 | Solo `platform_superadmin` |
| Membresía inactiva | Login denegado | — |

---

## 10. Autenticación y sesiones

### Ciclo completo de sesión

```
1. POST /login
   → Verificación de credenciales (bcrypt para password / hash para PIN)
   → _enriquecer_con_membresia()  ← hace membership_id autoritativo
   → session["usuario"] = {
       id, nombre, rol, panaderia_id, sede_id,
       membership_id, terminal_id?,
       must_change_password
     }
   → session["_login_ts"] = now
   → session["_last_activity_ts"] = now
   → session["_csrf_token"] = secrets.token_hex(32)

2. Cada request autenticada
   → @login_required actualiza _last_activity_ts
   → X-CSRF-Token validado contra session["_csrf_token"]

3. GET /logout
   → session.clear()
```

### Seguridad de la cookie de sesión

| Atributo | Valor |
|----------|-------|
| `HttpOnly` | True |
| `SameSite` | Lax |
| `Secure` | True si `FORCE_HTTPS`, `COOKIE_SECURE`, Railway o `PREFERRED_URL_SCHEME=https` |
| `Permanent` | True (controlado por `SESSION_LIFETIME_HOURS`) |

---

## 11. Seguridad

### Cabeceras HTTP (after_request)

```
X-Request-Id: <uuid>
X-Content-Type-Options: nosniff
X-Frame-Options: SAMEORIGIN
X-XSS-Protection: 1; mode=block
Referrer-Policy: strict-origin-when-cross-origin
Strict-Transport-Security: max-age=63072000; includeSubDomains  ← solo si HTTPS
```

### CSRF

- Token almacenado en sesión, regenerado en cada login
- Validado en `before_request` para métodos mutantes (`POST, PUT, PATCH, DELETE`)
- El endpoint `/login` está exento (crea la sesión)
- Enviado desde el front en el header `X-CSRF-Token` o el campo oculto `_csrf_token`

### Rate limiting de login

- `login_attempts` tabla con `scope_key = "login:ip:<ip>"`
- Máximo 5 intentos (configurable `MAX_LOGIN_ATTEMPTS`)
- Bloqueo de 5 minutos (configurable `LOGIN_LOCKOUT_MINUTES`)
- Se limpia automáticamente al login exitoso

### Contraseñas y PINs

- Contraseñas: `bcrypt` (werkzeug `generate_password_hash`)
- PINs: hash configurable (no texto plano)
- Mínimo 8 caracteres para contraseñas, 4 para PINs

### Inyección SQL

- Queries usan siempre parámetros posicionales (`?` o `%s`): sin concatenación de strings con datos de usuario
- No hay ORM, pero el patrón de parametrización es consistente

### Secret key

- Requiere `FLASK_SECRET_KEY` en entorno
- Si no se configura, genera `secrets.token_hex(32)` en runtime + emite `warnings.warn`
- En producción Railway, la env var debe estar configurada explícitamente

---

## 12. Algoritmo de pronóstico

### Modelo

El pronóstico usa un **promedio móvil ponderado** combinando tres señales:

```
produccion_sugerida = (
    0.50 * promedio_mismo_dia_semana   +  # histórico del mismo día (lunes, martes…)
    0.30 * promedio_ultimos_7_dias     +  # tendencia reciente
    0.20 * ajuste_tendencia            # "subiendo" x1.08 | "bajando" x0.95 | "estable" x1.00
) × factor_dia_especial               # multiplicador si es día especial
  × (1 + BUFFER_SEGURIDAD)            # +10% de colchón
```

### Calidad del pronóstico (`nivel_calidad` 0–6)

| Nivel | Condición |
|-------|-----------|
| 0 | Sin historial |
| 1–2 | < 7 días de datos |
| 3–4 | 7–30 días |
| 5–6 | > 30 días con consistencia |

### Detección de outliers

- Registros con desviación > 2.5σ de la media se excluyen del cálculo (evita que días atípicos sesguen el modelo)

### Resultado devuelto (`ResultadoPronostico`)

```python
@dataclass
class ResultadoPronostico:
    producto: str
    produccion_sugerida: int
    venta_estimada: int
    modelo_usado: str          # "promedio_ponderado" | "valor_base" | etc.
    promedio_ventas: float
    dias_historial: int
    nivel_calidad: float       # 0.0 – 6.0
    estado: str                # "bien" | "alerta" | "problema"
    mensaje: str
    confianza: str             # "poca" | "media" | "buena"
    detalles: dict             # breakdown por componente
```

---

## 13. Despliegue e infraestructura

### Docker Compose (producción)

```
Browser → Nginx (:80) → Gunicorn (:5000) → Flask → PostgreSQL (externo)
```

| Servicio | Imagen | Puerto | Health check |
|----------|--------|--------|--------------|
| `web` | python:3.11-slim | 5000 (interno) | `GET /health` |
| `nginx` | nginx:1.27-alpine | 80 (externo) | `wget /health` |

### Variables de entorno críticas

| Variable | Descripción | Requerida |
|----------|-------------|-----------|
| `FLASK_SECRET_KEY` | Firma de sesiones | **Sí** (prod) |
| `DATABASE_URL` | `postgresql://...` | **Sí** (prod) |
| `SESSION_LIFETIME_HOURS` | Duración de sesión (default: 8) | No |
| `GUNICORN_WORKERS` | Workers (default: min(cpu*2+1, 4)) | No |
| `FORCE_HTTPS` / `COOKIE_SECURE` | Activa HTTPS cookie | No |
| `RAILWAY_ENVIRONMENT` | Detecta Railway automáticamente | No |
| `MAX_LOGIN_ATTEMPTS` | (default: 5) | No |
| `LOGIN_LOCKOUT_MINUTES` | (default: 5) | No |

### Backup automático

- APScheduler corre dentro del proceso Flask
- Job: `_backup_diario` a las **23:00** cada día
- Retención: 30 días (configurable), máximo 50 backups
- SQLite: `sqlite3.connect().backup()` nativo
- PostgreSQL: `pg_dump` externo (requiere `postgresql-client` en el contenedor)

### Migraciones de schema

No hay framework de migraciones. El sistema usa un patrón idempotente propio:

```python
def inicializar_base_de_datos():
    # 1. CREATE TABLE IF NOT EXISTS (todas las tablas)
    # 2. Migraciones en orden:
    _migrar_plataforma_base(conn)    # columnas legacy
    _migrar_jornada(conn)            # jornada operativa
    _migrar_constraints_multitenant(conn)  # Fase 0: UNIQUE por tenant
    _migrar_fase1(conn)              # estado_operativo, created_by
    _migrar_fase2(conn)              # tenant_memberships
    _migrar_fase3(conn)              # tenant_subscriptions
    _migrar_fase5(conn)              # terminales
    # + índices complementarios
```

Cada `_migrar_*` usa `_ejecutar_migracion_tolerante()` que ignora `OperationalError` si la columna/índice ya existe.

---

## 14. Puntos de extensión y deuda técnica

### Fortalezas actuales

- **Aislamiento multi-tenant robusto**: `panaderia_id`/`sede_id` en todas las tablas, con scope automático por hilo
- **Contexto de request bien definido**: dataclasses `frozen=True` que pasan por `g` sin mutaciones accidentales
- **Autenticación en dos niveles**: contraseña larga para admins, PIN para operativos
- **Trazabilidad completa**: `audit_log` + `request_id` en cada respuesta
- **Seguridad HTTP correcta**: CSRF, cabeceras, cookie segura, rate limiting
- **Portabilidad de BD**: funciona con SQLite (dev) y PostgreSQL (prod) sin cambiar código de negocio
- **Pronóstico funcional**: modelo liviano con datos reales, sin dependencias de ML pesadas

### Deuda técnica identificada

| Área | Problema | Impacto |
|------|----------|---------|
| **app.py monolítico** | >4000 líneas en un solo archivo. Rutas de caja, pedidos, inventario, producción, plataforma y más conviven sin separación en blueprints | Mantenimiento difícil a medida que crece |
| **database.py monolítico** | ~3800 líneas. Sin separación por dominio (ventas, inventario, pedidos, etc.) | Dificulta tests unitarios y ownership |
| **Sin tests automáticos** | No hay suite de tests (`pytest`). Las migraciones y reglas de negocio solo se validan en runtime | Riesgo de regresiones |
| **APScheduler en proceso** | El backup automático corre dentro del worker de Gunicorn. Con múltiples workers, puede ejecutarse N veces | Con `preload_app=True` y forks puede ser problemático en prod |
| **Jornada vs membresía** | `jornada_activa` en la tabla `usuarios` es un flag global, no por sede/terminal. Puede generar colisiones si un cajero tiene membresías en múltiples sedes | Inconsistencia con el modelo de membresías |
| **No hay tenant_admin panel** | El `tenant_admin` usa las mismas vistas del `panadero`. No tiene panel propio de gestión (sedes, membresías, invitaciones) | Funcionalidad SaaS incompleta |
| **Sin invalidación de sesión server-side** | Al desactivar un usuario o membresía, su sesión activa sigue válida hasta el timeout natural (8h) | Riesgo de acceso post-revocación |
| **Seeding acoplado a migración** | Los datos iniciales (CAJA-01 por sede, categorías, productos base) se insertan dentro de `inicializar_base_de_datos`. En una arquitectura de fixtures separados sería más limpio | Menor riesgo, más cosmético |
| **Sin paginación en listados** | Las APIs de ventas, historial y pedidos devuelven todos los registros (solo con `LIMIT`). Sin paginación estándar `page/size` | Performance en bases de datos grandes |
| **ECharts desde CDN** | Dependencia de red para las gráficas. Si el CDN falla y el fallback también, las gráficas no cargan | Disponibilidad en entornos sin internet |

### Próximos pasos recomendados

1. **Refactor en blueprints**: extraer `cajero`, `mesero`, `panadero`, `plataforma` como blueprints separados registrados en `app.py`
2. **Tests**: `pytest` con fixtures SQLite en memoria para los dominios críticos (ventas, pronóstico, membresías)
3. **Panel tenant_admin**: gestión de sedes, invitación de usuarios, view de su propia suscripción
4. **Invalidación de sesión server-side**: tabla `sesiones_invalidas` o Redis con `SESSION_SECRET` rotable
5. **Celery / APScheduler externo**: mover el backup automático fuera del proceso Flask para evitar ejecuciones duplicadas en multi-worker
6. **Paginación estándar**: `?page=1&size=50` en todos los listados de la API

---

*Documento generado con base en el código fuente real. Para mantenerlo actualizado, revisar cada vez que se agreguen tablas, rutas o reglas de negocio nuevas.*
