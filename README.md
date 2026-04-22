# Panaderia Richs

Sistema web para ventas, produccion, inventario y pronostico de una panaderia.

## Despliegue

La aplicacion ya no expone el puerto de Flask al exterior. El acceso remoto pasa
por Nginx y el backend queda solo en la red interna del contenedor.

### Railway con PostgreSQL

La app ya queda preparada para Railway con `DATABASE_URL` de PostgreSQL.

1. Crea un proyecto nuevo en Railway y agrega dos servicios:
   `web` para esta app y `PostgreSQL` para la base de datos.
2. Conecta este repositorio al servicio `web`.
3. Define al menos estas variables en Railway:

```bash
FLASK_SECRET_KEY=una_clave_larga_y_aleatoria
DATABASE_URL=${{Postgres.DATABASE_URL}}
GUNICORN_WORKERS=2
DB_INIT_MAX_RETRIES=12
DB_INIT_RETRY_DELAY=2.5
```

4. Railway detectará el `Dockerfile` y aplicará el `railway.toml` del repo.
5. El healthcheck queda apuntando a `/ready`, que valida también la conexión a la base.

Notas:

- Railway inyecta `PORT`; la app y Gunicorn ya lo consumen automáticamente.
- Los backups/restores dentro de la app quedan desactivados para PostgreSQL.
  Usa snapshots o backups del propio Railway/PostgreSQL.
- Si quieres un dominio público, genéralo desde la sección `Networking` del servicio.

### Produccion remota con PostgreSQL externo

1. Define como minimo estas variables en un archivo propio, por ejemplo
   `.env.production`, o exportalas en tu shell:

```bash
FLASK_SECRET_KEY=una_clave_larga_y_aleatoria
DATABASE_URL=postgresql://usuario:password@host:5432/panaderia
NGINX_SERVER_NAME=panel.tu-dominio.com
GUNICORN_WORKERS=3
```

2. Levanta el stack:

```bash
docker compose --env-file .env.production up -d --build
```

Este modo usa el `docker-compose.yml` principal, `nginx.conf` como reverse proxy
y una base PostgreSQL administrada o externa.

Si vas a publicar el sistema, usa un dominio real en `NGINX_SERVER_NAME` para
que el proxy responda correctamente fuera de la red local.

El hostname `NGINX_SERVER_NAME` lo consume el servicio `nginx`. El backend
Flask ya queda preparado para trabajar detras de un proxy real con cabeceras
`X-Forwarded-*`.

No reutilices el `.env` de desarrollo del repositorio para produccion. Exporta
variables propias o usa un archivo dedicado como `.env.production`.

Importante: este stack publica HTTP en `:80`, pero la app marca las cookies
como seguras en produccion. Para un despliegue real debes poner HTTPS delante
del stack, por ejemplo con un balanceador, reverse proxy o tunel administrado
que termine TLS antes de llegar a Nginx.

### Demo local con PostgreSQL

```bash
POSTGRES_PASSWORD=una_clave_demo \
docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d --build
```

Este modo levanta PostgreSQL dentro de Docker para una demo reproducible.

### Desarrollo local rapido

```bash
python app.py
```

## Consideraciones

- `nginx.conf` es el archivo activo para el proxy.
- `nginix.conf` queda solo por compatibilidad historica.
- `5000` queda expuesto solo dentro de la red Docker; no debe publicarse al
  exterior.
- `/health` ahora verifica que el backend y la base respondan; el compose usa
  ese endpoint para healthchecks de `web` y `nginx`.
- Para produccion, usa una `FLASK_SECRET_KEY` propia y una instancia de
  PostgreSQL fuera de la laptop o del Wi-Fi local.
- En PostgreSQL, los backups/restores ya no se hacen desde la app. Usa
  `pg_dump`, snapshots del proveedor o un runbook externo de respaldo.

## Configuracion por cliente

Cada instalacion debe quedar parametrizada para una sola panaderia. En esta
fase la separacion queda asi:

- Dominio del cliente: `NGINX_SERVER_NAME`.
- Modo de ejecucion: `FLASK_ENV` para desarrollo y produccion.
- Clave de sesion: `FLASK_SECRET_KEY`.
- Conexion a la base del cliente: `DATABASE_URL`.
- Capacidad del proceso web: `GUNICORN_WORKERS`.

La identidad visual y fiscal del cliente ya sale de la panaderia local en la
base de datos, no de una seleccion dinamica de tenant:

- Nombre comercial: `tenant_branding.brand_name`
- Razon social o nombre legal: `tenant_branding.legal_name`
- Logo y favicon: `tenant_branding.logo_path` y `tenant_branding.favicon_path`
- Colores de marca: `tenant_branding.primary_color`,
  `tenant_branding.secondary_color` y `tenant_branding.accent_color`

Para cada despliegue de cliente recomendamos revisar este checklist antes de
salir a produccion:

1. Definir `NGINX_SERVER_NAME` con el dominio final del cliente.
2. Configurar `FLASK_SECRET_KEY` y `DATABASE_URL` propios de esa instalacion.
3. Ajustar los datos de `tenant_branding` para logo, nombre comercial y datos
   fiscales de la panaderia.
4. Mantener aislada la configuracion de correo saliente por cliente cuando se
   habilite el envio de documentos en releases posteriores.
