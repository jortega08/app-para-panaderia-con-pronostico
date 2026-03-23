# Panaderia Richs

Sistema web para ventas, produccion, inventario y pronostico de una panaderia.

## Despliegue

La aplicacion ya no expone el puerto de Flask al exterior. El acceso remoto pasa
por Nginx y el backend queda solo en la red interna del contenedor.

### Produccion remota con PostgreSQL externo

1. Define como minimo estas variables en un archivo propio, por ejemplo
   `.env.production`, o exportalas en tu shell:

```bash
FLASK_SECRET_KEY=una_clave_larga_y_aleatoria
DATABASE_URL=postgresql://usuario:password@host:5432/panaderia
NGINX_SERVER_NAME=panel.tu-dominio.com
GUNICORN_WORKERS=3
```

Puedes partir de [.env.production.example](./.env.production.example) para
produccion o de [.env.demo.example](./.env.demo.example) para la demo local.

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

## Documentacion

Consulta el manual de usuario completo en:

- [MANUAL_USUARIO.md](C:/Users/mondr/OneDrive/Documentos/VisualStudioCode/PuntoVentaPanaderia/app-para-panaderia-con-pronostico/MANUAL_USUARIO.md)
