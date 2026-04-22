# Runbook: Backup y Restore — App Panadería

Versión: 2026-04-18  
Aplica a: despliegues SQLite (local) y PostgreSQL (Railway / producción)

---

## 1. Backup manual desde la UI (ambos motores)

1. Ir a **Panadero → Respaldos** (`/panadero/backups`).
2. Hacer clic en **Crear Respaldo Ahora**.
3. Escribir una nota descriptiva (p. ej. `antes de actualizar inventario`).
4. El sistema detecta el motor activo:
   - **SQLite** → copia `.db` via API nativo.
   - **PostgreSQL** → ejecuta `pg_dump` y guarda un `.sql`.
5. El backup queda en `backups/panaderia_backup_YYYYMMDD_HHMMSS.[db|sql]`.

---

## 2. Backup manual via línea de comandos

### 2a. SQLite

```bash
# Copiar el archivo de base de datos con el servidor detenido o con WAL flush
cp data/panaderia.db backups/panaderia_backup_manual_$(date +%Y%m%d_%H%M%S).db
```

### 2b. PostgreSQL

```bash
# Exportar a SQL plain-text (recomendado para portabilidad)
pg_dump --no-password --format=plain --no-acl --no-owner \
  "$DATABASE_URL" \
  > backups/panaderia_backup_$(date +%Y%m%d_%H%M%S).sql

# Exportar a formato custom (más comprimido, requiere pg_restore)
pg_dump --no-password --format=custom --no-acl --no-owner \
  "$DATABASE_URL" \
  > backups/panaderia_backup_$(date +%Y%m%d_%H%M%S).dump
```

> **En Railway**: usa el panel de PostgreSQL → Backups, o configura
> `pg_dump` en un servicio cron externo apuntando al `DATABASE_URL` de Railway.

---

## 3. Restore — SQLite

```bash
# 1. Detener el servidor (recomendado para evitar escrituras concurrentes)
#    En Railway: hacer Deploy → Stop temporal o usar modo maintenance.

# 2. Crear backup de seguridad del estado actual
cp data/panaderia.db data/panaderia_BEFORE_RESTORE_$(date +%Y%m%d_%H%M%S).db

# 3. Copiar el backup como base activa
cp backups/panaderia_backup_20260401_120000.db data/panaderia.db

# 4. Reiniciar el servidor
```

Alternativamente, desde la UI (solo SQLite):
1. Ir a **Panadero → Respaldos**.
2. Hacer clic en **Restaurar** junto al backup deseado.
3. Confirmar — se crea un auto-backup previo antes de restaurar.
4. Reiniciar la aplicación manualmente si el ORM tiene caché en memoria.

---

## 4. Restore — PostgreSQL (producción multi-tenant)

> La restauración desde la UI está **deshabilitada** para PostgreSQL
> porque afecta a todos los tenants simultáneamente.
> Sigue estos pasos de forma controlada.

### Prerequisitos

- `psql` y `pg_restore` instalados (`apt install postgresql-client`).
- Acceso a `$DATABASE_URL` con privilegios de escritura.
- Ventana de mantenimiento coordinada (aviso a clientes activos).

### 4a. Restore desde archivo SQL plain-text

```bash
# 1. Poner la app en modo mantenimiento
#    (Railway: despliega una versión con MAINTENANCE_MODE=true o detén el servicio)

# 2. Crear backup del estado actual ANTES de restaurar
pg_dump --no-password --format=plain --no-acl --no-owner \
  "$DATABASE_URL" \
  > backups/panaderia_BEFORE_RESTORE_$(date +%Y%m%d_%H%M%S).sql

# 3. Restaurar desde el archivo de backup
psql "$DATABASE_URL" < backups/panaderia_backup_20260401_120000.sql

# 4. Verificar integridad básica
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM ventas;"
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM usuarios;"

# 5. Reiniciar el servicio de la app
```

### 4b. Restore desde archivo .dump (formato custom)

```bash
# Primero vaciar las tablas o recrear el schema
pg_restore --no-password --no-acl --no-owner \
  --clean --if-exists \
  --dbname "$DATABASE_URL" \
  backups/panaderia_backup_20260401_120000.dump
```

---

## 5. Cambio de servidor (migración completa)

```bash
# SERVIDOR ORIGEN
pg_dump --no-password --format=custom --no-acl --no-owner \
  "$OLD_DATABASE_URL" \
  > panaderia_migration_$(date +%Y%m%d).dump

# SERVIDOR DESTINO
# Crear base de datos vacía si no existe
psql "$NEW_DATABASE_URL" -c "CREATE DATABASE panaderia;"  # si aplica

pg_restore --no-password --no-acl --no-owner \
  --clean --if-exists \
  --dbname "$NEW_DATABASE_URL" \
  panaderia_migration_$(date +%Y%m%d).dump

# Actualizar variable de entorno
export DATABASE_URL="$NEW_DATABASE_URL"
# Reiniciar la app
```

---

## 6. Cambio de dominio

El cambio de dominio **no requiere** restore de base de datos.

1. Actualizar el registro DNS apuntando al nuevo IP / hostname.
2. Actualizar `ALLOWED_HOSTS` o configuración de nginx si aplica.
3. Actualizar `SESSION_COOKIE_DOMAIN` en las variables de entorno si está fijado.
4. Reiniciar la app.
5. Verificar que HTTPS funciona antes de publicar el nuevo dominio.

---

## 7. Verificación post-restore

Ejecutar los siguientes checks tras cualquier restauración:

```bash
# Verificar tablas principales
psql "$DATABASE_URL" <<'SQL'
SELECT 'ventas'     AS tabla, COUNT(*) AS filas FROM ventas
UNION ALL
SELECT 'pedidos',                COUNT(*)        FROM pedidos
UNION ALL
SELECT 'usuarios',               COUNT(*)        FROM usuarios
UNION ALL
SELECT 'productos',              COUNT(*)        FROM productos
UNION ALL
SELECT 'registros_diarios',      COUNT(*)        FROM registros_diarios;
SQL

# Verificar que la app arranca sin errores
curl -sf http://localhost:5000/health && echo "OK" || echo "FALLA"
curl -sf http://localhost:5000/ready  && echo "OK" || echo "FALLA"
```

---

## 8. Backups automáticos (programados)

La app programa un backup automático diario a las **02:00** (hora del servidor)
siempre que el motor sea SQLite. Para PostgreSQL en Railway, configura un
**cron job externo** (GitHub Actions, Railway Cron Service, etc.) que ejecute:

```bash
pg_dump --no-password --format=custom --no-acl --no-owner \
  "$DATABASE_URL" \
  > "/backups/auto_$(date +%Y%m%d_%H%M%S).dump"
# Copiar a S3 / GCS / Backblaze B2 para retención offsite
```

---

## 9. Retención recomendada

| Tipo | Retención |
|------|-----------|
| Backups diarios automáticos | 30 días |
| Backups manuales importantes | Indefinido (etiquetar con nota) |
| Auto-backup antes de restore | 7 días mínimo |
| Backups pre-migración | 90 días mínimo |

---

*Última revisión: 2026-04-18*
