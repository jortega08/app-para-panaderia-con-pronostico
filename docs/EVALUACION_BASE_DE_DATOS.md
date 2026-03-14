# Evaluacion Tecnica: SQLite vs PostgreSQL

## Resumen Ejecutivo

**Recomendacion: Mantener SQLite por ahora.** Migrar a PostgreSQL cuando haya mas de 3-4 dispositivos operando simultaneamente o cuando se necesite acceso remoto desde fuera de la red local.

---

## 1. Situacion Actual

| Aspecto | Valor |
|---------|-------|
| Base de datos | SQLite 3 |
| Archivo | `data/panaderia.db` |
| Tamano tipico | ~100 KB (crecera con historial) |
| Acceso | Via Flask (un proceso) |
| Concurrencia esperada | 1-5 dispositivos en red local |
| Transacciones/dia | ~50-200 pedidos |

## 2. Analisis de SQLite para el Caso de Uso

### Fortalezas de SQLite

- **Sin servidor**: No necesita instalar ni mantener un servidor de base de datos
- **Zero-config**: Funciona inmediatamente, sin configuracion
- **Portable**: Toda la DB es un archivo, facil de respaldar y mover
- **Rendimiento en lectura**: Excelente para volumen bajo-medio
- **Ideal para dispositivo unico**: Perfecto para una PC/laptop de caja

### Limitaciones de SQLite

- **Escritura concurrente**: SQLite usa bloqueo a nivel de archivo. Solo un proceso puede escribir a la vez
- **No soporta conexiones remotas**: Los dispositivos deben conectarse via HTTP (Flask), no directamente a la DB
- **Sin replicacion**: No hay forma nativa de replicar datos a otro servidor

### Impacto en la Operacion

| Escenario | SQLite | Impacto |
|-----------|--------|---------|
| 1 cajero + 1 mesero | OK | Bloqueos minimos, imperceptibles |
| 1 cajero + 2-3 meseros | OK con precaucion | Puede haber esperas de <100ms en escritura |
| 1 cajero + 4+ meseros | Riesgoso | Posibles errores "database is locked" |
| Multiples sucursales | NO | Imposible con SQLite |

## 3. Cuando Migrar a PostgreSQL

Migrar cuando se cumpla **cualquiera** de estas condiciones:

1. Mas de 3-4 tablets/celulares escribiendo pedidos simultaneamente
2. Se necesita acceso desde fuera de la red local (sucursal remota)
3. Se requiere replicacion o alta disponibilidad
4. El volumen supera 1000+ transacciones/dia

## 4. Preparacion para Migracion

El codigo actual esta **preparado** para una migracion futura:

### Lo que facilita la migracion

- Toda la logica de acceso a datos esta centralizada en `data/database.py`
- Las consultas SQL usan sintaxis estandar compatible con PostgreSQL
- Las funciones de la capa de datos retornan diccionarios (no objetos SQLite)
- No se usan features exclusivos de SQLite (excepto `GENERATED ALWAYS AS` en `sobrante`)

### Pasos para migrar (cuando sea necesario)

1. Instalar PostgreSQL en el servidor
2. Reemplazar `sqlite3.connect()` por `psycopg2.connect()` en `get_connection()`
3. Ajustar `AUTOINCREMENT` → `SERIAL`
4. Ajustar columna `sobrante GENERATED` → trigger o vista
5. Configurar cadena de conexion (host, puerto, usuario, password)
6. Migrar datos existentes con script de exportacion/importacion

### Estimacion de esfuerzo

- Tiempo de migracion del codigo: ~2-4 horas
- Migracion de datos: ~30 minutos (script automatizado)
- Testing: ~2 horas

## 5. Mitigaciones Actuales

Para reducir problemas de concurrencia con SQLite mientras no se migre:

1. **WAL mode**: Activar `PRAGMA journal_mode=WAL` para permitir lecturas concurrentes mientras se escribe
2. **Timeouts**: Configurar `timeout=10` en `sqlite3.connect()` para esperar en vez de fallar
3. **Transacciones cortas**: Las funciones actuales ya usan transacciones cortas (bien)
4. **Flask como proxy**: Todos los dispositivos acceden via HTTP, Flask serializa las escrituras

## 6. Conclusion

| Criterio | SQLite | PostgreSQL |
|----------|--------|------------|
| Facilidad de setup | Excelente | Requiere instalacion |
| Backup | Copiar archivo | pg_dump/pg_restore |
| Concurrencia | Baja (1-3 writers) | Alta (cientos) |
| Rendimiento lectura | Excelente | Excelente |
| Rendimiento escritura concurrente | Limitado | Excelente |
| Costo | $0 | $0 (open source) |
| Mantenimiento | Ninguno | Bajo-medio |

**Decision final**: SQLite es suficiente para la operacion actual de una panaderia con 1 caja y 2-3 meseros. Se recomienda aplicar las mitigaciones (WAL mode, timeouts) y monitorear. Migrar solo cuando las limitaciones se vuelvan tangibles.
