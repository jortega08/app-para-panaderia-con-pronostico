# Recordatorios diarios de encargos

Los recordatorios de encargos se ejecutan cada minuto y revisan los encargos activos cuya `hora_entrega` coincide con la hora actual `HH:MM`.

## Modo recomendado

En produccion con gunicorn o multiples workers, usa un solo proceso externo:

```bash
python jobs_runner.py daemon
```

El job inserta avisos in-app y envia email si SMTP esta configurado. La tabla `encargo_avisos` evita duplicados por `encargo_id`, fecha y canal, incluso si un job corre dos veces.

## Scheduler embebido

Solo activa el scheduler dentro de Flask si tienes exactamente un proceso web ejecutandolo:

```bash
ENABLE_IN_APP_SCHEDULER=1
```

Con multiples workers, deja esa variable apagada y usa el daemon externo.

## SMTP

Variables usadas:

```bash
SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=
SMTP_FROM=
SMTP_USE_TLS=true
```

Si SMTP no esta configurado, el sistema mantiene los avisos in-app y registra un warning unico por proceso.
