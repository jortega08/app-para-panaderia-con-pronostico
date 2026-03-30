"""
gunicorn.conf.py
----------------
Configuración de Gunicorn para producción.
Uso: gunicorn -c gunicorn.conf.py app:app
"""

import multiprocessing
import os

# ── Workers ──────────────────────────────────────────────────────────────────
_default_workers = min(multiprocessing.cpu_count() * 2 + 1, 4)
workers = int(os.environ.get("GUNICORN_WORKERS", _default_workers))
worker_class = "sync"
threads = 1

# ── Binding ───────────────────────────────────────────────────────────────────
# Railway inyecta PORT; localmente cae en 5000
_port = os.environ.get("PORT", "5000")
bind = os.environ.get("GUNICORN_BIND", f"0.0.0.0:{_port}")

# ── Timeouts ─────────────────────────────────────────────────────────────────
timeout = 120
keepalive = 5
graceful_timeout = 30

# ── Logs ──────────────────────────────────────────────────────────────────────
accesslog = "-"        # stdout
errorlog = "-"         # stderr
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info")
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)sµs'

# ── Seguridad ──────────────────────────────────────────────────────────────────
forwarded_allow_ips = "*"
proxy_allow_ips = "*"

# ── Preload ───────────────────────────────────────────────────────────────────
preload_app = os.environ.get("GUNICORN_PRELOAD", "true").lower() == "true"