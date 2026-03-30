"""
gunicorn.conf.py
----------------
Production Gunicorn settings for the bakery app.
"""

import multiprocessing
import os

_default_workers = min(multiprocessing.cpu_count() * 2 + 1, 4)
_default_port = os.environ.get("PORT", "5000")

# Workers
workers = int(os.environ.get("GUNICORN_WORKERS", os.environ.get("WEB_CONCURRENCY", _default_workers)))
worker_class = os.environ.get("GUNICORN_WORKER_CLASS", "sync")
threads = int(os.environ.get("GUNICORN_THREADS", "1"))

# Binding
bind = os.environ.get("GUNICORN_BIND", f"0.0.0.0:{_default_port}")

# Timeouts
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "60"))
keepalive = int(os.environ.get("GUNICORN_KEEPALIVE", "2"))
graceful_timeout = int(os.environ.get("GUNICORN_GRACEFUL_TIMEOUT", "30"))
max_requests = int(os.environ.get("GUNICORN_MAX_REQUESTS", "1000"))
max_requests_jitter = int(os.environ.get("GUNICORN_MAX_REQUESTS_JITTER", "100"))

# Logs
accesslog = "-"
errorlog = "-"
loglevel = os.environ.get("GUNICORN_LOG_LEVEL", "info")
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)dus'
capture_output = True
disable_redirect_access_to_syslog = True

# Proxy trust
forwarded_allow_ips = os.environ.get("GUNICORN_FORWARDED_ALLOW_IPS", "*")
proxy_allow_ips = os.environ.get("GUNICORN_PROXY_ALLOW_IPS", "*")

# Preload
preload_app = True
