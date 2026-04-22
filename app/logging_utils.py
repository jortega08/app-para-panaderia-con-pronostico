from __future__ import annotations

import logging
import uuid


def generate_request_id() -> str:
    return uuid.uuid4().hex


def configure_app_logging(app) -> None:
    if getattr(app, "_codex_logging_configured", False):
        return

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    for handler in root_logger.handlers:
        handler.setFormatter(formatter)

    if not root_logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    app.logger.setLevel(logging.INFO)
    app._codex_logging_configured = True
