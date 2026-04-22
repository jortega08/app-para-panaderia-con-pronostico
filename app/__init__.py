from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

_LEGACY_MODULE: ModuleType | None = None


def _load_legacy_module() -> ModuleType:
    global _LEGACY_MODULE
    if _LEGACY_MODULE is not None:
        return _LEGACY_MODULE

    legacy_path = Path(__file__).resolve().parent.parent / "app.py"
    spec = importlib.util.spec_from_file_location("legacy_monolith_app", legacy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el módulo legado desde {legacy_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _LEGACY_MODULE = module
    return module


def create_app():
    module = _load_legacy_module()
    return module.app
