from __future__ import annotations

from flask import jsonify, request


def wants_json_response() -> bool:
    accept = request.headers.get("Accept", "")
    return (
        request.path.startswith("/api/")
        or request.is_json
        or "application/json" in accept
        or request.headers.get("X-Requested-With", "").lower() == "xmlhttprequest"
    )


def json_ok(data=None, status: int = 200, meta: dict | None = None):
    return jsonify({
        "ok": True,
        "data": data if data is not None else {},
        "error": None,
        "meta": meta or {},
    }), status


def json_error(message: str, status: int = 400, *, code: str = "", data=None, meta: dict | None = None):
    payload_meta = dict(meta or {})
    if code:
        payload_meta["code"] = str(code)
    return jsonify({
        "ok": False,
        "data": data if data is not None else {},
        "error": str(message or "Error"),
        "meta": payload_meta,
    }), status
