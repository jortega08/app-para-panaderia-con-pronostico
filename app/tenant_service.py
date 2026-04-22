"""
tenant_service.py
-----------------
Resolución y validación del tenant activo (Fases 1-3).

No importa Flask directamente para facilitar tests unitarios; el abort() lo
llama quien corresponda (normalmente before_request en app.py).
"""

from __future__ import annotations

from app.context import BrandContext, SedeContext, SubscriptionContext, TenantContext, TerminalContext
from data.database import (
    PLAN_LIMITS,
    actualizar_last_seen_terminal,
    obtener_branding_panaderia,
    obtener_panaderia_por_dominio,
    obtener_panaderia_por_id,
    obtener_panaderia_por_slug,
    obtener_panaderia_principal,
    obtener_sede_por_codigo,
    obtener_sede_por_id,
    obtener_sede_por_panaderia_y_slug,
    obtener_sede_principal,
    obtener_suscripcion_panaderia,
    obtener_terminal_por_codigo,
    obtener_terminal_por_id,
    verificar_limite_productos,
    verificar_limite_sedes,
    verificar_limite_usuarios,
)


def _panaderia_to_context(row: dict) -> TenantContext:
    return TenantContext(
        id=int(row["id"]),
        slug=str(row.get("slug", "") or ""),
        nombre=str(row.get("nombre", "") or ""),
        activa=bool(int(row.get("activa", 1) or 1)),
        estado_operativo=str(row.get("estado_operativo", "activa") or "activa"),
    )


def _sede_to_context(row: dict, panaderia_id: int | None = None) -> SedeContext:
    return SedeContext(
        id=int(row["id"]),
        panaderia_id=int(row.get("panaderia_id") or panaderia_id or 0),
        slug=str(row.get("slug", "") or ""),
        nombre=str(row.get("nombre", "") or ""),
        codigo=str(row.get("codigo", "") or ""),
        activa=bool(int(row.get("activa", 1) or 1)),
    )


def _branding_to_context(branding: dict) -> BrandContext:
    return BrandContext(
        panaderia_id=branding.get("panaderia_id"),
        brand_name=str(branding.get("brand_name", "RICHS") or "RICHS"),
        legal_name=str(branding.get("legal_name", "") or ""),
        tagline=str(branding.get("tagline", "Panaderia artesanal") or "Panaderia artesanal"),
        support_label=str(branding.get("support_label", "Delicias que nutren") or "Delicias que nutren"),
        logo_path=str(branding.get("logo_path", "brand/richs-logo.svg") or "brand/richs-logo.svg"),
        favicon_path=str(branding.get("favicon_path", "brand/richs-logo.svg") or "brand/richs-logo.svg"),
        primary_color=str(branding.get("primary_color", "#8b5513") or "#8b5513"),
        secondary_color=str(branding.get("secondary_color", "#d4722a") or "#d4722a"),
        accent_color=str(branding.get("accent_color", "#e0a142") or "#e0a142"),
    )


def _terminal_to_context(row: dict) -> TerminalContext:
    return TerminalContext(
        id=int(row["id"]),
        panaderia_id=int(row.get("panaderia_id") or 0),
        sede_id=int(row.get("sede_id") or 0),
        nombre=str(row.get("nombre", "") or ""),
        codigo=str(row.get("codigo", "") or ""),
        tipo=str(row.get("tipo", "caja") or "caja"),
        activa=bool(int(row.get("activa", 1) or 1)),
    )


def _suscripcion_to_context(row: dict | None, panaderia_id: int | None = None) -> SubscriptionContext:
    if row is None:
        limites = PLAN_LIMITS["free"]
        return SubscriptionContext(
            panaderia_id=panaderia_id,
            plan="free",
            estado="activa",
            max_sedes=limites["max_sedes"],
            max_usuarios=limites["max_usuarios"],
            max_productos=limites["max_productos"],
        )
    return SubscriptionContext(
        panaderia_id=int(row.get("panaderia_id") or panaderia_id or 0),
        plan=str(row.get("plan", "free") or "free"),
        estado=str(row.get("estado", "activa") or "activa"),
        fecha_inicio=str(row.get("fecha_inicio", "") or ""),
        fecha_vencimiento=row.get("fecha_vencimiento") or None,
        max_sedes=int(row.get("max_sedes") or PLAN_LIMITS["free"]["max_sedes"]),
        max_usuarios=int(row.get("max_usuarios") or PLAN_LIMITS["free"]["max_usuarios"]),
        max_productos=int(row.get("max_productos") or PLAN_LIMITS["free"]["max_productos"]),
    )


class TenantService:
    """Resolución y validación del contexto de tenant/sede/suscripción."""

    # ── Resolución de panadería ───────────────────────────────────────────────

    @staticmethod
    def resolve_by_id(panaderia_id: int | None) -> TenantContext | None:
        if not panaderia_id:
            return None
        row = obtener_panaderia_por_id(int(panaderia_id))
        return _panaderia_to_context(row) if row else None

    @staticmethod
    def resolve_by_slug(slug: str) -> TenantContext | None:
        row = obtener_panaderia_por_slug(slug)
        return _panaderia_to_context(row) if row else None

    @staticmethod
    def resolve_by_domain(domain: str) -> TenantContext | None:
        row = obtener_panaderia_por_dominio(domain)
        return _panaderia_to_context(row) if row else None

    @staticmethod
    def resolve_current_panaderia() -> TenantContext:
        row = obtener_panaderia_principal()
        return _panaderia_to_context(row)

    @staticmethod
    def resolve_default() -> TenantContext:
        """Compatibilidad interna: la instalación actual usa una sola panadería."""
        return TenantService.resolve_current_panaderia()

    # ── Resolución de sede ────────────────────────────────────────────────────

    @staticmethod
    def resolve_sede_by_id(sede_id: int | None) -> SedeContext | None:
        if not sede_id:
            return None
        row = obtener_sede_por_id(int(sede_id))
        return _sede_to_context(row) if row else None

    @staticmethod
    def resolve_sede_by_codigo(panaderia_id: int, codigo: str) -> SedeContext | None:
        row = obtener_sede_por_codigo(panaderia_id, codigo)
        return _sede_to_context(row) if row else None

    @staticmethod
    def resolve_sede_by_slug(panaderia_id: int, slug: str) -> SedeContext | None:
        row = obtener_sede_por_panaderia_y_slug(panaderia_id, slug)
        return _sede_to_context(row) if row else None

    @staticmethod
    def resolve_sede_default(panaderia_id: int | None = None) -> SedeContext:
        row = obtener_sede_principal(panaderia_id)
        return _sede_to_context(row)

    # ── Resolución de suscripción ─────────────────────────────────────────────

    @staticmethod
    def get_subscription(panaderia_id: int | None) -> SubscriptionContext:
        if not panaderia_id:
            return _suscripcion_to_context(None)
        row = obtener_suscripcion_panaderia(int(panaderia_id))
        return _suscripcion_to_context(row, panaderia_id)

    # ── Validación de estado ──────────────────────────────────────────────────

    @staticmethod
    def assert_tenant_active(tenant: TenantContext) -> None:
        """Lanza TenantSuspendedError si el tenant no puede operar."""
        if not tenant.available:
            raise TenantNotAvailableError("Tenant no resuelto")
        if not tenant.is_active:
            raise TenantSuspendedError(
                f"La panaderia '{tenant.nombre}' no puede operar "
                f"(estado: {tenant.estado_operativo})"
            )

    @staticmethod
    def assert_subscription_active(subscription: SubscriptionContext) -> None:
        """Lanza SubscriptionExpiredError si la suscripción no está vigente."""
        if not subscription.is_active:
            raise SubscriptionExpiredError(
                f"Suscripción '{subscription.plan}' no vigente "
                f"(estado: {subscription.estado})"
            )

    # ── Límites de plan ───────────────────────────────────────────────────────

    @staticmethod
    def check_limite_usuarios(panaderia_id: int) -> dict:
        return verificar_limite_usuarios(panaderia_id)

    @staticmethod
    def check_limite_sedes(panaderia_id: int) -> dict:
        return verificar_limite_sedes(panaderia_id)

    @staticmethod
    def check_limite_productos(panaderia_id: int) -> dict:
        return verificar_limite_productos(panaderia_id)

    # ── Resolución de terminal ────────────────────────────────────────────────

    @staticmethod
    def resolve_terminal_by_id(terminal_id: int | None) -> TerminalContext | None:
        if not terminal_id:
            return None
        row = obtener_terminal_por_id(int(terminal_id))
        return _terminal_to_context(row) if row else None

    @staticmethod
    def resolve_terminal_by_codigo(sede_id: int, codigo: str) -> TerminalContext | None:
        if not sede_id or not codigo:
            return None
        row = obtener_terminal_por_codigo(int(sede_id), codigo)
        return _terminal_to_context(row) if row else None

    @staticmethod
    def touch_terminal(terminal_id: int) -> None:
        """Actualiza last_seen_at del terminal activo."""
        try:
            actualizar_last_seen_terminal(terminal_id)
        except Exception:
            pass

    # ── Branding ──────────────────────────────────────────────────────────────

    @staticmethod
    def get_branding(tenant_id: int | None) -> BrandContext:
        branding = obtener_branding_panaderia(tenant_id)
        return _branding_to_context(branding)


class TenantNotAvailableError(Exception):
    pass


class TenantSuspendedError(Exception):
    pass


class SubscriptionExpiredError(Exception):
    pass
