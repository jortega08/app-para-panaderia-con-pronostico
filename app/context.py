from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date


@dataclass(frozen=True)
class TenantContext:
    id: int | None = None
    slug: str = ""
    nombre: str = ""
    activa: bool = True
    is_platform: bool = False
    estado_operativo: str = "activa"

    @property
    def available(self) -> bool:
        return self.id is not None

    @property
    def is_active(self) -> bool:
        return self.estado_operativo in ("activa", "prueba")

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SedeContext:
    id: int | None = None
    panaderia_id: int | None = None
    slug: str = ""
    nombre: str = ""
    codigo: str = ""
    activa: bool = True

    @property
    def available(self) -> bool:
        return self.id is not None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SubscriptionContext:
    panaderia_id: int | None = None
    plan: str = "free"
    estado: str = "activa"
    fecha_inicio: str = ""
    fecha_vencimiento: str | None = None
    max_sedes: int = 1
    max_usuarios: int = 5
    max_productos: int = 50

    @property
    def is_active(self) -> bool:
        if self.estado not in ("activa", "trial"):
            return False
        if self.fecha_vencimiento:
            try:
                return date.fromisoformat(self.fecha_vencimiento) >= date.today()
            except ValueError:
                pass
        return True

    @property
    def is_expired(self) -> bool:
        return not self.is_active

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class TerminalContext:
    id: int | None = None
    panaderia_id: int | None = None
    sede_id: int | None = None
    nombre: str = ""
    codigo: str = ""
    tipo: str = "caja"
    activa: bool = True

    @property
    def available(self) -> bool:
        return self.id is not None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class BrandContext:
    panaderia_id: int | None = None
    brand_name: str = "RICHS"
    legal_name: str = ""
    tagline: str = "Panaderia artesanal"
    support_label: str = "Delicias que nutren"
    logo_path: str = "brand/richs-logo.svg"
    favicon_path: str = "brand/richs-logo.svg"
    primary_color: str = "#8b5513"
    secondary_color: str = "#d4722a"
    accent_color: str = "#e0a142"

    def to_dict(self) -> dict:
        return asdict(self)
