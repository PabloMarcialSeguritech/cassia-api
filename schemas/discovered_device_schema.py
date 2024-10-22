from pydantic import BaseModel, validator
from typing import Optional


class DiscoveredDevice(BaseModel):
    ip: str
    mac_address: str
    proxyId: Optional[int]
    proxyName: Optional[str]
    name: Optional[str]
    host: Optional[str]


class ProxyRequest(BaseModel):
    proxyId: Optional[int] = None  # Si es None, se hace descubrimiento local
    segment: str  # Segmento de red (CIDR o rango)

    @validator("segment")
    def validate_segment(cls, value):
        if "-" not in value and "/" not in value:
            raise ValueError(
                "El segmento debe ser en formato CIDR o rango (ej: '192.168.1.1-192.168.1.50')")
        return value
