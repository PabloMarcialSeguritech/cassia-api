from pydantic import BaseModel
from typing import Optional
# Schema para los grupos vinculados


class TelegramGroup(BaseModel):
    id: int
    name: str
    groupid: int
    description: Optional[str]  # Permitir que sea opcional o None


# Schema para el endpoint de vinculación del grupo de Telegram
class LinkTelegramGroupRequest(BaseModel):
    name: str
    groupid: int
    mensaje: str
