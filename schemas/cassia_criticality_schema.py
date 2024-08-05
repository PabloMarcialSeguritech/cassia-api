from pydantic import BaseModel
from typing import List, Optional
from pydantic import BaseModel
from pydantic import Field


class CassiaCriticalitySchema(BaseModel):
    level: int = Field(...,
                       example=1)
    name: str = Field(..., max_length=100,
                      example="Info")
    description: Optional[str] = Field(max_length=255,
                                       example="Severidad informativa")
