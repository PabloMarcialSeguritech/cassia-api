from pydantic import BaseModel
from typing import List
from pydantic import BaseModel
from pydantic import Field


class CassiaCriticalitySchema(BaseModel):
    level: int = Field(...,
                       example=1)
    name: str = Field(..., max_length=100,
                      example="Info")
    description: str = Field(..., max_length=255,
                             example="Severidad informativa")


""" class AutoActionUpdateSchema(CassiaTechnologySchema):
    action_auto_id: int = Field(..., example="1") """
