from pydantic import BaseModel
from typing import List, Optional
from pydantic import BaseModel
from pydantic import Field


class CassiaTechSchema(BaseModel):
    tech_name: str = Field(..., max_length=100,
                           example="Servicio de videovigilancia")
    tech_description: str = Field(..., max_length=100,
                                  example="")
    service_id: int = Field(..., example="1")
    cassia_criticality_id: Optional[int] = Field(...,
                                                 example="1")
    sla_hours: float = Field(..., example=32)


""" class AutoActionUpdateSchema(CassiaTechnologySchema):
    action_auto_id: int = Field(..., example="1") """
