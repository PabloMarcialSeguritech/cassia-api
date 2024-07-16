from pydantic import BaseModel
from typing import List, Optional
from pydantic import BaseModel
from pydantic import Field


class CassiaTechServiceSchema(BaseModel):
    service_name: str = Field(..., max_length=100,
                              example="Servicio de videovigilancia")
    description: str = Field(..., max_length=100,
                             example="")
    cassia_criticality_id: Optional[int] = Field(...,
                                                 example="1")


""" class AutoActionUpdateSchema(CassiaTechnologySchema):
    action_auto_id: int = Field(..., example="1") """
