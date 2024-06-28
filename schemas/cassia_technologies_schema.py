from pydantic import BaseModel
from typing import List
from pydantic import BaseModel
from pydantic import Field


class CassiaTechnologySchema(BaseModel):
    technology_name: str = Field(..., max_length=120,
                                 example="Tecnologia de servicio de videovigilancia")
    sla: float = Field(...,
                       example=36)
    tech_group_ids: str = Field(..., max_length=100,
                                example="11,2,9")


class AutoActionUpdateSchema(CassiaTechnologySchema):
    action_auto_id: int = Field(..., example="1")
