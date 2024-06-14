from pydantic import BaseModel
from typing import List
from pydantic import BaseModel
from pydantic import Field


class AutoActionSchema(BaseModel):
    name: str = Field(..., max_length=30,
                      example="Reinicio Windows por Ping")
    description: str = Field(..., max_length=30,
                             example="Reinicio Windows por Ping")
    action_id: int = Field(..., example="1")
    type_trigger: str = Field(..., max_length=30,
                              example="Metrica")
    condition_id: int = Field(..., example="1")


class AutoActionUpdateSchema(AutoActionSchema):
    action_auto_id: int = Field(..., example="1")
