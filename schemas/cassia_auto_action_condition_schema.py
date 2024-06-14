from pydantic import BaseModel
from typing import List, Optional
from pydantic import BaseModel
from pydantic import Field


class AutoActionConditionDetailSchema(BaseModel):
    delay: int = Field(..., example="30")
    template_name: str = Field(..., max_length=30, example="ping_response")
    template_id: int = Field(..., example="27065")
    range_min: str = Field(..., max_length=50, example="0.003")
    range_max: str = Field(..., max_length=50, example="0.005")
    units: str = Field(..., max_length=10, example="s")


class AutoActionConditionSchema(BaseModel):
    name: str = Field(..., max_length=120,
                      example="Condicion de reinicio de Windows por PING")
    detail: List[AutoActionConditionDetailSchema]


class AutoActionConditionDetailUpdateSchema(BaseModel):
    cond_detail_id: Optional[int]
    condition_id: int = Field(..., example="2")
    delay: int = Field(..., example="30")
    template_name: str = Field(..., max_length=30, example="ping_response")
    template_id: int = Field(..., example="27065")
    range_min: str = Field(..., max_length=50, example="0.003")
    range_max: str = Field(..., max_length=50, example="0.005")
    units: str = Field(..., max_length=10, example="s")


class AutoActionConditionUpdateSchema(BaseModel):
    condition_id: int = Field(..., example="2")
    name: str = Field(..., max_length=120,
                      example="Condicion de reinicio de Windows por PING")
    detail: List[AutoActionConditionDetailUpdateSchema]
