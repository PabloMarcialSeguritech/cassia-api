from pydantic import BaseModel
from pydantic import Field
from typing import Optional, Literal
from datetime import datetime


class ProblemRecordBase(BaseModel):
    estatus: Literal['En curso',
                     'Soporte 2do Nivel',  'Cerrado']


class ProblemRecord(ProblemRecordBase):
    problemrecord_id: int = Field(..., example="1")
    hostid: int = Field(..., example="1")
    problemid: int = Field(..., example="2")
    user_id: int = Field(..., example="3")
    exception_agency_id: int = Field(
        ...,
        example="5"
    )
    created_at: Optional[datetime] = None
    taken_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
