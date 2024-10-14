from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field


class CassiaHostGroupSchema(BaseModel):
    name: str = Field(..., max_length=255,
                      example="CELAYA")
    type_id: int = Field(..., example=1)
    groupid: Optional[int] = Field(None, example=1)


class CassiaHostGroupExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    groupids: list = Field(..., example=[1, 2, 3, 4])
