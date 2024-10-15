from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field


class CassiaHostDeviceTechSchema(BaseModel):
    dispId: int = Field(..., example=1)
    visible_name: str = Field(..., max_length=255,
                      example="Tecnología ejemplo ")

class CassiaHostDeviceTechExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    dispIds: list = Field(..., example=[1, 2, 3, 4])
