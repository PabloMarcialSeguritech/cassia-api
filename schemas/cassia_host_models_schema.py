from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field


class CassiaHostModelSchema(BaseModel):
    name_model: str = Field(..., max_length=255,
                            example="CELAYA")
    brand_id: int = Field(..., example=1)


class CassiaHostModelsExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    modelids: list = Field(..., example=[1, 2, 3, 4])
