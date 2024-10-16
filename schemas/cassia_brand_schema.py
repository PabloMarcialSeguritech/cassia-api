from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field

class CassiaBrandExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    brand_ids: list = Field(..., example=[1, 2, 3, 4])
