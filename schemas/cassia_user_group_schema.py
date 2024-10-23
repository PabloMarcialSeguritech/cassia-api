from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field

class CassiaUserGroupSchema(BaseModel):
    name: str = Field(..., max_length=255,
                      example="CELAYA")
    description: str = Field(..., max_length=255,
                      example="CELAYA")
