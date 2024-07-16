from pydantic import BaseModel
from typing import List, Optional
from pydantic import BaseModel
from pydantic import Field


class CassiaTechDeviceSchema(BaseModel):
    criticality_id: Optional[int] = Field(...,
                                          example="1")
    hostid: int = Field(..., example="20954")
    cassia_tech_id: int = Field(..., example="1")
