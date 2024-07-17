from pydantic import BaseModel
from pydantic import Field
from typing import Optional
from datetime import datetime


class CassiaMaintenanceBase(BaseModel):
    description: Optional[str]
    hostid: str = Field(..., example="1")
    date_start: datetime = Field(..., example="2024-01-31 18:08:47")
    date_end: datetime = Field(..., example="2024-01-31 18:08:47")


class CassiaMaintenance(CassiaMaintenanceBase):
    maintenance_id: Optional[int] = None
    updated_at: Optional[datetime] = None
