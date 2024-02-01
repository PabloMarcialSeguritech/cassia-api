from pydantic import BaseModel
from pydantic import Field
from typing import Optional
from datetime import datetime


class CassiaExceptionAgencyBase(BaseModel):
    name: str = Field(
        ...,
        example="CFE",
        max_length=120
    )


class CassiaExceptionAgency(CassiaExceptionAgencyBase):
    exception_agency_id: int = Field(
        ...,
        example="5"
    )
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
