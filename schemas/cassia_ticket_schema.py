from pydantic import BaseModel
from pydantic import Field
from pydantic import EmailStr
from typing import Optional
from datetime import datetime


class CassiaTicketBase(BaseModel):
    tracker_id: str = Field(
        ...,
        example="AR-CSD-1234",
        max_length=50
    )
    clock: datetime = Field(
        ...,
        example="2023-08-24 17:15:23",
    )
    event_id: str = Field(
        ...,
        example='34975081'
    )


class CassiaTicket(CassiaTicketBase):
    ticket_id: int = Field(
        ...,
        example="1"
    )
