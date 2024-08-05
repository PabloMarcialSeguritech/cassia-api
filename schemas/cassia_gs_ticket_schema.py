from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field


class CassiaGSTicketSchema(BaseModel):
    hostid: int = Field(...,
                        example=13250)
    comment: str = Field(..., max_length=100,
                         example="Componente Falla por defecto en transistor")


class CassiaGSTicketCommentSchema(BaseModel):
    ticket_id: int = Field(..., example="10086557")
    comment: str = Field(..., max_length=100,
                         example="Componente Falla por defecto en transistor")
    comment_type: Literal['internal_note',
                          'progress_solution'] = Field(..., example='internal_note')
