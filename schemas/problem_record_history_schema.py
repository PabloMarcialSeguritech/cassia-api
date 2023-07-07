from pydantic import BaseModel
from pydantic import Field
from typing import Optional, Literal
from datetime import datetime


class ProblemRecordHistoryBase(BaseModel):
    message: Optional[str] = None


class ProblemRecordHistory(ProblemRecordHistoryBase):
    problemsHistory_id: int = Field(..., example="1")
    problemrecord_id: int = Field(..., example="2")
    user_id: int = Field(..., example="3")
    created_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
