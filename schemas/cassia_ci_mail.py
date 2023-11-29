from pydantic import BaseModel, validator
from pydantic import Field, ValidationError
from typing import Optional, Literal
from datetime import datetime
from utils.traits import as_form
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
import pandas as pd


@as_form
class CiMailBase(BaseModel):

    process_id: str = Field(..., example="3")
    comments: str = Field(..., example='')


class CiMail(CiMailBase):
    mail_id: int = Field(..., example="1")
    autorizer_user_id: str = Field(..., example="2")
    request_date: datetime = Field(..., example="2023-08-24 17:15:23")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None


class CiMailAuthorize(BaseModel):
    action: bool = Field(...)
    action_comments: Optional[str] = Field(..., example="")
