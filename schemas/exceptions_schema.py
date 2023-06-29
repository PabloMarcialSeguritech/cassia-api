from pydantic import BaseModel, validator
from pydantic import Field
from typing import Optional
from datetime import datetime
from utils.db import DB_Zabbix
from models.problem_record import ProblemRecord
from models.exception_agency import ExceptionAgency
from fastapi.exceptions import HTTPException
from fastapi import status


class ExceptionsBase(BaseModel):
    problemrecord_id: int = Field(
        ...,
        example="1"
    )
    exception_agency_id: int = Field(
        ...,
        example="2"
    )
    description: str = Field(
        ...,
        example="Exception",
        min_length=1
    )

    @validator("exception_agency_id", pre=True)
    def check_exception_agency_relation(cls, v):
        db_zabbix = DB_Zabbix()
        session = db_zabbix.Session()
        pr = session.query(ExceptionAgency).filter(
            ExceptionAgency.exception_agency_id == v).first()
        session.close()
        db_zabbix.stop()
        if not pr:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Exception Agency not exists",
            )
        return v


class Exceptions(ExceptionsBase):
    exception_id: int = Field(
        ...,
        example="1"
    )
    user_id: int = Field(..., example="1")
    created_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
