from pydantic import BaseModel, validator
from pydantic import Field
from typing import Optional
from datetime import datetime
from utils.db import DB_Zabbix
from models.problem_record import ProblemRecord
from models.cassia_exception_agency import CassiaExceptionAgency
from fastapi.exceptions import HTTPException
from fastapi import status


class CassiaExceptionsBase(BaseModel):
    exception_agency_id: int = Field(
        ...,
        example="2"
    )
    description: Optional[str]
    hostid: str = Field(..., example="1")
    created_at: datetime = Field(..., example="2024-01-31 18:08:47")

    @validator("exception_agency_id", pre=True)
    def check_exception_agency_relation(cls, v):
        db_zabbix = DB_Zabbix()
        session = db_zabbix.Session()
        pr = session.query(CassiaExceptionAgency).filter(
            CassiaExceptionAgency.exception_agency_id == v).first()
        session.close()
        db_zabbix.stop()
        if not pr:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Exception Agency not exists",
            )
        return v


class CassiaExceptions(CassiaExceptionsBase):
    exception_id: int = Field(
        ...,
        example="1"
    )
    updated_at: datetime = Field(..., example="2024-01-31 18:08:47")


class CassiaExceptionsClose(BaseModel):
    closed_at: datetime = Field(..., example="2024-01-31 18:08:47")
