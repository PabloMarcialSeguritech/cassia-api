from utils.db import DB_Zabbix
from sqlalchemy import Column, Integer, DateTime, Text, String
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey


class CassiaExceptions(DB_Zabbix.Base):
    __tablename__ = "cassia_exceptions"
    exception_id = Column(Integer, autoincrement=True, primary_key=True)
    exception_agency_id = mapped_column(
        ForeignKey("cassia_exception_agencies.exception_agency_id"))
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    closed_at = Column(DateTime, default=None, nullable=True)
    session_id = Column(String(length=120), nullable=False)
    hostid = Column(Integer, nullable=False)
