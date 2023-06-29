from utils.db import DB_Zabbix
from sqlalchemy import Column, Integer, DateTime, Text
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey


class Exceptions(DB_Zabbix.Base):
    __tablename__ = "exceptions"
    exception_id = Column(Integer, autoincrement=True, primary_key=True)
    problemrecord_id = mapped_column(
        ForeignKey("problem_records.problemrecord_id"))
    exception_agency_id = mapped_column(
        ForeignKey("exception_agencies.exception_agency_id"))
    description = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
    user_id = mapped_column(ForeignKey("cassia_users.user_id"))
