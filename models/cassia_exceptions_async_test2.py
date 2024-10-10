from infraestructure.database_model import DB
from sqlalchemy import Column, Integer, DateTime, Text, String
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey


class CassiaExceptionsAsyncTest2(DB.Base):
    __tablename__ = "cassia_exceptions_test_2"
    exception_id = Column(Integer, autoincrement=True, primary_key=True)
    exception_agency_id = Column(Integer, nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    init_date = Column(DateTime, default=None, nullable=False)
    end_date = Column(DateTime, default=None, nullable=True)
    session_id = Column(String(length=120), nullable=False)
    hostid = Column(Integer, nullable=False)
    updated_at = Column(DateTime, default=None, nullable=True)
    deleted_at = Column(DateTime, default=None, nullable=True)
