from utils.db import DB_Zabbix
from sqlalchemy import Column, Integer, DateTime, Text
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey


class ProblemRecord(DB_Zabbix().Base):
    __tablename__ = "problem_records_history"
    problemsHistory_id = Column(Integer, primary_key=True, autoincrement=True)
    problemrecord_id = mapped_column(
        ForeignKey("problem_records.problemrecord_id"))
    user_id = mapped_column(ForeignKey("cassia_users.user_id"))
    message = Column(Text, nullable=True)
    file = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
