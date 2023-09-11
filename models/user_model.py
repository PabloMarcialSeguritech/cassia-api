from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from models.problem_record import ProblemRecord
from typing import List


class User(DB_Zabbix.Base):
    __tablename__ = "cassia_users"
    user_id = Column(Integer, primary_key=True, autoincrement=True)
    mail = Column(String(length=255), unique=True, index=True)

    name = Column(String(length=255), unique=False, index=True)

    password = Column(String(length=255))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
    verified_at = Column(DateTime, default=None, nullable=True)
    problem_records: Mapped[List['ProblemRecord']] = relationship()
