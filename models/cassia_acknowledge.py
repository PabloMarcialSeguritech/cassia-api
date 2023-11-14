from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from models.problem_record import ProblemRecord
from typing import List


class CassiaAcknowledge(DB_Zabbix.Base):
    __tablename__ = "cassia_acknowledge"
    c_acknowledge_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = mapped_column(
        ForeignKey("cassia_users.user_id"),  nullable=False)
    acknowledge_id = Column(Integer,  nullable=False)