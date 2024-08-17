from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from models.problem_record import ProblemRecord
from typing import List
from infraestructure.database_model import DB
from utils.traits import get_datetime_now_with_tz


class CassiaAcknowledge(DB.Base):
    __tablename__ = "cassia_acknowledge"
    c_acknowledge_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer,  nullable=False)
    acknowledge_id = Column(Integer,  nullable=False)
    clock = Column(DateTime, default=get_datetime_now_with_tz(), nullable=True)
