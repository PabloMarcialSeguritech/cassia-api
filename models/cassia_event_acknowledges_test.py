from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from models.problem_record import ProblemRecord
from typing import List
from infraestructure.database_model import DB
from utils.traits import get_datetime_now_with_tz


class CassiaEventAcknowledgeTest(DB.Base):
    __tablename__ = "cassia_event_acknowledges_test"
    acknowledgeid = Column(Integer, primary_key=True, autoincrement=True)
    userid = Column(Integer,  nullable=False)
    eventid = Column(Integer, nullable=False)
    clock = Column(DateTime, default=get_datetime_now_with_tz(), nullable=True)
    message = Column(Text, nullable=False)
    action = Column(Integer, nullable=False)
