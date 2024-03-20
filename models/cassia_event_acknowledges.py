from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from models.problem_record import ProblemRecord
from typing import List


class CassiaEventAcknowledge(DB_Zabbix.Base):
    __tablename__ = "cassia_event_acknowledges"
    acknowledgeid = Column(Integer, primary_key=True, autoincrement=True)
    userid = mapped_column(
        ForeignKey("cassia_users.user_id"),  nullable=False)
    eventid = Column(Integer, nullable=False)
    clock = Column(DateTime, default=datetime.now(), nullable=True)
    message = Column(Text, nullable=False)
    action = Column(Integer, nullable=False)
