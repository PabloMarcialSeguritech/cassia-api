from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from models.problem_record import ProblemRecord
from typing import List


class CassiaSlackNotification(DB_Zabbix.Base):
    __tablename__ = "cassia_slack_notifications"
    notification_id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(length=36), nullable=False)
    message = Column(Text, nullable=False)
    state = Column(String(length=100), nullable=True)
    problem_date = Column(DateTime, default=None, nullable=False)
    message_date = Column(DateTime, default=datetime.now, nullable=False)
    host = Column(String(length=140), nullable=False)
    incident = Column(String(length=100), nullable=True)
    severity = Column(Integer, nullable=True)
    eventid = Column(Integer, nullable=True)
    status = Column(String(length=100), nullable=True)
