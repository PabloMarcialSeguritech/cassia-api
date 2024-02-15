from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.orm import relationship
from models.cassia_credentials import Credential
from datetime import datetime


class CassiaActionLog(DB_Zabbix.Base):
    __tablename__ = "cassia_action_log"
    action_log_id = Column(Integer, primary_key=True, autoincrement=True)
    action_id = Column(Integer, nullable=False)
    clock = Column(DateTime, default=datetime.now)
    session_id = Column(String(length=120), nullable=False)
    interface_id = Column(Integer, nullable=False)
    event_id = Column(Integer, nullable=True)
    result = Column(Integer, nullable=True)
    comments = Column(Text, nullable=True)
