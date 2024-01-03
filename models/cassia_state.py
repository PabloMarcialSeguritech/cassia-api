from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime


class CassiaState(DB_Zabbix.Base):
    __tablename__ = "cassia_state"
    state_id = Column(Integer, primary_key=True, autoincrement=True)
    state_name = Column(String(length=50), nullable=False)
    url = Column(String(length=120), nullable=False)
    url_front = Column(String(length=120), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
    ip = Column(String(length=100), nullable=True)
    port = Column(String(length=100), nullable=True)
