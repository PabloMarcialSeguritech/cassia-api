from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime


class CassiaArchTrafficEvent(DB_Zabbix.Base):
    __tablename__ = "cassia_arch_traffic_events"
    cassia_arch_traffic_events_id = Column(
        Integer, primary_key=True, autoincrement=True)
    hostid = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    closed_at = Column(DateTime, default=None, nullable=True)
    severity = Column(Integer, nullable=False)
    message = Column(String(length=100), unique=False, nullable=False)
    status = Column(String(length=100), unique=False, nullable=False)
    latitude = Column(String(length=16), unique=False, nullable=True)
    longitude = Column(String(length=16), unique=False, nullable=True)
    municipality = Column(String(length=100), unique=False, nullable=True)
    hostname = Column(String(length=160), unique=False, nullable=True)
    ip = Column(String(length=20), unique=False, nullable=True)
