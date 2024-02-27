from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, Boolean
from datetime import datetime


class CassiaArchTrafficEvent2(DB_Zabbix.Base):
    __tablename__ = "cassia_arch_traffic_events_2"
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
    hostname = Column(String(length=128), unique=False, nullable=True)
    ip = Column(String(length=50), unique=False, nullable=True)
    tech_id = Column(String(length=5), unique=False, nullable=True, default=9)
