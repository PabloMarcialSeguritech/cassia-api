from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime


class CassiaCI(DB_Zabbix.Base):
    __tablename__ = "cassia_ci"
    ci_id = Column(Integer, primary_key=True, autoincrement=True)
    host_id = Column(Integer, primary_key=True)
    ip = Column(String(length=16), unique=False, nullable=False)
    date = Column(DateTime, default=datetime.now, nullable=False)
    responsible_name = Column(String(length=120), unique=False, nullable=False)
    auth_name = Column(String(length=120), unique=False, nullable=False)
    session_id = Column(String(length=120), ForeignKey(
        "cassia_users_sessions.session_id"), nullable=False)
    device_description = Column(
        String(length=120), unique=False, nullable=False)
    justification = Column(String(length=120), unique=False, nullable=False)
    previous_state = Column(String(length=120), unique=False, nullable=False)
    new_state = Column(String(length=120), unique=False, nullable=False)
    impact = Column(String(length=120), unique=False, nullable=False)
    observations = Column(Text, unique=False, nullable=False)
    result = Column(String(length=120), unique=False, nullable=False)
    status = Column(String(length=20), unique=False,
                    nullable=False, default="Activo")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
