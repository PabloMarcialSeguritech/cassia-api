from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime


class CassiaCIElement(DB_Zabbix.Base):
    __tablename__ = "cassia_ci_element"
    element_id = Column(Integer, primary_key=True, autoincrement=True)
    ip = Column(String(length=16), unique=False, nullable=False)
    host_id = Column(Integer, primary_key=True)
    technology = Column(String(length=120), unique=False, nullable=False)
    device_name = Column(String(length=120), unique=False, nullable=False)
    description = Column(String(length=254), unique=False, nullable=False)
    location = Column(String(length=120), unique=False, nullable=False)
    criticality = Column(Integer(), unique=False, nullable=False)
    status = Column(String(length=30), unique=False,
                    nullable=False, default="Activo")
    status_conf = Column(String(length=30), unique=False,
                         nullable=False, default="Sin cerrar")
    session_id = Column(String(length=120), ForeignKey(
        "cassia_users_sessions.session_id"), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
