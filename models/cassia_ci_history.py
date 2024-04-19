from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime


class CassiaCIHistory(DB_Zabbix.Base):
    __tablename__ = "cassia_ci_history"
    conf_id = Column(Integer, primary_key=True, autoincrement=True)
    element_id = Column(Integer, ForeignKey(
        "cassia_ci_element.element_id"), nullable=False)
    change_type = Column(String(length=120), unique=False, nullable=False)
    description = Column(String(length=254), unique=False, nullable=False)
    justification = Column(String(length=254), unique=False, nullable=False)
    hardware_no_serie = Column(
        String(length=120), unique=False, nullable=False)
    hardware_brand = Column(
        String(length=120), unique=False, nullable=False)
    hardware_model = Column(
        String(length=120), unique=False, nullable=False)
    software_version = Column(
        String(length=120), unique=False, nullable=False)
    responsible_name = Column(
        String(length=120), unique=False, nullable=True)
    auth_name = Column(
        String(length=120), unique=False, nullable=False)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.now)
    closed_at = Column(DateTime, default=None, nullable=True)
    deleted_at = Column(DateTime, default=None, nullable=True)
    session_id = Column(String(length=120), ForeignKey(
        "cassia_users_sessions.session_id"), nullable=False)
    status = Column(String(length=30), unique=False,
                    nullable=False, default="No iniciado")
    ticket = Column(String(length=50), unique=False,
                    nullable=True)
