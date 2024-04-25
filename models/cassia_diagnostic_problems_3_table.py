from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, Boolean
from datetime import datetime


class CassiaDiagnosticProblems3Table(DB_Zabbix.Base):
    __tablename__ = "cassia_diagnostic_problems_3"
    diagnostic_problem_id = Column(
        Integer, primary_key=True, autoincrement=True)
    hostid_origen = Column(Integer, unique=False, nullable=False)
    depends_hostid = Column(Integer, unique=False, nullable=False)
    status = Column(String(length=30), unique=False, nullable=False)
    closed_at = Column(DateTime, default=None, nullable=True)
    local = Column(Boolean, nullable=False)
    hostid = Column(Integer, unique=False, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=True)
    dependents = Column(Integer, unique=False, nullable=False, default=0)
    local_eventid = Column(Integer, unique=False, nullable=False)
