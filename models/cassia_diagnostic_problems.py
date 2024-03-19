from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime


class CassiaDiagnosticProblems(DB_Zabbix.Base):
    __tablename__ = "cassia_diagnostic_problems"
    diagnostic_problem_id = Column(
        Integer, primary_key=True, autoincrement=True)
    eventid = Column(Integer, unique=False, nullable=False)
    depends_eventid = Column(Integer, unique=False, nullable=True)
    closed_at = Column(DateTime, default=None, nullable=True)
    status = Column(String(length=30), unique=False, nullable=False)
