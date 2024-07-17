from infraestructure.database_model import DB
from sqlalchemy import Column, Integer, DateTime, Text, String
from datetime import datetime


class CassiaMaintenance(DB.Base):
    __tablename__ = "cassia_maintenance"
    maintenance_id = Column(Integer, autoincrement=True, primary_key=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    date_start = Column(DateTime, default=None, nullable=False)
    date_end = Column(DateTime, default=None, nullable=False)
    session_id = Column(String(length=120), nullable=False)
    hostid = Column(Integer, nullable=False)
    updated_at = Column(DateTime)
