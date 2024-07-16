from sqlalchemy import Column, String, Integer, DateTime, Float, SmallInteger, Text
from infraestructure.database_model import DB
from datetime import datetime


class CassiaCriticalitiesModel(DB.Base):
    __tablename__ = "cassia_criticalities"
    cassia_criticality_id = Column(
        Integer, primary_key=True, autoincrement=True)
    level = Column(SmallInteger,  nullable=False)
    name = Column(String(length=100),  nullable=False)
    description = Column(String(length=255),  nullable=True)
    icon = Column(Text, nullable=True)
    filename = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True, default=None)
    deleted_at = Column(DateTime, nullable=True, default=None)
