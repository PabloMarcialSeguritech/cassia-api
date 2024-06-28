from sqlalchemy import Column, String, Integer, DateTime, Float
from infraestructure.database_model import DB
from datetime import datetime


class CassiaTechnologiesModel(DB.Base):
    __tablename__ = "cassia_technologies"
    cassia_technologies_id = Column(
        Integer, primary_key=True, autoincrement=True)
    technology_name = Column(String(length=120),  nullable=False)
    sla = Column(Float,  nullable=False)
    tech_group_ids = Column(String(length=100),  nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True, default=None)
    deleted_at = Column(DateTime, nullable=True, default=None)
