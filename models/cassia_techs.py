from sqlalchemy import Column, String, Integer, DateTime, BigInteger
from infraestructure.database_model import DB
from datetime import datetime


class CassiaTechModel(DB.Base):
    __tablename__ = "cassia_techs"
    cassia_tech_id = Column(
        Integer, primary_key=True, autoincrement=True)
    tech_name = Column(String(length=100),  nullable=False)
    tech_description = Column(String(length=100),  nullable=True)
    service_id = Column(BigInteger,  nullable=False)
    cassia_criticality_id = Column(BigInteger,  nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True, default=None)
    deleted_at = Column(DateTime, nullable=True, default=None)
