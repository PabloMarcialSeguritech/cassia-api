from sqlalchemy import Column, String, Integer, DateTime, BigInteger
from infraestructure.database_model import DB
from datetime import datetime


class CassiaTechServiceModel(DB.Base):
    __tablename__ = "cassia_tech_services"
    cassia_tech_service_id = Column(
        Integer, primary_key=True, autoincrement=True)
    service_name = Column(String(length=100),  nullable=False)
    description = Column(String(length=100),  nullable=True)
    cassia_criticality_id = Column(BigInteger,  nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True, default=None)
    deleted_at = Column(DateTime, nullable=True, default=None)
