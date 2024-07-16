from sqlalchemy import Column, String, Integer, DateTime, BigInteger
from infraestructure.database_model import DB
from datetime import datetime


class CassiaTechDeviceModel(DB.Base):
    __tablename__ = "cassia_tech_devices"
    cassia_tech_device_id = Column(
        Integer, primary_key=True, autoincrement=True)
    criticality_id = Column(BigInteger,  nullable=True)
    hostid = Column(BigInteger,  nullable=True)
    cassia_tech_id = Column(BigInteger,  nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True, default=None)
    deleted_at = Column(DateTime, nullable=True, default=None)
