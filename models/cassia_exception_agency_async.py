from utils.db import DB_Zabbix
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime
from infraestructure.database_model import DB


class CassiaExceptionAgencyAsync(DB.Base):
    __tablename__ = "cassia_exception_agencies"
    exception_agency_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(length=120), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, nullable=True)
    deleted_at = Column(DateTime, default=None, nullable=True)
    img = Column(String(length=100), nullable=True)
    color = Column(String(length=100), nullable=True)
    shortName = Column(String(length=7), nullable=True)

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
