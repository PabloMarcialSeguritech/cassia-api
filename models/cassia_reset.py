from infraestructure.database_model import DB
from sqlalchemy import Column, Integer, DateTime, Text
from datetime import datetime


class CassiaReset(DB.Base):
    __tablename__ = "cassia_reset"
    reset_id = Column(Integer, autoincrement=True, primary_key=True)
    affiliation = Column(Text, nullable=True)
    object_id = Column(Text, nullable=True)
    imei = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
