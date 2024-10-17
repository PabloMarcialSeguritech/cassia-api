from sqlalchemy import Column, String, Integer, DateTime, BigInteger, Float
from infraestructure.database_model import DB
from datetime import datetime


class CassiaHostModelsModel(DB.Base):
    __tablename__ = "cassia_host_model"
    model_id = Column(
        Integer, primary_key=True, autoincrement=True)
    name_model = Column(String(length=30),  nullable=False)
    brand_id = Column(BigInteger,  nullable=False)
    editable = Column(Integer, nullable=True, default=1)
