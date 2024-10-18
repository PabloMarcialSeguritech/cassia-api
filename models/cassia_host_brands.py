from sqlalchemy import Column, String, Integer, DateTime, BigInteger, Float
from infraestructure.database_model import DB
from datetime import datetime


class CassiaHostBrandModel(DB.Base):
    __tablename__ = "cassia_host_brand"
    brand_id = Column(
        Integer, primary_key=True, autoincrement=True)
    name_brand = Column(String(length=30),  nullable=False)
    mac_address_brand_OUI = Column(String(length=30),  nullable=False)
    editable = Column(Integer, nullable=True, default=1)