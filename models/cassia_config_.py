from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer
from infraestructure.database_model import DB


class CassiaConfig(DB.Base):
    __tablename__ = "cassia_config"
    config_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(length=120), unique=False, nullable=False)
    data_type = Column(String(length=30), unique=False, nullable=False)
    value = Column(String(length=50), unique=False, nullable=False)
