from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship
from models.cassia_credentials import Credential


class CassiaAction(DB_Zabbix.Base):
    __tablename__ = "cassia_action"
    action_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(length=50), unique=False, nullable=False)
    protocol = Column(String(length=10), unique=False, nullable=False)
    comand = Column(String(length=120), unique=False, nullable=False)
    active = Column(Integer, nullable=True)
