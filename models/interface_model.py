from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship
from models.cassia_credentials import Credential

class Interface(DB_Zabbix.Base):
    __tablename__ = "interface"
    interfaceid = Column(Integer, primary_key=True, autoincrement=True)
    hostid = Column(Integer)
    ip = Column(String(length=64))

    credentials = relationship("Credential")

    def __str__(self):
        return f"Interface interfaceid: {self.interfaceid} ip: {self.ip} hostid: {self.hostid}"
