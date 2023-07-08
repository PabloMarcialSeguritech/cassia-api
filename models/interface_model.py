from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer


class Interface(DB_Zabbix.Base):
    __tablename__ = "interface"
    interfaceid = Column(Integer, primary_key=True, autoincrement=True)
    hostid = Column(Integer)
    ip = Column(String(length=64))

    def __str__(self):
        return f"Interface interfaceid: {self.interfaceid} ip: {self.ip} hostid: {self.hostid}"
