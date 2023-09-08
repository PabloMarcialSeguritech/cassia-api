from utils.db import DB_Zabbix
from sqlalchemy import Column, ForeignKey, String, Integer
from sqlalchemy.orm import relationship


class Credential(DB_Zabbix.Base):
    __tablename__ = "cassia_credentials"
    cred_id = Column(Integer, primary_key=True, autoincrement=True)
    interface_id = Column(Integer, ForeignKey("interface.interfaceid"))
    usr = Column(String(length=120))
    psswrd = Column(String(length=120))
