from utils.db import DB_Zabbix
from sqlalchemy import Column, ForeignKey, String, Integer, Text
from sqlalchemy.orm import relationship


class HostCredential(DB_Zabbix.Base):
    __tablename__ = "cassia_host_credentials"
    host_credential_id = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String(length=120), nullable=False)
    password = Column(Text, nullable=False)
    protocol_id = Column(Integer, nullable=False)
    port = Column(Integer, nullable=False)
    hostid = Column(Integer, nullable=False)
