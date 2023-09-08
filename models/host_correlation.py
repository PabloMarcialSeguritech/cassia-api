from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey


class HostCorrelation(DB_Zabbix.Base):
    __tablename__ = "host_correlation"
    correlarionid = Column(Integer, primary_key=True, autoincrement=True)
    hostidP = mapped_column(ForeignKey("hosts.hostid"))
    hostidC = mapped_column(ForeignKey("hosts.hostid"))
    session_id = Column(String(length=120), unique=False, nullable=False)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
