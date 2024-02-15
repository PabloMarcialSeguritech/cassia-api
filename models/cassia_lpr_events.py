from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer


class CassiaLprEvent(DB_Zabbix.Base):
    __tablename__ = "cassia_lpr_events"
    event_id = Column(Integer, primary_key=True, autoincrement=True)
    devicedReportedTime = Column(String(length=50), unique=False, nullable=False)
    ip = Column(String(length=20), unique=False, nullable=False)
    FromHost = Column(String(length=120), unique=False, nullable=False)
    message = Column(String, nullable=True)
    SysLogTag = Column(String(length=255), unique=False, nullable=False)

    def __init__(self, devicedReportedTime, ip, FromHost, message, SysLogTag):
        self.devicedReportedTime = devicedReportedTime
        self.ip = ip
        self.FromHost = FromHost
        self.message = message
        self.SysLogTag = SysLogTag
