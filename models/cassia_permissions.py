from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime


class CassiaPermission(DB_Zabbix.Base):
    __tablename__ = "cassia_permissions"
    permission_id = Column(Integer, primary_key=True, autoincrement=True)
    module_name = Column(String(length=50), unique=False, nullable=False)
    name = Column(String(length=50), unique=False, nullable=False)
    description = Column(String(length=120), unique=False, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
