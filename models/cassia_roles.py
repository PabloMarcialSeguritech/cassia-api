from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime
from sqlalchemy.orm import relationship
from models.cassia_permissions import CassiaPermission
from models.role_has_permissions import RoleHasPermission

class CassiaRole(DB_Zabbix.Base):
    __tablename__ = "cassia_roles"
    rol_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(length=50), unique=False, nullable=False)
    description = Column(String(length=120), unique=False, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)

    permissions = relationship('CassiaPermission', secondary="role_has_permissions", lazy="subquery")
