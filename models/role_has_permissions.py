from utils.db import DB_Zabbix
from sqlalchemy import Column, Integer, DateTime, Text
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey
from sqlalchemy import Column, String, Integer, DateTime
from models.cassia_permissions import CassiaPermission


class RoleHasPermission(DB_Zabbix.Base):
    __tablename__ = "role_has_permissions"
    permission_id = mapped_column(
        ForeignKey("cassia_permissions.permission_id"), primary_key=True, nullable=False)
    cassia_rol_id = mapped_column(
        ForeignKey("cassia_roles.rol_id"), primary_key=True, nullable=False)

    """ permission_id = Column(
        Integer, primary_key=True, nullable=False)
    cassia_rol_id = Column(
        Integer, primary_key=True, nullable=False) """
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
