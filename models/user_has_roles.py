from utils.db import DB_Zabbix
from sqlalchemy import Column, DateTime
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey
from sqlalchemy import Column, DateTime


class UserHasRole(DB_Zabbix.Base):
    __tablename__ = "user_has_roles"
    user_id = mapped_column(
        ForeignKey("cassia_users.user_id"), primary_key=True, nullable=False)
    role_id = mapped_column(
        ForeignKey("cassia_roles.rol_id"), primary_key=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
