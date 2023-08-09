from utils.db import DB_Zabbix
from sqlalchemy import Column, DateTime
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey
from sqlalchemy import Column, DateTime, String


class CassiaUsersLog(DB_Zabbix.Base):
    __tablename__ = "cassia_users_log"
    user_id = mapped_column(
        ForeignKey("cassia_users.user_id"), primary_key=True, nullable=False)
    description = Column(String(length=120), unique=False, nullable=False)
    date = Column(DateTime, default=datetime.now)
