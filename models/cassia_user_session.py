from utils.db import DB_Zabbix
from sqlalchemy import Column, DateTime
from datetime import datetime
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey
from sqlalchemy import Column, DateTime, String, UUID, Boolean, Text
import uuid


class CassiaUserSession(DB_Zabbix.Base):
    __tablename__ = "cassia_users_sessions"
    session_id = Column(UUID(as_uuid=True),
                        primary_key=True, default=uuid.uuid4)
    user_id = mapped_column(
        ForeignKey("cassia_users.user_id"), primary_key=True, nullable=False)
    access_token = Column(Text, unique=False, nullable=False)
    start_date = Column(DateTime, default=datetime.now)
    end_date = Column(DateTime, nullable=True)
    active = Column(Boolean, default=True)
