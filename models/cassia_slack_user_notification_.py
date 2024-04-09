from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm import mapped_column
from models.problem_record import ProblemRecord
from typing import List
from infraestructure.database_model import DB


class CassiaSlackUserNotification(DB.Base):
    __tablename__ = "cassia_slack_user_notifications"
    slack_user_notification_id = Column(
        Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    last_date = Column(DateTime, nullable=False)
