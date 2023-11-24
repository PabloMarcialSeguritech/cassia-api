from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime


class CassiaMail(DB_Zabbix.Base):
    __tablename__ = "cassia_mail"
    mail_id = Column(Integer, primary_key=True, autoincrement=True)
    request_user_id = Column(Integer, ForeignKey(
        "cassia_users.user_id"), nullable=False)
    autorizer_user_id = Column(Integer, ForeignKey(
        "cassia_users.user_id"), nullable=True)
    request_date = Column(DateTime, default=datetime.now)
    process_id = Column(Integer,  nullable=False)
    comments = Column(Text, unique=False, nullable=False)
    action = Column(Integer, nullable=True, default=None)
    action_comments = Column(Text, unique=False, nullable=False)
    cassia_conf_id = Column(Integer, nullable=False)
