from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from datetime import datetime
from infraestructure.database_model import DB


class CassiaTicketAsync(DB.Base):
    __tablename__ = "cassia_tickets"
    ticket_id = Column(Integer, primary_key=True, autoincrement=True)
    tracker_id = Column(String(length=50), unique=False, nullable=False)
    user_id = Column(Integer, nullable=False)
    clock = Column(DateTime, default=datetime.now, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    event_id = Column(Integer,  nullable=False)
    is_cassia_event = Column(Integer,  nullable=False, default=0)
