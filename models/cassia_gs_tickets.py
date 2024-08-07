from sqlalchemy import Column, String, Integer, DateTime, BigInteger, Float
from infraestructure.database_model import DB
from datetime import datetime


class CassiaGSTicketsModel(DB.Base):
    __tablename__ = "cassia_gs_tickets"
    cassia_gs_tickets_id = Column(
        Integer, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger,  nullable=False)
    afiliacion = Column(String(length=100),  nullable=False)
    no_serie = Column(String(length=100),  nullable=True)
    host_id = Column(BigInteger,  nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    last_update = Column(DateTime, nullable=True, default=None)
    closed_at = Column(DateTime, nullable=True, default=None)
    status = Column(String(length=100),  nullable=True)
    ticket_id = Column(BigInteger, nullable=True)
    message_id = Column(String(length=100),  nullable=True)
    error = Column(String(length=100),  nullable=True)
