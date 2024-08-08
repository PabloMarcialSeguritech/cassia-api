from sqlalchemy import Column, String, Integer, DateTime, BigInteger, Text
from infraestructure.database_model import DB
from datetime import datetime


class CassiaGSTicketsDetailModel(DB.Base):
    __tablename__ = "cassia_gs_tickets_detail"
    cassia_gs_tickets_detail_id = Column(
        Integer, primary_key=True, autoincrement=True)
    cassia_gs_tickets_id = Column(BigInteger,  nullable=False)
    ticket_id = Column(BigInteger,  nullable=False)
    type = Column(String(length=100),  nullable=True)
    comment = Column(String(length=100),  nullable=True)
    status = Column(String(length=100),  nullable=True)
    created_at = Column(DateTime, nullable=True)
    user_email = Column(String,  nullable=False)
    file_url = Column(Text, nullable=True)
    last_update = Column(DateTime, nullable=True, default=None)
    message_id = Column(String,  nullable=False)
    requested_at = Column(DateTime, default=datetime.now)
    created_with_mail = Column(String,  nullable=True)
