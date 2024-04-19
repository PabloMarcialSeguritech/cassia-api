from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
import datetime


class CassiaCIDocument1(DB_Zabbix.Base):
    __tablename__ = "cassia_ci_documents1"
    doc_id = Column(Integer, primary_key=True, autoincrement=True)
    element_id = Column(Integer, ForeignKey(
        "cassia_ci_element.element_id"), nullable=False)
    path = Column(Text, nullable=False)
    filename = Column(Text, nullable=False)
    is_link = Column(Integer, nullable=False)
