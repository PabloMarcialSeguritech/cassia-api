from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
import datetime


class CassiaCIRelation(DB_Zabbix.Base):
    __tablename__ = "cassia_ci_relations"

    cassia_ci_relation_id = Column(
        Integer, primary_key=True, autoincrement=True)
    cassia_ci_element_id = Column(Integer, ForeignKey(
        "cassia_ci_element.element_id"), nullable=False)
    depends_on_ci = Column(Integer, ForeignKey(
        "cassia_ci_element.element_id"), nullable=False)
    folio = Column(
        String(length=30), unique=False, nullable=False)
