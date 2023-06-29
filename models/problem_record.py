from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer, DateTime, Enum
from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import ForeignKey
import enum


class ProblemRecord(DB_Zabbix().Base):
    __tablename__ = "problem_records"
    problemrecord_id = Column(Integer, primary_key=True, autoincrement=True)
    hostid = Column(Integer)
    problemid = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)
    closed_at = Column(DateTime, default=None, nullable=True)
    estatus = Column(Enum("Creado", "En curso", "Cerrado",
                          "Soporte 2do Nivel", "Excepcion",
                          ))
    user_id = mapped_column(ForeignKey("cassia_users.user_id"))
