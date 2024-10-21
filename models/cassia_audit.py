from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, MetaData, select, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from utils.db import DB_Zabbix

class CassiaAuditLog(DB_Zabbix.Base):
    __tablename__ = "cassia_audit_log"
    id = Column(Integer, primary_key=True)
    user_name = Column(String(length=120), nullable=False)
    user_email = Column(String(length=120), nullable=False)
    summary = Column(String(length=120), nullable=False)
    timestamp =  Column(DateTime, default=None, nullable=False)
    id_audit_action = Column(Integer, nullable=False)
    id_audit_module = Column(Integer, nullable=False)

class CassiaAuditAction(DB_Zabbix.Base):
    __tablename__ = "cassia_audit_action"
    id = Column(Integer, primary_key=True)
    name = Column(String(length=120), nullable=False)

class CassiaAuditModule(DB_Zabbix.Base):
    __tablename__ = "cassia_audit_module"
    id = Column(Integer, primary_key=True)
    name = Column(String(length=120), nullable=False)

