from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, MetaData, select, func
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from utils.db import DB_Zabbix

class CassiaUsersGroups(DB_Zabbix.Base):
    __tablename__ = "cassia_user_groups"
    id = Column(Integer, primary_key=True)
    name = Column(String(length=255), nullable=False)
    description = Column(String(length=255), nullable=False)