from utils.db import DB_Auth
from sqlalchemy import Column, String, Integer, DateTime
from datetime import datetime


class User(DB_Auth.Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(length=255), unique=True, index=True)
    username = Column(String(length=255), unique=True, index=True)
    password = Column(String(length=255))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now)
    deleted_at = Column(DateTime, default=None, nullable=True)
