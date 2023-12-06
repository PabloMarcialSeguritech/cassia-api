from utils.db import DB_Zabbix
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship
from models.cassia_credentials import Credential


class InterfaceAction(DB_Zabbix.Base):
    __tablename__ = "interface_action"
    int_act_id = Column(Integer, primary_key=True, autoincrement=True)
    interface_id = Column(Integer, nullable=False)
    action_id = Column(Integer, nullable=False)
