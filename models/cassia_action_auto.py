from sqlalchemy import Column, String, Integer, BigInteger
from infraestructure.database_model import DB


class CassiaActionAutoModel(DB.Base):
    __tablename__ = "cassia_action_auto"
    action_auto_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(length=30),  nullable=False)
    description = Column(String(length=30),  nullable=False)
    action_id = Column(BigInteger,  nullable=False)
    type_trigger = Column(String(length=30),  nullable=False)
    condition_id = Column(BigInteger,  nullable=False)
