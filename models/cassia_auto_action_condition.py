from sqlalchemy import Column, String, Integer
from infraestructure.database_model import DB


class CassiaAutoActionConditionModel(DB.Base):
    __tablename__ = "cassia_auto_condition"
    condition_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(length=120),  nullable=False)
