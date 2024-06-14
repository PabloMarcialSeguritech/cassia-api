from sqlalchemy import Column, String, Integer
from infraestructure.database_model import DB


class CassiaAutoActionConditionDetailModel(DB.Base):
    __tablename__ = "cassia_auto_condition_detail"
    cond_detail_id = Column(Integer, primary_key=True, autoincrement=True)
    condition_id = Column(Integer, nullable=False)
    delay = Column(String(length=30), nullable=False)
    template_name = Column(String(length=30), nullable=False)
    template_id = Column(Integer, nullable=False)
    range_min = Column(String(length=50), nullable=False)
    range_max = Column(String(length=50), nullable=False)
    units = Column(String(length=10),  nullable=False)
