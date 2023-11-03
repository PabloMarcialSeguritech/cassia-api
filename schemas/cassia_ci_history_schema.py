from pydantic import BaseModel, validator
from pydantic import Field, ValidationError
from typing import Optional, Literal
from datetime import datetime
from utils.traits import as_form
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
import pandas as pd


@as_form
class CiHistoryBase(BaseModel):
    element_id: int = Field(..., example="1")
    change_type: str = Field(..., max_length=120,
                             example="Software")

    description: str = Field(..., max_length=254,
                             example="Se sube de version de 21.03 a 22.02")

    justification: str = Field(..., max_length=254,
                               example="Se optimiza")
    hardware_no_serie: str = Field(..., max_length=120,
                                   example="SWX2023-XYZ")
    hardware_brand: str = Field(..., max_length=120,
                                example="Cambium Networks")
    hardware_model: str = Field(..., max_length=120,
                                example="PM 4501")
    software_version: str = Field(..., max_length=120,
                                  example="22.02")
    responsible_name: str = Field(..., max_length=120,
                                  example="Victor Hernández")
    auth_name: str = Field(..., max_length=120,
                           example="Daniel Pérez")
    created_at: datetime
    closed_at: Optional[datetime]
    status: Literal['Iniciada', 'Cerrada', 'Cancelada']


class CiHistory(CiHistoryBase):
    conf_id: int = Field(..., example="1")

    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
