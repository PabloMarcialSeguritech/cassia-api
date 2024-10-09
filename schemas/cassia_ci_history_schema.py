from pydantic import BaseModel, validator
from pydantic import Field, ValidationError
from typing import Optional, Literal
from datetime import datetime
from utils.traits import as_form
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
import pandas as pd
from utils.traits import get_datetime_now_with_tz

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
    """ auth_name: str = Field(..., max_length=120,
                           example="Daniel Pérez") """
    """ created_at: datetime """
    closed_at: Optional[datetime] = Field(default_factory=get_datetime_now_with_tz)
    status: Optional[Literal['No iniciado', 'Cerrada', 'Cancelada']]
    ticket: Optional[str]

    # Validador para manejar un valor vacío de closed_at
    @validator('closed_at', pre=True, always=True)
    def handle_empty_closed_at(cls, v):
        if v in [None, '', 'null']:
            return None
        return v


class CiHistory(CiHistoryBase):
    conf_id: int = Field(..., example="1")

    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
