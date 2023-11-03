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
class CiElementBase(BaseModel):
    ip: str = Field(..., max_length=16, example="172.18.206.105")
    host_id: int = Field(..., example="10659")
    folio: str = Field(..., max_length=20, example="CI-GTO-00001")
    technology: str = Field(..., max_length=120, example="Red municipal")
    device_name: str = Field(..., max_length=120, example="Suscriptor")
    description: str = Field(..., max_length=254, example="Descripcion")
    location: str = Field(..., max_length=120, example="Estado Guanajuato")
    criticality: int = Field(..., example="1")
    status: Literal['Activo', 'Inactivo']


class CiElement(CiElementBase):
    element_id: int = Field(..., example="1")

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
