from pydantic import BaseModel, validator
from pydantic import Field, ValidationError
from pydantic_core import PydanticCustomError
from typing import Optional, Literal
from datetime import datetime
from utils.traits import as_form
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
import pandas as pd


@as_form
class CiBase(BaseModel):
    host_id: int = Field(..., example="1342")
    ip: str = Field(..., max_length=16, example="172.168.2.3")
    date: datetime = Field(..., example="2023-04-23T10:20:30.400+02:30")
    responsible_name: str = Field(..., max_length=120,
                                  example="Victor Hernández")
    auth_name: str = Field(..., max_length=120,
                           example="Daniel Pérez")
    device_description: str = Field(..., max_length=120,
                                    example="Cámara Fija perteneciente a dirección")
    justification: str = Field(..., max_length=120,
                               example="Se actualiza la version de firmware de la camara")
    previous_state: str = Field(..., max_length=120,
                                example="Version firmware 5.1")
    new_state: str = Field(..., max_length=120,
                           example="Version firmware 5.2")
    impact: str = Field(..., max_length=120,
                        example="Mejora en la estabilidad del sistema y la deteccion de movimiento.")
    observations: Optional[str] = Field(example="Observaciones")
    result: str = Field(..., max_length=120,
                        example="Dispositivo configurado sin inconvenientes.")
    status: Literal['Activo', 'Inactivo']


class Ci(CiBase):
    ci_id: int = Field(..., example="1")

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None


@as_form
class CiUpdate(CiBase):
    doc_ids: Optional[list] = None
