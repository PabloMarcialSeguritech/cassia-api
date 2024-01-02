from pydantic import BaseModel, validator
from pydantic import Field, ValidationError
from typing import Optional, Literal, List
from datetime import datetime
from utils.traits import as_form
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
import pandas as pd


""" @as_form
class CiMailBase(BaseModel):

    process_id: str = Field(..., example="3")
    comments: str = Field(..., example='')
 """


@as_form
class HostManagementBase(BaseModel):
    host: str = Field(..., example="SUSCRIPTOR-001-CELAYA")
    interface_agent_ip: Optional[str] = Field(..., example="192.12.12.15")
    interface_agent_dns: Optional[str] = Field(..., example="")
    interface_agent_port: Optional[str] = Field(..., example="10050")
    interface_snmp_ip: Optional[str] = Field(example="192.12.12.15")
    interface_snmp_dns: Optional[str] = Field(example="")
    interface_snmp_port: Optional[str] = Field(example="10050")
    interface_snmp_version: Optional[Literal[1, 2, 3]]
    interface_snmp_community: Optional[str] = Field(example="snmp_community")
    host_groups: List[str]
    host_templates: List[str]
    inventory_mode: Literal[0, 1]
    location_lat: str = Field(..., example="12.12")
    location_lon: str = Field(..., example="12.12")
    alias: str = Field(..., example="Alias_equipo")
    name: str = Field(..., example="Nombre_equipo")
    status: Literal[0, 1]
    proxy_hostid: Optional[str] = Field(..., example="10515")
    credentials_protocol_ids: Optional[List[int]]
    credentials_users: Optional[List[str]]
    credentials_password: Optional[List[str]]
    credentials_ports: Optional[List[int]]


class HostManagementUpdate(HostManagementBase):
    interface_agent_id: Optional[str] = Field(..., example="123")
    interface_snmp_id: Optional[str] = Field(..., example="1234")


""" class CiMail(CiMailBase):
    mail_id: int = Field(..., example="1")
    autorizer_user_id: str = Field(..., example="2")
    request_date: datetime = Field(..., example="2023-08-24 17:15:23")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None


class CiMailAuthorize(BaseModel):
    action: bool = Field(...)
    action_comments: Optional[str] = Field(..., example="")
 """
