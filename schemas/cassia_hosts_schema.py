from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field


class CassiaHostExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    hostids: list = Field(..., example=[1, 2, 3, 4])


class CassiaHostUpdateSchema(BaseModel):
    # HOST DATA
    host: str = Field(...,
                      max_length=128,
                      example="ABAS-C4-TELEFONIA-SYNWAY-SMG1004C-40-MEDIA-GATEWAY-SYNWAY02-GTO-SUB-ABAS-001")

    name: str = Field(...,
                      max_length=128,
                      example="ABAS-C4-TELEFONIA-SYNWAY-SMG1004C-40-MEDIA-GATEWAY-SYNWAY02-GTO-SUB-ABAS-001")

    # No permite 0, Si no hay proxies forzar null
    proxy_id: int = Field(None, example=1)
    description: str = Field("",
                             example="ABAS-C4-TELEFONIA-SYNWAY-SMG1004C-40-MEDIA-GATEWAY-SYNWAY02-GTO-SUB-ABAS-001")
    status: Literal[0, 1]

    # HOST INVENTORY DATA
    device_id: int = Field(None, example=1)
    alias: str = Field("",
                       max_length=128
                       example="GTO-SUB-ABAS-001")
    location_lat: str = Field("",
                              max_length=16,
                              example="12.12")
    location_lon: str = Field("",
                              max_length=16,
                              example="12.12")
    serialno_a: str = Field("",
                            max_length=64,
                            example="SMG1004C40XC")
    macaddress_a: str = Field("",
                              max_length=64,
                              example="00:00:00:00:00:00")

    # HOST BRAND MODEL DATA
    brand_id: int = Field(None, example=1)
    model_id: int = Field(None, example=1)

    # HOST INTERFACE DATA
    agent_ip: str = Field("",
                          max_length=64,
                          example="127.0.0.1")
    agent_port: str = Field("",
                            max_length=64,
                            example="10050")
    snmp_ip: str = Field("",
                         max_length=64,
                         example="127.0.0.1")
    snpm_port: str = Field("",
                           max_length=64,
                           example="161")

    # HOST GROUPS DATA
    groupids: Optional[list] = Field([], example=[1, 2, 3, 4])
    zona_groupid: Optional[int] = Field(None, example=1)
