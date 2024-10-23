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
    proxy_id: Optional[int] = Field(None, example=1)
    description: Optional[str] = Field(None,
                                       example="ABAS-C4-TELEFONIA-SYNWAY-SMG1004C-40-MEDIA-GATEWAY-SYNWAY02-GTO-SUB-ABAS-001")
    status: Literal[0, 1]

    # HOST INVENTORY DATA
    device_id: Optional[int] = Field(None, example=1)
    alias: Optional[str] = Field(None,
                                 max_length=128,
                                 example="GTO-SUB-ABAS-001")
    location_lat: str = Field(...,
                              max_length=16,
                              example="12.12")
    location_lon: str = Field(...,
                              max_length=16,
                              example="12.12")
    serialno_a: Optional[str] = Field(None,
                                      max_length=64,
                                      example="SMG1004C40XC")
    macaddress_a: Optional[str] = Field(None,
                                        max_length=64,
                                        example="00:00:00:00:00:00")

    # HOST BRAND MODEL DATA
    brand_id: Optional[int] = Field(None, example=1)
    model_id: Optional[int] = Field(None, example=1)

    # HOST INTERFACE DATA
    agent_ip: str = Field(...,
                          max_length=64,
                          example="127.0.0.1")
    agent_port: str = Field("10050",
                            max_length=64,
                            example="10050")
    snmp_ip: Optional[str] = Field(None,
                                   max_length=64,
                                   example="127.0.0.1")
    snmp_port: Optional[str] = Field(None,
                                     max_length=64,
                                     example="161")
    snmp_version: Optional[int] = Field(default=2,
                                        example=2)
    snmp_community: Optional[str] = Field(default="public",
                                          max_length=64,
                                          example="public")

    # HOST GROUPS DATA
    groupids: Optional[list] = Field(default=[], example=[1, 2, 3, 4])
    zona_groupid: Optional[int] = Field(None, example=1)


class CassiaHostSchema(BaseModel):
    # HOST DATA
    host: str = Field(...,
                      max_length=128,
                      example="ABAS-C4-TELEFONIA-SYNWAY-SMG1004C-40-MEDIA-GATEWAY-SYNWAY02-GTO-SUB-ABAS-001")

    name: str = Field(...,
                      max_length=128,
                      example="ABAS-C4-TELEFONIA-SYNWAY-SMG1004C-40-MEDIA-GATEWAY-SYNWAY02-GTO-SUB-ABAS-001")

    # No permite 0, Si no hay proxies forzar null
    proxy_id: Optional[int] = Field(None, example=1)
    description: Optional[str] = Field(None,
                                       example="ABAS-C4-TELEFONIA-SYNWAY-SMG1004C-40-MEDIA-GATEWAY-SYNWAY02-GTO-SUB-ABAS-001")
    status: Literal[0, 1]

    # HOST INVENTORY DATA
    device_id: Optional[int] = Field(None, example=1)
    alias: Optional[str] = Field(None,
                                 max_length=128,
                                 example="GTO-SUB-ABAS-001")
    location_lat: str = Field(...,
                              max_length=16,
                              example="12.12")
    location_lon: str = Field(...,
                              max_length=16,
                              example="12.12")
    serialno_a: Optional[str] = Field(None,
                                      max_length=64,
                                      example="SMG1004C40XC")
    macaddress_a: Optional[str] = Field(None,
                                        max_length=64,
                                        example="00:00:00:00:00:00")

    # HOST BRAND MODEL DATA
    brand_id: Optional[int] = Field(None, example=1)
    model_id: Optional[int] = Field(None, example=1)

    # HOST INTERFACE DATA
    agent_ip: str = Field(...,
                          max_length=64,
                          example="127.0.0.1")
    agent_port: str = Field("10050",
                            max_length=64,
                            example="10050")
    snmp_ip: Optional[str] = Field(None,
                                   max_length=64,
                                   example="127.0.0.1")
    snmp_port: Optional[str] = Field(None,
                                     max_length=64,
                                     example="161")
    snmp_version: Optional[int] = Field(default=2,
                                        example=2)
    snmp_community: Optional[str] = Field(default="public",
                                          max_length=64,
                                          example="public")

    # HOST GROUPS DATA
    groupids: Optional[list] = Field(default=[], example=[1, 2, 3, 4])
    zona_groupid: Optional[int] = Field(None, example=1)
