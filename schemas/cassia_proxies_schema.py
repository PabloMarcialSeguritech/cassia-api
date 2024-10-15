from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field


class CassiaProxiesSchema(BaseModel):
    name: str = Field(..., max_length=128,
                      example="CELAYA")
    description: str = Field(..., max_length=255,
                             example="CELAYA")
    ip: str = Field(...,  example="127.0.0.1")
    mode: Literal['Active']


class CassiaProxiesExportSchema(BaseModel):
    proxy_ids: list = Field(..., example=[10436,
                                          10445,
                                          10448,
                                          10458])
    file_type: Literal['csv', 'json', 'excel']
