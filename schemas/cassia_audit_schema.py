from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field

class CassiaAuditExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    audit_ids: list = Field(..., example=[1, 2, 3, 4])

class CassiaAuditSchema(BaseModel):
    user_name: str = Field(..., max_length=255,
                      example="CELAYA")
    user_email: str = Field(..., max_length=255,
                      example="CELAYA")
    summary : str = Field(..., max_length=255,
                      example="CELAYA")
    id_audit_action: int = Field(..., example=1)
    id_audit_module: int = Field(..., example=1)
