from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field

class CassiaAuditExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    audit_ids: list = Field(..., example=[1, 2, 3, 4])