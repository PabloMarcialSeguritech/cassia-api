from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field

class CassiaBrandExportSchema(BaseModel):
    file_type: Literal['csv', 'json', 'excel']
    brand_ids: list = Field(..., example=[1, 2, 3, 4])


class CassiaBrandSchema(BaseModel):
    name_brand: str = Field(..., max_length=255, example="Nueva_marca")
    mac_address_brand_OUI: str = Field(..., max_length=255, example="xx:xx:xx")



