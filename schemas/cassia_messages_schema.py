from pydantic import BaseModel
from typing import List, Optional, Literal
from pydantic import BaseModel
from pydantic import Field

class SendMessageRequest(BaseModel):
    groupid: int
    mensaje: str