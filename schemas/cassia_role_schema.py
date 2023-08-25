from pydantic import BaseModel
from pydantic import Field
from pydantic import EmailStr
from typing import Optional


class RoleBase(BaseModel):
    name: str = Field(
        ...,
        example="Admin",
        max_length=50
    )
    description: str = Field(
        ...,
        example="Admin",
        max_length=50
    )


class Role(RoleBase):
    rol_id: int = Field(
        ...,
        example="1"
    )


class RoleRegister(RoleBase):
    permissions: Optional[str]
