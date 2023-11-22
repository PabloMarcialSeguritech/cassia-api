from pydantic import BaseModel
from pydantic import Field
from pydantic import EmailStr
from typing import Optional


class UserBase(BaseModel):
    mail: EmailStr = Field(
        ...,
        example="email@securitech.com"
    )
    name: str = Field(
        ...,
        min_length=3,
        max_length=120,
        example="Juan Pérez"
    )


class User(UserBase):
    user_id: int = Field(
        ...,
        example="5"
    )


class UserRegister(UserBase):
    """ password: str = Field(
        ...,
        min_length=8,
        max_length=64,
        example="strongpass"
    ) """
    roles: Optional[str]
    authorizer: Optional[int]
