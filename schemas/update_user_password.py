from pydantic import BaseModel
from pydantic import Field, validator, root_validator
import re


class UpdateUserPassword(BaseModel):
    old_password: str = Field(
        ...,
        min_length=8,
        max_length=64,
        example="strongpass",
    )
    new_password: str = Field(
        ...,
        min_length=8,
        max_length=64,
        example="A1b2c3.",

    )

    @validator("new_password")
    def validate_new_password(cls, new_password, **kwargs):
        regex = "((?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[\W]).{8,64})"
        if not re.fullmatch(regex, new_password):
            raise ValueError("The new_password do not match with the format")
        return new_password

    @validator("new_password_confirmation")
    def validate_new_password_confirmation(cls, new_password, **kwargs):
        regex = "((?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[\W]).{8,64})"
        if not re.fullmatch(regex, new_password):
            raise ValueError(
                "The new_password_confirmation do not match with the format")
        return new_password

    @root_validator()
    def verify_password_match(cls, values):
        new_password = values.get("new_password")
        new_password_conf = values.get("new_password_confirmation")
        old = values.get("old_password")
        if new_password != new_password_conf:
            raise ValueError(
                "The new_password and the new_password_confirmation did not match.")
        if new_password == values:
            raise ValueError(
                "The new password cannot be the same as the old one")
        return values
    new_password_confirmation: str = Field(
        ...,
        min_length=8,
        max_length=64,
        example="A1b2c3.",
        regex="((?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[\W]).{8,64})"
    )
