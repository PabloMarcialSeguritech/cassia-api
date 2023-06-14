from fastapi import HTTPException, status
import json
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi import Depends
from typing import Annotated

from models.user_model import User as UserModel
from schemas import user_schema
from utils.db import DB_Auth
from sqlalchemy import or_, text
from sqlalchemy.orm import load_only
import pandas as pd
from services.auth_service import get_password_hash


def create_user(user: user_schema.UserRegister):
    db = DB_Auth.Session()
    get_user = db.query(UserModel).filter(
        or_(UserModel.email == user.email, UserModel.username == user.username)
    ).first()
    # get_user = UserModel.filter((UserModel.email == user.email) | (
    #    UserModel.username == user.username)).first()
    if get_user:
        msg = "Email already registered"
        if get_user.username == user.username:
            msg = "Username already registered"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=msg
        )
    db_user = UserModel(
        username=user.username,
        email=user.email,
        password=get_password_hash(user.password),
    )
    db.add(db_user)
    db.commit()
    # db_user.save()

    return user_schema.User(
        id=db_user.id,
        username=db_user.username,
        email=db_user.email
    )


def get_users():
    db = DB_Auth.Session()
    """ users = db.query(UserModel).options(
        load_only(UserModel.id, UserModel.email, UserModel.username)).all() """
    statement = text("SELECT id, email, username FROM users")
    users = db.execute(statement)
    # us = []
    # for user in users:
    #    print(user._mapping)
    #    us.append(user._mapping)
    data = pd.DataFrame(users)
    # print(us)

    return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))
