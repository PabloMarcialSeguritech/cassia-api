from fastapi import HTTPException, status
import json
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi import Depends
from typing import Annotated

from models.user_model import User as UserModel
from schemas import user_schema
from utils.db import DB_Zabbix
from sqlalchemy import or_, text
from sqlalchemy.orm import load_only
import pandas as pd
from services.auth_service import get_password_hash
from utils.traits import success_response


def create_user(user: user_schema.UserRegister):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    get_user = session.query(UserModel).filter(
        or_(UserModel.mail == user.mail)
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
        mail=user.mail,
        password=get_password_hash(user.password),
    )
    session.add(db_user)
    session.commit()
    session.refresh(db_user)
    session.close()
    db_zabbix.stop()
    # db_user.save()

    return success_response(message="User created", data=user_schema.User(
        id=db_user.user_id,
        username=db_user.username,
        email=db_user.email
    ))


def get_users():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    # db = DB_Auth.Session()
    """ users = db.query(UserModel).options(
        load_only(UserModel.id, UserModel.email, UserModel.username)).all() """
    statement = text("SELECT id, mail, username FROM users")
    users = session.execute(statement)
    # us = []
    # for user in users:
    #    print(user._mapping)
    #    us.append(user._mapping)
    data = pd.DataFrame(users)
    # print(us)
    session.close()
    db_zabbix.stop()
    return success_response(data=data.to_dict(orient="records"))
