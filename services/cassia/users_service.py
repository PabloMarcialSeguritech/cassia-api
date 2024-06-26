from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_Prueba
from sqlalchemy import text, or_
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from models.user_model import User
from models.user_has_roles import UserHasRole
from models.cassia_user_authorizer import UserAuthorizer
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
import numpy as np
from datetime import datetime
from utils.traits import success_response
from fastapi import status, File, UploadFile
from fastapi.responses import FileResponse
from schemas import user_schema
from services.auth_service import get_password_hash, verify_password
import secrets
import string
import os
import ntpath
import shutil
from fastapi import BackgroundTasks
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from schemas import update_user_password
settings = Settings()


def get_users():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        "SELECT user_id, mail, name FROM cassia_users where deleted_at IS NULL")
    users = session.execute(statement)
    data = pd.DataFrame(users)
    roles = []
    for ind in data.index:
        roles.append(get_roles_user(data['user_id'][ind]))
    data['roles'] = roles
    session.close()
    return success_response(data=data.to_dict(orient="records"))


def get_roles_user(user_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select role_id from user_has_roles where user_id={user_id}")
    roles = session.execute(statement)
    roles = pd.DataFrame(roles).replace(np.nan, "")
    if not roles.empty:
        if len(roles["role_id"]) > 1:
            rol_ids = tuple(roles['role_id'].values.tolist())
        else:
            rol_ids = f"({roles['role_id'][0]})"
        statement = text(
            f"select rol_id,name from cassia_roles where rol_id in{rol_ids} and deleted_at IS NULL")
        roles = session.execute(statement)
        roles = pd.DataFrame(roles).replace(np.nan, "")
        session.close()
        return roles.to_dict(orient="records")
    else:
        session.close()
        return []


def get_roles():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"SELECT rol_id, name, created_at FROM cassia_roles WHERE deleted_at IS NULL")
    roles = session.execute(statement)
    roles = pd.DataFrame(roles).replace(np.nan, "")
    if not roles.empty:
        permissions = []
        for ind in roles.index:
            permissions.append(get_permissions(roles['rol_id'][ind]))
        roles['permissions'] = permissions
        roles["id"] = roles["rol_id"]
    session.close()
    return success_response(data=roles.to_dict(orient="records"))


def get_user(user_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    user = session.query(User).filter(
        User.user_id == user_id, User.deleted_at == None
    ).first()
    if user is None:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    data_roles = get_roles_permissions(user_id)
    data = {
        "user_id": user.user_id,
        "mail": user.mail,
        "name": user.name,
        "created_at": user.created_at,
        "updated_": user.updated_at,
        "deleted_at": user.deleted_at,
        "roles": data_roles
    }
    session.close()
    return success_response(data=data)


def get_roles_permissions(user_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select role_id from user_has_roles where user_id={user_id}")
    roles = session.execute(statement)
    roles = pd.DataFrame(roles).replace(np.nan, "")
    if not roles.empty:
        if len(roles["role_id"]) > 1:
            rol_ids = tuple(roles['role_id'].values.tolist())
        else:
            rol_ids = f"({roles['role_id'][0]})"
        statement = text(
            f"select rol_id,name from cassia_roles where rol_id in{rol_ids} and deleted_at IS NULL")
        roles = session.execute(statement)
        roles = pd.DataFrame(roles).replace(np.nan, "")

        if not roles.empty:
            permissions = []
            for ind in roles.index:
                permissions.append(get_permissions(roles['rol_id'][ind]))
            roles['permissions'] = permissions
            session.close()
            return roles.to_dict(orient="records")
        else:
            session.close()
            return []
    else:
        session.close()
        return []


def get_permissions(role_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select permission_id,cassia_rol_id from role_has_permissions where cassia_rol_id={role_id}")
    permissions = session.execute(statement)
    permissions = pd.DataFrame(permissions).replace(np.nan, "")
    if len(permissions) > 0:
        permission_ids = permissions['permission_id'].values.tolist()
    else:
        permission_ids = [0]
    statement = text(
        f"select permission_id,module_name, name from cassia_permissions where permission_id in :ids")
    permissions = session.execute(statement, {'ids': permission_ids})
    permissions = pd.DataFrame(permissions).replace(np.nan, "")
    session.close()
    return permissions.to_dict(orient="records")


async def create_user(user: user_schema.UserRegister):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    get_user = session.query(User).filter(
        User.mail == user.mail
    ).first()
    # get_user = UserModel.filter((UserModel.email == user.email) | (
    #    UserModel.username == user.username)).first()
    if get_user and not get_user.deleted_at:
        msg = "Email already registered"
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=msg
        )
    try:
        roles = [int(role) for role in user.roles.split(",")]
        roles = set(roles)
    except:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The role_ids values are not a valid numbers"
        )
    statement = text(
        f"SELECT rol_id from cassia_roles where deleted_at IS NULL")
    roles_ids = session.execute(statement)
    roles_ids = pd.DataFrame(roles_ids)
    invalid_roles = []
    for role in roles:
        if role not in roles_ids.values:
            invalid_roles.append(role)
    if len(invalid_roles):
        invalid_roles = ','.join(str(e) for e in invalid_roles)
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The next role_ids values are not a valid role_id: {invalid_roles} "
        )
    password = create_password_v2(8)
    if get_user and get_user.deleted_at:
        get_user.mail = user.mail
        get_user.name = user.name
        get_user.deleted_at = None
        get_user.password = get_password_hash(password)
        session.commit()
        session.refresh(get_user)
        roles_actual = session.query(UserHasRole).filter(
            UserHasRole.user_id == get_user.user_id
        ).all()
        for role_model in roles_actual:
            session.delete(role_model)
        session.commit()
        db_user = get_user
    else:
        db_user = User(
            name=user.name,
            mail=user.mail,
            password=get_password_hash(password),
        )
        session.add(db_user)
        session.commit()
        session.refresh(db_user)

    authorizer = UserAuthorizer(
        user_id=db_user.user_id
    )
    session.add(authorizer)
    session.commit()
    session.refresh(authorizer)
    for role in roles:
        role_user = UserHasRole(
            user_id=db_user.user_id,
            role_id=role,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        session.add(role_user)
        session.commit()
        session.refresh(role_user)
    """  print(db_user.username, password) """
    url = ""
    if settings.env == "prod":
        url = f"{settings.cassia_server_ip}:8001/"
    else:
        url = f"{settings.cassia_server_ip}:8003/"

    body = {
        "name": db_user.name,
        "password": password,
        "url": url
    }
    print(url)
    session.close()
    await send_email(email_to=db_user.mail, body=body)
    # db_user.save()

    return success_response(message=f"User created", data=user_schema.User(
        user_id=db_user.user_id,
        name=db_user.name,
        mail=db_user.mail
    ))


def create_password(length):
    password = ''.join((secrets.choice(
        string.ascii_letters + string.digits + string.punctuation) for i in range(length)))
    return password


def create_password_v2(length):
    longitud = length
    caracteres = string.ascii_letters + string.digits
    password = ''
    while len(password) < longitud:
        password += secrets.choice(caracteres)
    return password


conf = ConnectionConfig(
    MAIL_USERNAME=settings.MAIL_USERNAME,
    MAIL_PASSWORD=settings.MAIL_PASSWORD,
    MAIL_FROM=settings.MAIL_FROM,
    MAIL_PORT=settings.MAIL_PORT,
    MAIL_SERVER=settings.MAIL_SERVER,
    MAIL_FROM_NAME=settings.MAIL_FROM_NAME,
    MAIL_STARTTLS=True,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=True,
    TEMPLATE_FOLDER='./static/templates/emails'
)


async def send_email(email_to: str, body: dict):

    message = MessageSchema(
        subject="Bienvenido a CASSIA",
        recipients=[email_to],
        template_body=body,
        subtype="html"
    )
    fm = FastMail(conf)
    print("sent mail")
    await fm.send_message(message, template_name="new_user.html")


async def update_user(user_id, user: user_schema.UserRegister):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    actual_user = session.query(User).filter(
        User.user_id == user_id, User.deleted_at == None).first()
    if not actual_user:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    get_user = session.query(User).filter(
        or_(User.mail == user.mail)
    ).first()
    if get_user and not actual_user.user_id == get_user.user_id:
        msg = "Email already registered"
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=msg
        )
    if user.roles:
        try:
            roles = [int(role) for role in user.roles.split(",")]
            roles = set(roles)
        except:
            session.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The role_ids values are not a valid numbers"
            )
        statement = text(
            f"SELECT rol_id from cassia_roles where deleted_at IS NULL")
        roles_ids = session.execute(statement)
        roles_ids = pd.DataFrame(roles_ids)
        invalid_roles = []
        for role in roles:
            if role not in roles_ids.values:
                invalid_roles.append(role)
        if len(invalid_roles):
            invalid_roles = ','.join(str(e) for e in invalid_roles)
            session.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"The next role_ids values are not a valid role_id: {invalid_roles} "
            )
    actual_user.name = user.name
    mail_nuevo = False
    if actual_user.mail != user.mail:
        actual_user.mail = user.mail
        password = create_password_v2(8)
        actual_user.password = get_password_hash(password)
        actual_user.verified_at = None
        url = ""

        if settings.env == "prod":
            url = f"{settings.cassia_server_ip}:8001/"
        else:
            url = f"{settings.cassia_server_ip}:8003/"

        body = {
            "username": actual_user.name,
            "password": password,
            "url": url
        }
        mail_nuevo = True
    actual_user.updated_at = datetime.now()
    session.commit()
    session.refresh(actual_user)
    roles_actual = session.query(UserHasRole).filter(
        UserHasRole.user_id == actual_user.user_id
    ).all()
    if user.roles:
        for role_model in roles_actual:
            session.delete(role_model)
        for role in roles:
            role_user = UserHasRole(
                user_id=actual_user.user_id,
                role_id=role,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(role_user)
            session.commit()
    if mail_nuevo:
        await send_email(email_to=actual_user.mail, body=body)

    if user.authorizer:
        authroizer = session.query(UserAuthorizer).filter(
            UserAuthorizer.user_id == actual_user.user_id).first()
        if user.authorizer == 0:
            if authroizer:
                session.delete(authroizer)
                session.commit()
        if user.authorizer:
            if not authroizer:
                authorizer_create = UserAuthorizer(
                    user_id=actual_user.user_id
                )
                session.add(authorizer_create)
                session.commit()
                session.refresh(authorizer_create)
    # db_user.save()
    user_response = user_schema.User(
        user_id=actual_user.user_id,
        name=actual_user.name,
        mail=actual_user.mail
    )
    session.close()
    return success_response(message=f"User updated successfully", data=user_response)


async def delete_user(user_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    actual_user = session.query(User).filter(User.user_id == user_id).first()
    if not actual_user:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    roles_actual = session.query(UserHasRole).filter(
        UserHasRole.user_id == actual_user.user_id
    ).all()
    for role_model in roles_actual:
        session.delete(role_model)
    actual_user.deleted_at = datetime.now()
    authorizer = session.query(UserAuthorizer).filter(
        UserAuthorizer.user_id == user_id).first()
    if authorizer:
        session.delete(authorizer)
    session.commit()
    session.refresh(actual_user)

    session.close()
    return success_response(message=f"User deleted successfully")


async def update_password(data: update_user_password.UpdateUserPassword, user_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    actual_user = session.query(User).filter(User.user_id == user_id).first()
    if not verify_password(data.old_password, actual_user.password):
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The old password is incorrectly"
        )
    actual_user.password = get_password_hash(data.new_password)
    actual_user.updated_at = datetime.now()
    if not actual_user.verified_at:
        actual_user.verified_at = datetime.now()
    session.commit()
    session.refresh(actual_user)
    session.close()
    return success_response(message=f"Password updated correctly")
