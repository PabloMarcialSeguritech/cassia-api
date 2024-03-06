from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_Prueba
from sqlalchemy import text, or_
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from models.user_model import User
from models.user_has_roles import UserHasRole
from models.cassia_roles import CassiaRole
from models.cassia_permissions import CassiaPermission
from models.role_has_permissions import RoleHasPermission
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
import schemas.cassia_role_schema as cassia_role_schema

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


def get_roles():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    roles = session.query(CassiaRole).filter(
        CassiaRole.deleted_at == None).all()
    roles = pd.DataFrame(roles).replace(np.nan, "")
    if len(roles) > 0:
        roles["id"] = roles["rol_id"]
    session.close()

    return success_response(data=roles)


def get_role(role_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    role = session.query(CassiaRole).filter(
        CassiaRole.deleted_at == None, CassiaRole.rol_id == role_id).first()
    if not role:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    session.close()
    return success_response(data=role)


def get_permissions():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    permissions = text(
        f"SELECT * FROM cassia_permissions where deleted_at IS NULL")
    permissions = session.execute(permissions)
    permissions = pd.DataFrame(permissions).replace(np.nan, "")
    if len(permissions) > 0:
        print(permissions.head())
        permissions["id"] = permissions["permission_id"]
    session.close()
    return success_response(data=permissions.to_dict(orient="records"))


async def create_role(role: cassia_role_schema.RoleRegister):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    get_role = session.query(CassiaRole).filter(
        or_(CassiaRole.name == role.name)
    ).first()
    # get_user = UserModel.filter((UserModel.email == user.email) | (
    #    UserModel.username == user.username)).first()
    if get_role and not get_role.deleted_at:
        session.close()
        msg = "Role name is registered"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=msg
        )
    try:
        permissions = [int(permission)
                       for permission in role.permissions.split(",")]
        permissions = set(permissions)
    except:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The permissions_ids values are not a valid numbers"
        )
    statement = text(
        f"SELECT permission_id from cassia_permissions where deleted_at IS NULL")
    permissions_ids = session.execute(statement)
    permissions_ids = pd.DataFrame(permissions_ids)
    invalid_permissions = []
    for permission in permissions:
        if permission not in permissions_ids.values:
            invalid_permissions.append(permission)
    if len(invalid_permissions):
        invalid_permissions = ','.join(str(e) for e in invalid_permissions)
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The next permissions_ids values are not a valid role_id: {invalid_permissions} "
        )
    db_role = CassiaRole(
        name=role.name,
        description=role.description,
    )
    session.add(db_role)
    session.commit()
    """ session.refresh(db_role) """
    for permission in permissions:
        role_has_permission = RoleHasPermission(
            permission_id=permission,
            cassia_rol_id=db_role.rol_id,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        session.add(role_has_permission)
        session.commit()
        """ session.refresh(role_has_permission) """
    data = {
        'rol_id': db_role.rol_id,
        'name': db_role.name,
        'description': db_role.description
    }
    session.close()
    return success_response(message=f"Role created", data=data)


async def update_role(role_id: int, role: cassia_role_schema.RoleRegister):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    actual_role = session.query(CassiaRole).filter(
        CassiaRole.rol_id == role_id,
        CassiaRole.deleted_at == None).first()
    if not actual_role:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )

    if role.permissions:
        try:
            permissions = [int(permission)
                           for permission in role.permissions.split(",")]
            permissions = set(permissions)
        except:
            session.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The permissions_ids values are not a valid numbers"
            )
        statement = text(
            f"SELECT permission_id from cassia_permissions where deleted_at IS NULL")
        permissions_ids = session.execute(statement)
        permissions_ids = pd.DataFrame(permissions_ids)
        invalid_permissions = []
        for permission in permissions:
            if permission not in permissions_ids.values:
                invalid_permissions.append(permission)
        if len(invalid_permissions):
            invalid_permissions = ','.join(str(e) for e in invalid_permissions)
            session.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"The next permissions_ids values are not a valid permission_id: {invalid_permissions} "
            )
    actual_role.name = role.name
    actual_role.description = role.description
    actual_role.updated_at = datetime.now()
    session.commit()
    session.refresh(actual_role)
    permissions_actual = session.query(RoleHasPermission).filter(
        RoleHasPermission.cassia_rol_id == actual_role.rol_id
    ).all()
    if role.permissions:
        for permission_model in permissions_actual:
            session.delete(permission_model)
        for permission in permissions:
            role_has_permission = RoleHasPermission(
                permission_id=permission,
                cassia_rol_id=actual_role.rol_id,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(role_has_permission)
            session.commit()
            """ session.refresh(role_has_permission) """
    data = {
        'rol_id': actual_role.rol_id,
        'name': actual_role.name,
        'description': actual_role.description
    }
    session.close()
    return success_response(message=f"Role updated successfully", data=data)


async def delete_role(role_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    actual_role = session.query(CassiaRole).filter(
        CassiaRole.rol_id == role_id, CassiaRole.deleted_at == None).first()
    if not actual_role:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found"
        )
    permissions_actual = session.query(RoleHasPermission).filter(
        RoleHasPermission.cassia_rol_id == actual_role.rol_id,
        RoleHasPermission.deleted_at == None
    ).all()
    for cassia_has_roles in permissions_actual:
        session.delete(cassia_has_roles)
    actual_role.deleted_at = datetime.now()
    session.commit()
    session.refresh(actual_role)
    session.close()
    return success_response(message=f"Role deleted successfully")
