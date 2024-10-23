from datetime import date, timedelta
from fastapi import APIRouter, Depends, status, Query
from services.cassia import cassia_user_groups_service
from infraestructure.database import DB
from dependencies import get_db
from services import auth_service2
from typing import Optional
from schemas import cassia_user_group_schema
from datetime import datetime

cassia_user_groups_router = APIRouter(prefix="/user_groups")


@cassia_user_groups_router.get(
    "/",
    tags=["Cassia - Users Groups"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los grupos de usuarios",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_audit_logs(db: DB = Depends(get_db)):

    return await cassia_user_groups_service.get_user_groups(db)


@cassia_user_groups_router.post(
    "/",
    tags=["Cassia - Users Groups"],
    status_code=status.HTTP_200_OK,
    summary="Crea un registro de grupo de usuarios",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_audit_log(user_group_data: cassia_user_group_schema.CassiaUserGroupSchema, db : DB = Depends(get_db)):
    return await cassia_user_groups_service.create_user_group(user_group_data, db)


@cassia_user_groups_router.put(
    "/{user_group_id}",
    tags=["Cassia - Users Groups"],
    status_code=status.HTTP_200_OK,
    summary="Actualiza un grupo de usuarios",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def update_brand(user_group_id: int, user_group_data: cassia_user_group_schema.CassiaUserGroupSchema, db: DB = Depends(get_db)):
    return await cassia_user_groups_service.modify_user_group(user_group_id, user_group_data, db)

