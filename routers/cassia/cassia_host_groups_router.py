from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_host_groups_schema
from typing import List, Optional
from services.cassia import cassia_host_groups_service
from dependencies import get_db
from infraestructure.database import DB
from models.cassia_user_session import CassiaUserSession

cassia_host_groups_router = APIRouter(prefix="/host_groups")


@cassia_host_groups_router.get(
    "/",
    tags=["Host Groups"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene la los tipos de hosts de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_groups(db: DB = Depends(get_db)):
    return await cassia_host_groups_service.get_host_groups(db)


@cassia_host_groups_router.post(
    "/",
    tags=["Host Groups"],
    status_code=status.HTTP_200_OK,
    summary="Crea un host group de Zabbix con tipado de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_host_group(group_data: cassia_host_groups_schema.CassiaHostGroupSchema = Body(..., exclude={"groupid"}),
                           db: DB = Depends(get_db), current_user: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    return await cassia_host_groups_service.create_host_group(db, group_data, current_user)


@cassia_host_groups_router.delete(
    "/{groupid}",
    tags=["Host Groups"],
    status_code=status.HTTP_200_OK,
    summary="Eliminar un host group de Zabbix",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_host_group(groupid: int, db: DB = Depends(get_db), current_user: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    return await cassia_host_groups_service.delete_host_group(groupid, current_user, db)


@cassia_host_groups_router.post(
    "/export",
    tags=["Host Groups", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Exporta los grupos de host de Zabbix",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_groups_data(export_data: cassia_host_groups_schema.CassiaHostGroupExportSchema, db: DB = Depends(get_db)):
    return await cassia_host_groups_service.export_groups_data(export_data, db)


@cassia_host_groups_router.post(
    "/import",
    tags=["Host Groups", "CASSIA Imports"],
    status_code=status.HTTP_200_OK,
    summary="Importa los grupos de host de Zabbix con un archivo proporcionado",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def import_groups_data(file_import: UploadFile = File(...), db: DB = Depends(get_db)):
    return await cassia_host_groups_service.import_groups_data(file_import, db)


@cassia_host_groups_router.put('/',
                               tags=["Host Groups"],
                               status_code=status.HTTP_200_OK,
                               summary="Actualiza el host group de CASSIA (nombre y tipo)",
                               dependencies=[Depends(auth_service2.get_current_user_session)])
async def update_host_group(group_data: cassia_host_groups_schema.CassiaHostGroupSchema,
                            current_user: CassiaUserSession = Depends(auth_service2.get_current_user_session), db: DB = Depends(get_db)):
    return await cassia_host_groups_service.update_host_group(group_data, current_user, db)
