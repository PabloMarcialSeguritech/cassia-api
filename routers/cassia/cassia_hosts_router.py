from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_hosts_schema
from typing import List, Optional
from services.cassia import cassia_hosts_service
from dependencies import get_db
from infraestructure.database import DB

cassia_hosts_router = APIRouter(prefix="/hosts")


@cassia_hosts_router.get(
    "/{hostid}",
    tags=["Cassia Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene un host de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host(hostid, db: DB = Depends(get_db)):
    return await cassia_hosts_service.get_host(hostid, db)


@cassia_hosts_router.get(
    "/",
    tags=["Cassia Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los hosts de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts(db: DB = Depends(get_db)):
    return await cassia_hosts_service.get_hosts(db)


@cassia_hosts_router.post(
    "/",
    tags=["Cassia Hosts", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Crea un host de Zabbix",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_host(host_data: cassia_hosts_schema.CassiaHostUpdateSchema, db: DB = Depends(get_db)):
    return await cassia_hosts_service.create_host(host_data, db)


@cassia_hosts_router.put(
    "/{hostid}",
    tags=["Cassia Hosts", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Edita un host de Zabbix",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_host_data(hostid: int, host_data: cassia_hosts_schema.CassiaHostUpdateSchema, db: DB = Depends(get_db)):
    return await cassia_hosts_service.update_hosts_data(hostid, host_data, db)


@cassia_hosts_router.post(
    "/export",
    tags=["Cassia Hosts", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Exporta los hosts de Zabbix",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_hosts_data(export_data: cassia_hosts_schema.CassiaHostExportSchema, db: DB = Depends(get_db)):
    return await cassia_hosts_service.export_hosts_data(export_data, db)


@cassia_hosts_router.post(
    "/import",
    tags=["Cassia Hosts", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Importa los hosts de Zabbix",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def import_hosts_data(file_import: UploadFile = File(...), db: DB = Depends(get_db)):
    return await cassia_hosts_service.import_hosts_data(file_import, db)


@cassia_hosts_router.delete(
    "/{hostid}",
    tags=["Cassia Hosts", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Elimina un host de Zabbix",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_host(hostid: int, db: DB = Depends(get_db)):
    return await cassia_hosts_service.delete_host(hostid, db)
