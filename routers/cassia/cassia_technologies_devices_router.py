from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_host_device_tech_schema
from typing import List, Optional
from services.cassia import cassia_host_tech_devices_service
from dependencies import get_db
from infraestructure.database import DB

cassia_tech_devices_router = APIRouter(prefix="/tech_devices")

@cassia_tech_devices_router.get(
    "/get_technologies",
    tags=["Cassia - Technologies Devices"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los tipos de dispositivos de los host de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_tech_devices_cat(db: DB = Depends(get_db)):
    return await cassia_host_tech_devices_service.get_technologies(db)

@cassia_tech_devices_router.put('/update_technology',
    tags=["Cassia - Technologies Devices"],
    status_code=status.HTTP_200_OK,
    summary="Actualiza la tecnología de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def update_technology_cat(tecnology_data: cassia_host_device_tech_schema.CassiaHostDeviceTechSchema, db: DB = Depends(get_db)):
    return await cassia_host_tech_devices_service.update_technology(tecnology_data, db)


@cassia_tech_devices_router.post(
    "/export",
    tags=["Cassia - Technologies Devices"],
    status_code=status.HTTP_200_OK,
    summary="Exporta los tipos de tecnologías de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_groups_data(export_data: cassia_host_device_tech_schema.CassiaHostDeviceTechExportSchema, db: DB = Depends(get_db)):
    return await cassia_host_tech_devices_service.export_technologies_data(export_data, db)