from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import discovered_device_schema
from typing import List, Optional
from services.cassia import cassia_hosts_discovery_service
from dependencies import get_db
from infraestructure.database import DB

cassia_hosts_discovery_router = APIRouter(prefix="/hosts/discovery")


@cassia_hosts_discovery_router.post(
    "/",
    tags=["Cassia Hosts Discovery"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene un host de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_discovered_devices(proxies: List[discovered_device_schema.ProxyRequest], db: DB = Depends(get_db)):
    return await cassia_hosts_discovery_service.get_discovered_devices(proxies, db)


@cassia_hosts_discovery_router.post(
    "/download",
    tags=["Cassia Hosts Discovery", "Cassia Exports"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene un host de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def download_hosts(proxies: List[discovered_device_schema.ProxyRequest], db: DB = Depends(get_db)):
    return await cassia_hosts_discovery_service.download_discovery_devices(proxies, db)
