from fastapi import APIRouter
from services.zabbix import proxies_service
from fastapi import Depends, status, UploadFile, File
from services import auth_service2
from dependencies import get_db
from infraestructure.database import DB
from schemas import cassia_proxies_schema
proxies_router = APIRouter(prefix="/proxies")


@proxies_router.get(
    '/',
    tags=["Zabbix - Proxies"],
    status_code=status.HTTP_200_OK,
    summary="Get proxies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_proxies(db: DB = Depends(get_db)):
    return await proxies_service.get_proxies(db)


@proxies_router.post(
    '/',
    tags=["Zabbix - Proxies"],
    status_code=status.HTTP_200_OK,
    summary="Create Proxy",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_proxy(proxy_data: cassia_proxies_schema.CassiaProxiesSchema, db: DB = Depends(get_db)):
    return await proxies_service.create_proxy(proxy_data, db)


@proxies_router.post(
    '/export',
    tags=["Zabbix - Proxies", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Export proxies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_proxies(proxy_data_export: cassia_proxies_schema.CassiaProxiesExportSchema, db: DB = Depends(get_db)):
    return await proxies_service.export_proxies(proxy_data_export, db)



@proxies_router.post(
    '/import',
    tags=["Zabbix - Proxies", "CASSIA Imports"],
    status_code=status.HTTP_200_OK,
    summary="Importa los proxies con un archivo proporcionado",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def import_proxies(file_import: UploadFile = File(...), db: DB = Depends(get_db)):
    return await proxies_service.import_proxies(file_import, db)

@proxies_router.delete(
    "/{proxyid}",
    tags=["Zabbix - Proxies"],
    status_code=status.HTTP_200_OK,
    summary="Eliminar un Proxy",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_proxy(proxyid: int, db: DB = Depends(get_db)):
    return await proxies_service.delete_proxy(proxyid, db)
