from fastapi import APIRouter
import services.zabbix.groups_service as group_service
from fastapi import Depends, status

from services import auth_service

groups_router = APIRouter()


@groups_router.get(
    '/groups/municipios',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all municipality",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_municipios():
    return group_service.get_municipios()


@groups_router.get(
    '/groups/devices',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_devices():
    return group_service.get_devices()


@groups_router.get(
    '/groups/technologies',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device technologies",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_technologies():
    return group_service.get_technologies()
