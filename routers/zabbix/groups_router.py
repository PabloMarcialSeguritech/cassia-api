from fastapi import APIRouter
import services.zabbix.groups_service as group_service
from fastapi import Depends, status

from services import auth_service
from services import auth_service2

groups_router = APIRouter()


@groups_router.get(
    '/groups/municipios',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_municipios():
    return group_service.get_municipios()


@groups_router.get(
    '/groups/devices',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_devices():
    return group_service.get_devices()


@groups_router.get(
    '/groups/devices/{municipalityId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies by municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices(municipalityId=0):
    return await group_service.get_devices_by_municipality(municipalityId)


@groups_router.get(
    '/groups/devices/map/{municipalityId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies by municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices(municipalityId=0):
    return await group_service.get_devices_by_municipality_map(municipalityId)


@groups_router.get(
    '/groups/subtypes/',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device subtypes",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_subtypes():
    return group_service.get_subtypes("0")


@groups_router.get(
    '/groups/subtypes/{techId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device subtypes",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_subtypes(techId):
    return group_service.get_subtypes(techId)


@groups_router.get(
    '/groups/brands/{techId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device brands by tech",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_subtypes(techId):
    return group_service.get_brands(techId)


@groups_router.get(
    '/groups/models/{brand_id}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device models by brand",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_subtypes(brand_id):
    return group_service.get_models(brand_id)
