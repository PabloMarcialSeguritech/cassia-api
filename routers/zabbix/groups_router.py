from fastapi import APIRouter
import services.zabbix.groups_service as group_service
from fastapi import Depends, status

from services import auth_service
from services import auth_service2

groups_router = APIRouter()


@groups_router.get(
    '/groups/municipios_old',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_municipios():
    return group_service.get_municipios()


@groups_router.get(
    '/groups/municipios',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_municipios_async():
    return await group_service.get_municipios_async()


@groups_router.get(
    '/groups/devices_old',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_devices():
    return group_service.get_devices()


@groups_router.get(
    '/groups/devices',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices_async():
    return await group_service.get_devices_async()


@groups_router.get(
    '/groups/devices_old/{municipalityId}',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies by municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices(municipalityId=0):
    return await group_service.get_devices_by_municipality(municipalityId)


@groups_router.get(
    '/groups/devices/{municipalityId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies by municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices_async(municipalityId=0):
    return await group_service.get_devices_by_municipality_async(municipalityId)


@groups_router.get(
    '/groups/devices_old/map/{municipalityId}',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies by municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices(municipalityId=0):
    return await group_service.get_devices_by_municipality_map(municipalityId)


@groups_router.get(
    '/groups/devices/map/{municipalityId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types and technologies by municipality",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices_async(municipalityId=0):
    return await group_service.get_devices_by_municipality_map_async(municipalityId)


@groups_router.get(
    '/groups/subtypes_old/',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all device subtypes",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_subtypes():
    return group_service.get_subtypes("0")


@groups_router.get(
    '/groups/subtypes/',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device subtypes",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_subtypes_async():
    return await group_service.get_subtypes_async(0)


@groups_router.get(
    '/groups/subtypes_old/{techId}',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all device subtypes",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_subtypes(techId):
    return group_service.get_subtypes(techId)


@groups_router.get(
    '/groups/subtypes/{techId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device subtypes",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_subtypes_async(techId):
    return await group_service.get_subtypes_async(techId)


@groups_router.get(
    '/groups/brands_old/{techId}',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all device brands by tech",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_brands(techId):
    return group_service.get_brands(techId)


@groups_router.get(
    '/groups/brands/{techId}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device brands by tech",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_brands_async(techId):
    return await group_service.get_brands_async(techId)


@groups_router.get(
    '/groups/models_old/{brand_id}',
    tags=["Zabbix - Groups - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get all device models by brand",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_models(brand_id):
    return group_service.get_models(brand_id)


@groups_router.get(
    '/groups/models/{brand_id}',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device models by brand",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_models_async(brand_id):
    return await group_service.get_models_async(brand_id)
