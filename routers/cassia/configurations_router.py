from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service
from services import auth_service2

import services.cassia.configurations_service as config_service
configuration_router = APIRouter(prefix="/configuration")


@configuration_router.get(
    '/',
    tags=["Cassia - Configuration"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA configurations",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_role():
    return await config_service.get_configuration()


@configuration_router.get(
    '/estados',
    tags=["Cassia - Configuration - Estados"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA estados",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_estados():
    return await config_service.get_estados()


@configuration_router.get(
    '/estados/ping/{id_estado}',
    tags=["Cassia - Configuration - Estados"],
    status_code=status.HTTP_200_OK,
    summary="Ping to cassia state",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def ping_estado(id_estado: str):
    return await config_service.ping_estado(id_estado)
