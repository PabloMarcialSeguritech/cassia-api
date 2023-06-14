from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from services.zabbix_service import ZabbixService
import services.zabbix_service as zabbix_service
from fastapi import Depends, status, Body
from typing import List
from services import auth_service

zabbix_router = APIRouter(prefix="/api/v1/zabbix")


""" @zabbix_router.get(
    '/api/municipios',
    tags=["Zabbix by api"],
    status_code=status.HTTP_200_OK,
    summary="Get all municipies",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_municios():
    zabbix_service = ZabbixService()
    data = zabbix_service.get_municipios()
    return JSONResponse(content=jsonable_encoder(data))
 """


@zabbix_router.get(
    '/db/municipios',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all municipality",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_municipios():
    return zabbix_service.get_municipios()


@zabbix_router.get(
    '/db/devices',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device types",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_devices():
    return zabbix_service.get_devices()


@zabbix_router.get(
    '/db/technologies',
    tags=["Zabbix - Groups"],
    status_code=status.HTTP_200_OK,
    summary="Get all device technologies",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_technologies():
    return zabbix_service.get_technologies()


@zabbix_router.get(
    '/db/problems/{municipalityId}',
    tags=["Zabbix - Problems(Alerts)"],
    status_code=status.HTTP_200_OK,
    summary="Get problems by municipality ID, technology, and dispId",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_problems_filter(municipalityId: str, tech: str = "", hostType: str = ""):
    return zabbix_service.get_problems_filter(municipalityId, tech, hostType)


@zabbix_router.get(
    '/db/hosts/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology, and dispId",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_hosts_filter(municipalityId: str, tech: str = "", hostType: str = ""):
    return zabbix_service.get_host_filter(municipalityId, tech, hostType)
