from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from services.zabbix.hosts_service import ZabbixService
import services.zabbix.hosts_service as hosts_service
from fastapi import Depends, status, Body
from typing import List
from services import auth_service
from services import auth_service2


zabbix_router = APIRouter(prefix="/api/v1/zabbix")


@zabbix_router.get(
    '/db/hosts/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology, and dispId",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str, tech: str = "", hostType: str = ""):
    return hosts_service.get_host_filter(municipalityId, tech, hostType)


@zabbix_router.get(
    "/db/hosts/relations/{municipalityId}",
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host corelations filtered by municipality ID",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_host_relations(municipalityId: str):
    return hosts_service.get_host_correlation_filter(municipalityId)
