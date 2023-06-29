from fastapi import APIRouter
import services.zabbix.hosts_service as hosts_service
from fastapi import Depends, status
from services import auth_service

hosts_router = APIRouter(prefix="/hosts")


@hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology, and dispId",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_hosts_filter(municipalityId: str, tech: str = "", hostType: str = ""):
    return hosts_service.get_host_filter(municipalityId, tech, hostType)


@hosts_router.get(
    "/relations/{municipalityId}",
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host corelations filtered by municipality ID",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_host_relations(municipalityId: str):
    return hosts_service.get_host_correlation_filter(municipalityId)
