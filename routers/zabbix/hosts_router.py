from fastapi import APIRouter
import services.zabbix.hosts_service as hosts_service
from fastapi import Depends, status, Path
from services import auth_service
import services.zabbix.interface_service as interface_service

hosts_router = APIRouter(prefix="/hosts")


@hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_hosts_filter(municipalityId: str, dispId: str = "", subtype_id: str = ""):
    return hosts_service.get_host_filter(municipalityId, dispId, subtype_id)


@hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_hosts_filter(municipalityId: str, dispId: str = "", subtype_id: str = ""):
    return hosts_service.get_host_filter(municipalityId, dispId, subtype_id)


@hosts_router.get(
    "/relations/{municipalityId}",
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host corelations filtered by municipality ID",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_host_relations(municipalityId: str):
    return hosts_service.get_host_correlation_filter(municipalityId)


@hosts_router.post('/ping/{hostId}',
                   tags=["Zabbix - Hosts"],
                   status_code=status.HTTP_200_OK,
                   summary="Create a ping over a device",
                   dependencies=[Depends(auth_service.get_current_user)])
def get_hosts_filter(hostId: int = Path(description="ID of Host", example="10596")):
    return interface_service.create_ping(hostId)
