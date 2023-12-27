from fastapi import APIRouter
import services.zabbix.host_management_service as hosts_management_service
from fastapi import Depends, status, Path
from services import auth_service2
from schemas import host_management_schema
""" import services.zabbix.interface_service as interface_service """

host_management_router = APIRouter(prefix="/hosts_management")


@host_management_router.get(
    '/',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Get hosts",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts():
    return await hosts_management_service.get_hosts()


@host_management_router.get(
    '/protocols',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Get protocols",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_protocols():
    return await hosts_management_service.get_protocols()


@host_management_router.get(
    '/templates',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Get templates",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_templates():
    return await hosts_management_service.get_templates()


@host_management_router.get(
    '/groups',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Get groups",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_groups():
    return await hosts_management_service.get_groups()


@host_management_router.get(
    '/proxys',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Get proxys",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_groups():
    return await hosts_management_service.get_proxys()


@host_management_router.post(
    '/',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Create host",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_host(host_data: host_management_schema.HostManagementBase):
    return await hosts_management_service.create_host(host_data)


@host_management_router.put(
    '/{hostid}',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Update host",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_host(hostid, host_data: host_management_schema.HostManagementUpdate):
    return await hosts_management_service.update_host(hostid, host_data)


@host_management_router.get(
    '/{hostid}',
    tags=["Zabbix - Hosts Management "],
    status_code=status.HTTP_200_OK,
    summary="Get host by hostid",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_host(hostid):
    return await hosts_management_service.get_host(hostid)

""" @hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str = "", dispId: str = "", subtype_id: str = ""):
    return hosts_service.get_host_filter(municipalityId, dispId, subtype_id)


@hosts_router.post('/ping/{hostId}',
                   tags=["Zabbix - Hosts"],
                   status_code=status.HTTP_200_OK,
                   summary="Create a ping on a device",
                   dependencies=[Depends(auth_service2.get_current_user_session)])
def create_ping(hostId: int = Path(description="ID of Host", example="10596")):
    return interface_service.create_ping(hostId)
 """
