from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service
from fastapi import Body
from models.user_model import User
from fastapi import File, UploadFile, Form
from typing import Optional
from services import auth_service2
import services.zabbix.layers_service as layers_service
from dependencies import get_db
from infraestructure.database import DB

layers_router = APIRouter(prefix="/layers")


@layers_router.get(
    "/aps_old/{municipality_id}",
    tags=["Zabbix - Layers - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get towers layer by municipality ID",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_aps(municipality_id=0):
    return await layers_service.get_aps_layer()


@layers_router.get(
    "/aps/{municipality_id}",
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get towers layer by municipality ID",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_aps_async(municipality_id=0):
    return await layers_service.get_aps_layer_async()


@layers_router.get(
    "/towers_old",
    tags=["Zabbix - Layers - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get towers layer by municipality ID",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_towers():
    return await layers_service.get_aps_layer()


@layers_router.get(
    "/towers",
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get towers layer by municipality ID",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_towers_async():
    return await layers_service.get_aps_layer_async()


@layers_router.get(
    '/downs_old/{municipalityId}',
    tags=["Zabbix - Layers - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get host with status down by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts_filter(municipalityId: str = "0", dispId: str = "", subtype_id: str = ""):
    return await layers_service.get_downs_layer(municipalityId, dispId, subtype_id)


@layers_router.get(
    '/downs/{municipalityId}',
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get host with status down by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts_filter_async(municipalityId: str = "0", dispId: str = "", subtype_id: str = ""):
    return await layers_service.get_downs_layer_async(municipalityId, dispId, subtype_id)


@layers_router.get(
    '/carreteros_old/{municipalityId}',
    tags=["Zabbix - Layers - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get traffic info",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str = "0"):
    return layers_service.get_carreteros2(municipalityId)


@layers_router.get(
    '/carreteros/{municipalityId}',
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get traffic info",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts_filter_async(municipalityId: str = "0"):
    return await layers_service.get_carreteros2_async(municipalityId)


@layers_router.get(
    '/lpr_old/{municipalityId}',
    tags=["Zabbix - Layers - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get traffic info",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str = "0"):
    return layers_service.get_lpr(municipalityId)


@layers_router.get(
    '/lpr/{municipalityId}',
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get traffic info",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts_filter_asyn(municipalityId: str = "0"):
    return await layers_service.get_lpr_async(municipalityId)


@layers_router.get(
    '/switches_connectivity_old/{municipality_id}',
    tags=["Zabbix - Layers - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get switches connectivity info",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_switches_connectivity(municipality_id="0"):
    return await layers_service.get_switches_connectivity(municipality_id)


@layers_router.get(
    '/switches_connectivity/{municipality_id}',
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get switches connectivity info",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_switches_connectivity_async(municipality_id="0"):
    return await layers_service.get_switches_connectivity_async(municipality_id)


@layers_router.get(
    '/downs/origen_old/{municipalityId}',
    tags=["Zabbix - Layers - Old"],
    status_code=status.HTTP_200_OK,
    summary="Get host with status down by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts_filter(municipalityId: str = "0", dispId: str = "", subtype_id: str = ""):
    return await layers_service.get_downs_origin_layer(municipalityId, dispId, subtype_id)


@layers_router.get(
    '/downs/origen/{municipalityId}',
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get host with status down by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_hosts_filter_async(municipalityId: str = "0", dispId: str = "", subtype_id: str = "", lenght: int = 500, db=Depends(get_db)):
    return await layers_service.get_downs_origin_layer_async(municipalityId, dispId, subtype_id, lenght, db)
