from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service
from fastapi import Body
from models.user_model import User
from fastapi import File, UploadFile, Form
from typing import Optional
from services import auth_service2
import services.zabbix.layers_service as layers_service

layers_router = APIRouter(prefix="/layers")


@layers_router.get(
    "/aps/{municipality_id}",
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get aps layer by municipality ID",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_aps(municipality_id=0):
    return layers_service.get_aps_layer(municipality_id)


@layers_router.get(
    '/downs/{municipalityId}',
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get host with status down by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str = "0", dispId: str = "", subtype_id: str = ""):
    return layers_service.get_downs_layer(municipalityId, dispId, subtype_id)


@layers_router.get(
    '/carreteros/{municipalityId}',
    tags=["Zabbix - Layers"],
    status_code=status.HTTP_200_OK,
    summary="Get traffic info",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str = "0"):
    return layers_service.get_carreteros2(municipalityId)
