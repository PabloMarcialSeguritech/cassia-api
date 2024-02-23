from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from services.cassia import diagnosta_service
diagnosta_router = APIRouter(prefix="/diagnosta")


@diagnosta_router.get(
    '/{hostid_or_ip}',
    tags=["Cassia - Diagnosta"],
    status_code=status.HTTP_200_OK,
    summary="Get actions catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def analize_host(hostid_or_ip: str):
    return await diagnosta_service.analize_host(hostid_or_ip)
