from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from services.integration.reset_service_service_impl import ResetServiceImpl

resets_router = APIRouter(prefix="/resets")


@resets_router.post(
    '/restart',
    tags=["Cassia - Resets"],
    status_code=status.HTTP_200_OK,
    summary="Restart a Reset",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_maintenance_async(objectId: str = ""):
    reset_service = ResetServiceImpl()
    result = await reset_service.restart_reset(objectId)
    return result
