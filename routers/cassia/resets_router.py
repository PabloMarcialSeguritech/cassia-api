from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from services.integration.reset_service_service_impl import ResetServiceImpl
from models.cassia_user_session import CassiaUserSession
resets_router = APIRouter(prefix="/resets")


@resets_router.post(
    '/reset_pmi',
    tags=["Cassia - Resets"],
    status_code=status.HTTP_200_OK,
    summary="Restart a Reset",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def reset_pmi_async(affiliation: str = "", current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    reset_service = ResetServiceImpl()
    result = await reset_service.reset(affiliation, current_user_session)
    return result
