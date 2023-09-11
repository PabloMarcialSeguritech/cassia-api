from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service
from fastapi.responses import FileResponse
import services.cassia.host_correlation_service as host_correlation_service
host_correlation_router = APIRouter(prefix="/host_correlation")


@host_correlation_router.get(
    '/',
    tags=["Cassia - Host - Correlations"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA host correlations",
    dependencies=[Depends(auth_service.get_current_user)]
)
async def get_role():
    return host_correlation_service.get_correlations()


@host_correlation_router.get(
    '/download_file',
    tags=["Cassia - Host - Correlations"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA hosts without correlations",
    dependencies=[Depends(auth_service.get_current_user)],
    response_class=FileResponse
)
async def get_hosts_without_relations():
    return await host_correlation_service.get_hosts_without_relations()
