from fastapi import APIRouter, Query
from fastapi import Depends, status
from services import auth_service
from services import auth_service2
from fastapi.responses import FileResponse
import services.cassia.host_correlation_service as host_correlation_service
from models.user_model import User
from fastapi import UploadFile
host_correlation_router = APIRouter(prefix="/host_correlation")


@host_correlation_router.get(
    '/',
    tags=["Cassia - Host - Correlations"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA host correlations",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_correlations(page: int = Query(1, ge=1), page_size: int = Query(10, le=4000)):
    return host_correlation_service.get_correlations(page, page_size)


@host_correlation_router.get(
    '/download_file',
    tags=["Cassia - Host - Correlations"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA hosts without correlations",
    dependencies=[Depends(auth_service2.get_current_user_session)],
    response_class=FileResponse
)
async def get_hosts_without_relations():
    return await host_correlation_service.get_hosts_without_relations()


@host_correlation_router.get(
    '/layouts/ejemplo/download',
    tags=["Cassia - Host - Correlations"],
    status_code=status.HTTP_200_OK,
    summary="Return example layout",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def download_file():
    return await host_correlation_service.download_file()


@host_correlation_router.post(
    '/upload_file',
    tags=["Cassia - Host - Correlations"],
    status_code=status.HTTP_200_OK,
    summary="Create correlations with a csv file",
    dependencies=[Depends(auth_service2.get_current_user_session)],
    response_class=FileResponse
)
async def create_message(file: UploadFile, current_user: User = Depends(auth_service2.get_current_user_session)):
    return await host_correlation_service.process_file(current_user_session=current_user, file=file)
