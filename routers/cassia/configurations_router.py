from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service
import services.cassia.configurations_service as config_service
configuration_router = APIRouter(prefix="/configuration")


@configuration_router.get(
    '/',
    tags=["Cassia - Configuration"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA configurations",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_role():
    return config_service.get_configuration()
