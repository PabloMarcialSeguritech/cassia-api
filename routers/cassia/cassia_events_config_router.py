from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from schemas import cassia_ci_mail
from services.cassia import cassia_technologies_service
from services.cassia import cassia_events_config_service
from schemas import cassia_technologies_schema
from schemas import cassia_auto_action_schema
cassia_events_config_router = APIRouter(prefix="/events/config")


@cassia_events_config_router.get(
    '/',
    tags=["Cassia - Events - Config"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia events config",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_events_config():
    return await cassia_events_config_service.get_events_config()
