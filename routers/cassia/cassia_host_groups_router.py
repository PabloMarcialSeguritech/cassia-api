from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from typing import List, Optional
from services.cassia import cassia_host_groups_service
from dependencies import get_db
from infraestructure.database import DB

cassia_host_groups_router = APIRouter(prefix="/host_groups")


@cassia_host_groups_router.get(
    "/",
    tags=["Host Groups"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene la los tipos de hosts de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_groups(db: DB = Depends(get_db)):
    return await cassia_host_groups_service.get_host_groups(db)
