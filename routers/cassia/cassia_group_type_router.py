from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from typing import List, Optional
from services.cassia import cassia_group_types_service
from dependencies import get_db
from infraestructure.database import DB

cassia_group_type_router = APIRouter(prefix="/group_types")


@cassia_group_type_router.get(
    "/",
    tags=["Group Types"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene la los tipos de grupos de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_group_types(db: DB = Depends(get_db)):
    return await cassia_group_types_service.get_group_types(db)
