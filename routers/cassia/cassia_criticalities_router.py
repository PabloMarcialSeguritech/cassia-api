from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from schemas import cassia_ci_mail
from services.cassia import cassia_criticalities_service
from schemas import cassia_criticality_schema
from schemas import cassia_auto_action_schema

cassia_criticalities_router = APIRouter(prefix="/criticalities")


@cassia_criticalities_router.get(
    '/',
    tags=["Cassia - Criticalities"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia criticalities catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_criticalities():
    return await cassia_criticalities_service.get_criticalities()


@cassia_criticalities_router.get(
    '/{cassia_criticality_id}',
    tags=["Cassia - Criticalities"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia criticality by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_criticality(cassia_criticality_id):
    return await cassia_criticalities_service.get_criticality(cassia_criticality_id)


@cassia_criticalities_router.post(
    '/',
    tags=["Cassia - Criticalities"],
    status_code=status.HTTP_200_OK,
    summary="Create cassia technology register",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_criticality(level: int = Form(...), name: str = Form(...), description: str = Form(...), icon: Optional[UploadFile] = File(None)):
    criticality_data = cassia_criticality_schema.CassiaCriticalitySchema(
        level=level, name=name, description=description)

    return await cassia_criticalities_service.create_criticality(criticality_data, icon)


@cassia_criticalities_router.put(
    '/{cassia_criticality_id}',
    tags=["Cassia - Criticalities"],
    status_code=status.HTTP_200_OK,
    summary="Update cassia technology register",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_criticality(cassia_criticality_id, level: int = Form(...), name: str = Form(...), description: str = Form(...), icon: Optional[UploadFile] = File(None)):
    criticality_data = cassia_criticality_schema.CassiaCriticalitySchema(
        level=level, name=name, description=description)
    return await cassia_criticalities_service.update_criticality(cassia_criticality_id, criticality_data, icon)


@cassia_criticalities_router.delete(
    '/{cassia_criticality_id}',
    tags=["Cassia - Criticalities"],
    status_code=status.HTTP_200_OK,
    summary="Delete cassia technology register by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_criticality(cassia_criticality_id):
    return await cassia_criticalities_service.delete_criticality(cassia_criticality_id)
