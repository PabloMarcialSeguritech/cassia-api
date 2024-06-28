from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from schemas import cassia_ci_mail
from services.cassia import cassia_technologies_service
from schemas import cassia_technologies_schema
from schemas import cassia_auto_action_schema
cassia_technologies_router = APIRouter(prefix="/technologies")


""" @cassia_technologies_router.get(
    '/',
    tags=["Cassia - Technologies"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia technologies catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_technologies():
    return await cassia_technologies_service.get_technologies()
 """


@cassia_technologies_router.get(
    '/',
    tags=["Cassia - Technologies Portfolio"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia technologies catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_technologies():
    return await cassia_technologies_service.get_technologies()


@cassia_technologies_router.get(
    '/{cassia_technology_id}',
    tags=["Cassia - Technologies Portfolio"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia technologies by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_technology(cassia_technology_id):
    return await cassia_technologies_service.get_technology(cassia_technology_id)

""" 
@cassia_technologies_router.post(
    '/',
    tags=["Cassia - Technologies"],
    status_code=status.HTTP_200_OK,
    summary="Create cassia technology register",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_technology(tech_data: cassia_technologies_schema.CassiaTechnologySchema):
    return await cassia_technologies_service.create_technology(tech_data)


@cassia_technologies_router.put(
    '/{cassia_technology_id}',
    tags=["Cassia - Technologies"],
    status_code=status.HTTP_200_OK,
    summary="Update cassia technology register",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_technology(cassia_technology_id, tech_data: cassia_technologies_schema.CassiaTechnologySchema):
    return await cassia_technologies_service.update_technology(cassia_technology_id, tech_data)


@cassia_technologies_router.delete(
    '/{cassia_technology_id}',
    tags=["Cassia - Technologies"],
    status_code=status.HTTP_200_OK,
    summary="Delete cassia technology register by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_technology(cassia_technology_id):
    return await cassia_technologies_service.delete_technology(cassia_technology_id)
 """


@cassia_technologies_router.get(
    '/devices/{cassia_technology_id}',
    tags=["Cassia - Technologies Portfolio"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia technologies devices",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_technology_devices(cassia_technology_id):
    return await cassia_technologies_service.get_technology_devices(cassia_technology_id)
""" @cassia_technologies_router.get(
    '/devices/{cassia_technology_id}',
    tags=["Cassia - Technologies"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia technologies devices",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_technology_devices(cassia_technology_id):
    return await cassia_technologies_service.get_technology_devices(cassia_technology_id)
 """
