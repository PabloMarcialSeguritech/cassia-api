from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from services.cassia import cassia_techs_service
from services.cassia import cassia_techs_devices_service
from schemas import cassia_technologies_schema
from schemas import cassia_auto_action_schema
from schemas import cassia_service_tech_schema
from schemas import cassia_techs_schema
cassia_techs_devices_router = APIRouter(prefix="/services/techs/devices")


@cassia_techs_devices_router.get(
    '/{tech_id}',
    tags=["Cassia - Services - Techs - Devices"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia tech catalog by service id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices_by_tech(tech_id):
    return await cassia_techs_devices_service.get_devices_by_tech(tech_id)

""" 
@cassia_techs_devices_router.post(
    '/',
    tags=["Cassia - Services - Techs"],
    status_code=status.HTTP_200_OK,
    summary="Create cassia tech device",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_tech_device(tech_data: cassia_techs_schema.CassiaTechSchema):
    return await cassia_techs_devices_service.create_tech_device(tech_data) """


""" @cassia_techs_devices_router.get(
    '/detail/{tech_id}',
    tags=["Cassia - Services - Techs"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia technologies by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_tech_by_id(tech_id):
    return await cassia_techs_devices_service.get_tech_by_id(tech_id)


@cassia_techs_devices_router.post(
    '/',
    tags=["Cassia - Services - Techs"],
    status_code=status.HTTP_200_OK,
    summary="Create cassia tech ",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_tech(tech_data: cassia_techs_schema.CassiaTechSchema):
    return await cassia_techs_devices_service.create_tech(tech_data)


@cassia_techs_devices_router.put(
    '/{cassia_tech_id}',
    tags=["Cassia - Services - Techs"],
    status_code=status.HTTP_200_OK,
    summary="Update cassia tech ",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_tech(cassia_tech_id, tech_data: cassia_techs_schema.CassiaTechSchema):
    return await cassia_techs_devices_service.update_tech(cassia_tech_id, tech_data)


@cassia_techs_devices_router.delete(
    '/{cassia_tech_id}',
    tags=["Cassia - Services - Techs"],
    status_code=status.HTTP_200_OK,
    summary="Delete cassia tech",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_tech(cassia_tech_id):
    return await cassia_techs_devices_service.delete_tech(cassia_tech_id)
 """
