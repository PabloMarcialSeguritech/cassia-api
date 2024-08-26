from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from fastapi.responses import FileResponse
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from schemas import cassia_ci_mail
from services.cassia import cassia_technologies_service
from services.cassia import cassia_services_tech_service
from schemas import cassia_technologies_schema
from schemas import cassia_auto_action_schema
from schemas import cassia_service_tech_schema
cassia_services_router = APIRouter(prefix="/services")


@cassia_services_router.get(
    '/plantilla_carga_portafolio',
    tags=["Cassia - Services"],
    status_code=status.HTTP_200_OK,
    response_class=FileResponse,
    summary="Obtiene la plantilla para carga masiva",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_plantilla():
    return await cassia_services_tech_service.get_plantilla()


@cassia_services_router.get(
    '/catalogo_dispositivos_cassia',
    tags=["Cassia - Services"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los dispositivos de cassia con su tecnologia y servicio",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_devices():
    return await cassia_services_tech_service.get_devices()


@cassia_services_router.get(
    '/',
    tags=["Cassia - Services"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia services catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_services():
    return await cassia_services_tech_service.get_services()


@cassia_services_router.get(
    '/{cassia_tech_service_id}',
    tags=["Cassia - Services"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia technologies by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_service(cassia_tech_service_id):
    return await cassia_services_tech_service.get_service(cassia_tech_service_id)


@cassia_services_router.post(
    '/',
    tags=["Cassia - Services"],
    status_code=status.HTTP_200_OK,
    summary="Create cassia tech service",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_service(service_data: cassia_service_tech_schema.CassiaTechServiceSchema):
    return await cassia_services_tech_service.create_service(service_data)


@cassia_services_router.put(
    '/{cassia_tech_service_id}',
    tags=["Cassia - Services"],
    status_code=status.HTTP_200_OK,
    summary="Update cassia tech service",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_service(cassia_tech_service_id, service_data: cassia_service_tech_schema.CassiaTechServiceSchema):
    return await cassia_services_tech_service.update_service(cassia_tech_service_id, service_data)


@cassia_services_router.delete(
    '/{cassia_tech_service_id}',
    tags=["Cassia - Services"],
    status_code=status.HTTP_200_OK,
    summary="Delete cassia tech service",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_service(cassia_tech_service_id):
    return await cassia_services_tech_service.delete_service(cassia_tech_service_id)
