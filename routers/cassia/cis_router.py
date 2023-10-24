from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
cis_router = APIRouter(prefix="/ci_elements")


@cis_router.get(
    '/search_host/{ip}',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Search host by ip",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_by_ip(ip: str):
    return await cis_service.get_host_by_ip(ip)


@cis_router.get(
    '/',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs elements",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_elements():
    return await cis_service.get_ci_elements()


@cis_router.post(
    '/',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Create a CI register",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def create_ci_element(ci_element_data: cassia_ci_element_schema.CiElementBase, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.create_ci_element(ci_element_data, current_session)


@cis_router.get(
    '/{element_id}',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element(element_id: str):
    return await cis_service.get_ci_element(element_id)


@cis_router.get(
    '/relations/{element_id}',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element relations by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_relations(element_id: str):
    return await cis_service.get_ci_element_relations(element_id)


@cis_router.post(
    '/relations/{element_id}',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Create a CIs element relation",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_relations(element_id: str, affected_ci_element_id=Form(...)):
    return await cis_service.create_ci_element_relation(element_id, affected_ci_element_id)


""" @cis_router.put(
    '/',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Update a CI register",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def create_ci_element(ci_element_data: cassia_ci_element_schema.CiElementBase, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.create_ci_element(ci_element_data, current_session)
 """

""" 
@cis_router.get(
    '/search_host/{ip}',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Search host by ip",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_by_ip(ip: str):
    return await cis_service.get_host_by_ip(ip)


@cis_router.get(
    '/',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_cis():
    return await cis_service.get_cis()


@cis_router.put(
    '/change-status/{ci_id}',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Change ci status",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def change_ci_status(ci_id: str):
    return await cis_service.change_status(ci_id)


@cis_router.delete(
    '/{ci_id}',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Delete ci by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def change_ci_status(ci_id: str):
    return await cis_service.delete_ci(ci_id)


@cis_router.post(
    '/',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Create a CI register",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_ci(ci_data: cassia_ci_schema.CiBase = Depends(cassia_ci_schema.CiBase.as_form), files: Optional[List[UploadFile]] = File(None), current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.create_ci(ci_data, files, current_session)


@cis_router.put(
    '/{ci_id}',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Update a CI register",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_ci(ci_id: str, ci_data: cassia_ci_schema.CiUpdate = Depends(cassia_ci_schema.CiUpdate.as_form), files: Optional[List[UploadFile]] = File(None), current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.update_ci(ci_id, ci_data, files, current_session)


@cis_router.get(
    '/{ci_id}',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Get CI by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_by_id(ci_id: str):
    return await cis_service.get_ci_by_id(ci_id)


@cis_router.get(
    '/download_ci_document/{ci_document_id}',
    tags=["Cassia - CI's"],
    status_code=status.HTTP_200_OK,
    summary="Get CI document by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_document_by_id(ci_document_id: str):
    return await cis_service.get_ci_document_by_id(ci_document_id)
 """
