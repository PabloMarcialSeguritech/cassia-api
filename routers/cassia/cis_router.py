from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from schemas import cassia_ci_mail
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


@cis_router.put(
    '/{element_id}',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Update a CI register",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def update_ci_element(element_id: str, ci_element_data: cassia_ci_element_schema.CiElementBase, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.update_ci_element(element_id, ci_element_data, current_session)


@cis_router.delete(
    '/{element_id}',
    tags=["Cassia - CI's Elements"],
    status_code=status.HTTP_200_OK,
    summary="Delete a CI register",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def delete_ci_element(element_id: str,  current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.delete_ci_element(element_id, current_session)


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
    tags=["Cassia - CI's Elements - Relations"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element relations by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_relations(element_id: str):
    return await cis_service.get_ci_element_relations(element_id)


@cis_router.get(
    '/relations/catalog/elements',
    tags=["Cassia - CI's Elements - Relations"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_catalog():
    return await cis_service.get_ci_element_catalog()


@cis_router.post(
    '/relations/{element_id}',
    tags=["Cassia - CI's Elements - Relations"],
    status_code=status.HTTP_200_OK,
    summary="Create a CIs element relation",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_relations(element_id: str, affected_ci_element_id=Form(...)):
    return await cis_service.create_ci_element_relation(element_id, affected_ci_element_id)


@cis_router.delete(
    '/relations/{cassia_ci_relation_id}',
    tags=["Cassia - CI's Elements - Relations"],
    status_code=status.HTTP_200_OK,
    summary="Create a CIs element relation",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_relations(cassia_ci_relation_id: str):
    return await cis_service.delete_ci_element_relation(cassia_ci_relation_id)


@cis_router.get(
    '/docs/{element_id}',
    tags=["Cassia - CI's Elements - Docs"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element docs by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_docs(element_id: str):
    return await cis_service.get_ci_element_docs(element_id)


@cis_router.post(
    '/docs/{element_id}',
    tags=["Cassia - CI's Elements - Docs"],
    status_code=status.HTTP_200_OK,
    summary="Upload files to a CIs element ",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_relations(element_id: str, files: List[UploadFile]):
    return await cis_service.upload_ci_element_docs(element_id, files)


@cis_router.get(
    '/docs/download/{doc_id}',
    tags=["Cassia - CI's Elements - Docs"],
    status_code=status.HTTP_200_OK,
    summary="Download CIs element doc by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def download_ci_element_doc(doc_id: str):
    return await cis_service.download_ci_element_doc(doc_id)


@cis_router.delete(
    '/docs/{doc_id}',
    tags=["Cassia - CI's Elements - Docs"],
    status_code=status.HTTP_200_OK,
    summary="Delete CIs element doc by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_ci_element_doc(doc_id: str):
    return await cis_service.delete_ci_element_doc(doc_id)


@cis_router.get(
    '/history/{element_id}',
    tags=["Cassia - CI's Elements - History"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element history by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_history(element_id: str):
    return await cis_service.get_ci_element_history(element_id)


@cis_router.get(
    '/history/detail/{history_id}',
    tags=["Cassia - CI's Elements - History"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element history row by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_history_record(history_id: str):
    return await cis_service.get_ci_element_history_detail(history_id)


@cis_router.post(
    '/history',
    tags=["Cassia - CI's Elements - History"],
    status_code=status.HTTP_200_OK,
    summary="Create a CI history register",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def create_ci_history_record(ci_element_history_data: cassia_ci_history_schema.CiHistoryBase, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.create_ci_history_record(ci_element_history_data, current_session)


@cis_router.put(
    '/history/{ci_element_history_id}',
    tags=["Cassia - CI's Elements - History"],
    status_code=status.HTTP_200_OK,
    summary="Update a CI history register",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def update_ci_history_record(ci_element_history_id, ci_element_history_data: cassia_ci_history_schema.CiHistoryBase, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.update_ci_history_record(ci_element_history_id, ci_element_history_data, current_session)


@cis_router.delete(
    '/history/{ci_element_history_id}',
    tags=["Cassia - CI's Elements - History"],
    status_code=status.HTTP_200_OK,
    summary="Delete a CI history register",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def delete_ci_history_record(ci_element_history_id, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.delete_ci_history_record(ci_element_history_id,  current_session)


@cis_router.get(
    '/history/process/catalog',
    tags=["Cassia - CI's Elements - Authorizations"],
    status_code=status.HTTP_200_OK,
    summary="Get catalog of process",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_process():
    return await cis_service.get_ci_process()


@cis_router.post(
    '/history/authorization/create/{ci_element_history_id}',
    tags=["Cassia - CI's Elements - Authorizations"],
    status_code=status.HTTP_200_OK,
    summary="Get catalog of process",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_authorization_request(ci_element_history_id, ci_authorization_data: cassia_ci_mail.CiMailBase, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.create_authorization_request(ci_element_history_id, ci_authorization_data, current_session)


@cis_router.post(
    '/history/authorization/cancel/{ci_element_history_id}',
    tags=["Cassia - CI's Elements - Authorizations"],
    status_code=status.HTTP_200_OK,
    summary="Get catalog of process",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def cancel_authorization_request(ci_element_history_id, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.cancel_authorization_request(ci_element_history_id, current_session)


@cis_router.get(
    '/history/requests/get',
    tags=["Cassia - CI's Elements - Authorizations"],
    status_code=status.HTTP_200_OK,
    summary="Get pending requests",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_authorization_requests(current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.get_authorization_requests(current_session)


@cis_router.post(
    '/history/authorization/authorize/{cassia_mail_id}',
    tags=["Cassia - CI's Elements - Authorizations"],
    status_code=status.HTTP_200_OK,
    summary="Authorize o decline a request",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def authorize_request(cassia_mail_id, ci_authorization_data: cassia_ci_mail.CiMailAuthorize, current_session=Depends(auth_service2.get_current_user_session)):
    return await cis_service.authorize_request(cassia_mail_id, ci_authorization_data, current_session)
