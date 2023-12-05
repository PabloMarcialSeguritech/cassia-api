from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from schemas import cassia_ci_mail
from services.cassia import actions_service
actions_router = APIRouter(prefix="/actions")


@actions_router.get(
    '/',
    tags=["Cassia - Actions"],
    status_code=status.HTTP_200_OK,
    summary="Get actions catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_actions():
    return await actions_service.get_actions()


@actions_router.get(
    '/search_host/{ip}',
    tags=["Cassia - Actions"],
    status_code=status.HTTP_200_OK,
    summary="Search host by ip",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_by_ip(ip: str):
    return await actions_service.get_host_by_ip(ip)


@actions_router.get(
    '/relations/{element_id}',
    tags=["Cassia - Actions"],
    status_code=status.HTTP_200_OK,
    summary="Get CIs element relations by id",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_actions_relations(action_id: str):
    return await actions_service.get_ci_element_relations(action_id)


@actions_router.post(
    '/relations/{action_id}',
    tags=["Cassia - Actions"],
    status_code=status.HTTP_200_OK,
    summary="Create a CIs element relation",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_interface_action_relation(action_id: str, affected_interface_id=Form(...)):
    return await actions_service.create_interface_action_relation(action_id, affected_interface_id)


@actions_router.delete(
    '/relations/{int_act_id}',
    tags=["Cassia - Actions"],
    status_code=status.HTTP_200_OK,
    summary="Delete interface action relation",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_interface_action_relation(int_act_id: str):
    return await actions_service.delete_interface_action_relation(int_act_id)

""" @actions_router.delete(
    '/relations/{cassia_ci_relation_id}',
    tags=["Cassia - CI's Elements - Relations"],
    status_code=status.HTTP_200_OK,
    summary="Create a CIs element relation",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ci_element_relations(cassia_ci_relation_id: str):
    return await cis_service.delete_ci_element_relation(cassia_ci_relation_id)
 """
