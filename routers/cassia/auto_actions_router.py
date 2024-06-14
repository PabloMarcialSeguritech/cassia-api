from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
from typing import List, Optional
import services.cassia.cis_service as cis_service
from schemas import cassia_ci_mail
from services.cassia import auto_actions_service
from schemas import cassia_auto_action_condition_schema
from schemas import cassia_auto_action_schema
auto_actions_router = APIRouter(prefix="/auto_actions")


@auto_actions_router.get(
    '/conditions/',
    tags=["Cassia - Auto Actions - Conditions"],
    status_code=status.HTTP_200_OK,
    summary="Get auto actions conditions catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_conditions():
    return await auto_actions_service.get_conditions()


@auto_actions_router.get(
    '/conditions/{condition_id}',
    tags=["Cassia - Auto Actions - Conditions"],
    status_code=status.HTTP_200_OK,
    summary="Get auto action condition detail",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_condition_detail(condition_id):
    return await auto_actions_service.get_condition_detail(condition_id)


@auto_actions_router.post(
    '/conditions',
    tags=["Cassia - Auto Actions - Conditions"],
    status_code=status.HTTP_200_OK,
    summary="Create Auto Action condition",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_auto_action_condition(condition_data: cassia_auto_action_condition_schema.AutoActionConditionSchema):
    return await auto_actions_service.create_auto_action_condition(condition_data)


@auto_actions_router.put(
    '/conditions',
    tags=["Cassia - Auto Actions - Conditions"],
    status_code=status.HTTP_200_OK,
    summary="Update Auto Action condition",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_auto_action_condition(condition_data: cassia_auto_action_condition_schema.AutoActionConditionUpdateSchema):
    return await auto_actions_service.update_auto_action_condition(condition_data)


@auto_actions_router.delete(
    '/conditions/{condition_id}',
    tags=["Cassia - Auto Actions - Conditions"],
    status_code=status.HTTP_200_OK,
    summary="Delete Auto Action condition",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_auto_action_condition(condition_id):
    return await auto_actions_service.delete_auto_action_condition(condition_id)


""" 
@auto_actions_router.put(
    '/conditions',
    tags=["Cassia - Auto Actions"],
    status_code=status.HTTP_200_OK,
    summary="Update Auto Action condition",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_auto_action_condition():
    return await auto_actions_service.update_auto_action_condition
 """


@auto_actions_router.get(
    '/',
    tags=["Cassia - Auto Actions"],
    status_code=status.HTTP_200_OK,
    summary="Get auto actions catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_actions_auto():
    return await auto_actions_service.get_actions_auto()


@auto_actions_router.get(
    '/{action_auto_id}',
    tags=["Cassia - Auto Actions"],
    status_code=status.HTTP_200_OK,
    summary="Get auto action detail",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_action_auto(action_auto_id):
    return await auto_actions_service.get_action_auto(action_auto_id)


@auto_actions_router.post(
    '/',
    tags=["Cassia - Auto Actions"],
    status_code=status.HTTP_200_OK,
    summary="Create Auto Action",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_auto_action(action_data: cassia_auto_action_schema.AutoActionSchema):
    return await auto_actions_service.create_auto_action(action_data)


@auto_actions_router.put(
    '/',
    tags=["Cassia - Auto Actions"],
    status_code=status.HTTP_200_OK,
    summary="Update Auto Action",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_auto_action(action_data: cassia_auto_action_schema.AutoActionUpdateSchema):
    return await auto_actions_service.update_auto_action(action_data)


@auto_actions_router.delete(
    '/{action_auto_id}',
    tags=["Cassia - Auto Actions"],
    status_code=status.HTTP_200_OK,
    summary="Delete Auto Action condition",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_auto_action(action_auto_id):
    return await auto_actions_service.delete_auto_action(action_auto_id)
