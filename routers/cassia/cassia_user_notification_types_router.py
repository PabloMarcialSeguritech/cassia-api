from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from services.cassia import cassia_techs_service
from services.cassia import cassia_user_notification_types_service
from schemas import cassia_user_notification_types_schema
cassia_user_notification_type_router = APIRouter(prefix="/user/notifications")


@cassia_user_notification_type_router.get(
    '/techs',
    tags=["Cassia - User - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia tech catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_techs():
    return await cassia_user_notification_types_service.get_techs()


@cassia_user_notification_type_router.get(
    '/notification_types',
    tags=["Cassia - User - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia notification types",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_notification_types():
    return await cassia_user_notification_types_service.get_notification_types()


@cassia_user_notification_type_router.get(
    '/',
    tags=["Cassia - User - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia notification types",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_users_notification_types():
    return await cassia_user_notification_types_service.get_users_notification_types()


@cassia_user_notification_type_router.get(
    '/{user_id}',
    tags=["Cassia - User - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get cassia notification types",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_user_notification_types(user_id):
    return await cassia_user_notification_types_service.get_user_notification_types(user_id)


@cassia_user_notification_type_router.put(
    '/',
    tags=["Cassia - User - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Update cassia notification types to user, include insert, update and delete",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_user_notification_tech(notification_data: cassia_user_notification_types_schema.CassiaUserNotificationTechsSchema):
    return await cassia_user_notification_types_service.update_user_notification_tech(notification_data)
