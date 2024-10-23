from fastapi import Depends, status
from services import auth_service2
from fastapi import APIRouter
from dependencies import get_db
from infraestructure.database import DB
from services.cassia import cassia_media_telegram_service
from schemas import cassia_media_telegram_schema
media_telegram_router = APIRouter(prefix='/media/telegram')


@media_telegram_router.get(
    "/groups",
    tags=["Cassia Media - Telegram"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los grupos de Telegram de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_groups(db: DB = Depends(get_db)):
    return await cassia_media_telegram_service.get_groups(db)


@media_telegram_router.post(
    "/groups/link",
    tags=["Cassia Media - Telegram"],
    status_code=status.HTTP_200_OK,
    summary="Liga un grupo de Telegram de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def link_telegram_group(group_data: cassia_media_telegram_schema.LinkTelegramGroupRequest, db: DB = Depends(get_db)):
    return await cassia_media_telegram_service.link_telegram_group(group_data, db)


@media_telegram_router.get(
    "/groups/discovery_new_groups",
    tags=["Cassia Media - Telegram"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los grupos nuevos de Telegram de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def discovery_new_groups(db: DB = Depends(get_db)):
    return await cassia_media_telegram_service.discovery_new_groups(db)


@media_telegram_router.get(
    "/config",
    tags=["Cassia Media - Telegram"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene la configuracion de Telegram de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_config(db: DB = Depends(get_db)):
    return await cassia_media_telegram_service.get_config(db)
