from fastapi import APIRouter, Query
from fastapi import Depends, status
from services import auth_service
from services import auth_service2
from fastapi import Depends, status, Form, Body, File
from infraestructure.database import DB
from dependencies import get_db
import services.cassia.slack_notifications_service as slack_notifications_service

notifications_router = APIRouter(prefix="/slack_notifications")


@notifications_router.get(
    '/count',
    tags=["Cassia - Slack Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA Slack Notifications count",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_count(current_session=Depends(auth_service2.get_current_user_session), db: DB = Depends(get_db)):
    return await slack_notifications_service.get_count(current_session=current_session, db=db)


@notifications_router.get(
    '/',
    tags=["Cassia - Slack Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA Slack Notifications",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_items(skip: int = Query(0, ge=0), limit: int = Query(10, le=400), current_session=Depends(auth_service2.get_current_user_session)):
    return await slack_notifications_service.get_items(skip, limit, current_session=current_session)
