from fastapi import APIRouter, Query
from fastapi import Depends, status
from services import auth_service2
import services.cassia.reports_notifications_service as reports_notifications_service
from schemas.cassia_user_notification_report_schema import CassiaUserReportNotificationSchema
reports_notifications_router = APIRouter(prefix="/reports/notifications")


@reports_notifications_router.get(
    '/report_names',
    tags=["Cassia - Reports - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA reports notifications names",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_report_names():
    return await reports_notifications_service.get_report_names()


@reports_notifications_router.get(
    '/user_reports',
    tags=["Cassia - Reports - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Get CASSIA user reports notifications ",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_user_reports():
    return await reports_notifications_service.get_user_reports()


@reports_notifications_router.post(
    '/user_reports',
    tags=["Cassia - Reports - Notifications"],
    status_code=status.HTTP_200_OK,
    summary="Save CASSIA user reports notifications ",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def save_user_reports(data: CassiaUserReportNotificationSchema):
    return await reports_notifications_service.save_user_reports(data)
