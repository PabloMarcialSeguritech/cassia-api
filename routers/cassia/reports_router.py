from datetime import datetime
from fastapi import APIRouter
from fastapi import Depends, status, Query
from services import auth_service2
from typing import List
import services.cassia.reports_service as reports_service
from fastapi.responses import FileResponse
reports_router = APIRouter(prefix="/reports")


@reports_router.get(
    '/availability',
    tags=["Cassia - Reports"],
    status_code=status.HTTP_200_OK,
    summary="Get availability report data",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_by_ip(municipality_id: str = '0', tech_id:  str = '0', brand_id: str = '', model_id:  str = '', init_date: datetime = "2023-09-15 12:15:00", end_date: datetime = "2023-09-15 22:16:00"):

    return await reports_service.get_graphic_data(municipality_id, tech_id, brand_id, model_id, init_date, end_date)
""" dependencies=[Depends(auth_service2.get_current_user_session)] """


@reports_router.get(
    '/availability/multiple',
    tags=["Cassia - Reports"],
    status_code=status.HTTP_200_OK,
    summary="Get availability report data multiple",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_by_ip(municipality_id: List[str] = Query('0'), tech_id:  List[str] = Query('0'), brand_id: List[str] = Query(''), model_id:  List[str] = Query(''), init_date: datetime = "2023-09-15 12:15:00", end_date: datetime = "2023-09-15 22:16:00"):
    print(municipality_id)

    return await reports_service.get_graphic_data_multiple(municipality_id, tech_id, brand_id, model_id, init_date, end_date)
""" dependencies=[Depends(auth_service2.get_current_user_session)] """


@reports_router.get(
    '/availability/multiple/download',
    tags=["Cassia - Reports"],
    status_code=status.HTTP_200_OK,
    summary="Get availability report data multiple",
    response_class=FileResponse,
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_by_ip(municipality_id: List[str] = Query('0'), tech_id:  List[str] = Query('0'), brand_id: List[str] = Query(''), model_id:  List[str] = Query(''), init_date: datetime = "2023-09-15 12:15:00", end_date: datetime = "2023-09-15 22:16:00"):
    return await reports_service.download_graphic_data_multiple(municipality_id, tech_id, brand_id, model_id, init_date, end_date)
