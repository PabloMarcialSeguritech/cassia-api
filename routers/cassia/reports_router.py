from datetime import datetime
from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
import services.cassia.reports_service as reports_service
reports_router = APIRouter(prefix="/ci")


@reports_router.get(
    '/search_host/',
    tags=["Cassia - Reports"],
    status_code=status.HTTP_200_OK,
    summary="Get availability report data",

)
async def get_host_by_ip(municipality_id: str = "0", tech_id: str = "0", brand_id: str = "", model_id: str = "", init_date: datetime = "2023-09-15 12:15:00", end_date: datetime = "2023-09-15 22:16:00"):
    return await reports_service.get_graphic_data(municipality_id, tech_id, brand_id, model_id, init_date, end_date)
