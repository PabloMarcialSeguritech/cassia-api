from fastapi import APIRouter
from services.zabbix import proxies_service
from fastapi import Depends, status
from services import auth_service2
from dependencies import get_db
from infraestructure.database import DB
proxies_router = APIRouter(prefix="/proxies")


@proxies_router.get(
    '/',
    tags=["Zabbix - Proxies"],
    status_code=status.HTTP_200_OK,
    summary="Get proxies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_proxies(db: DB = Depends(get_db)):
    return await proxies_service.get_proxies(db)
