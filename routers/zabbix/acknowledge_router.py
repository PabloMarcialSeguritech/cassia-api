from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from models.cassia_user_session import CassiaUserSession
from services.zabbix import acknowledge_service
from fastapi import Form, Query
from infraestructure.database import DB
from dependencies import get_db
acknowledge_router = APIRouter()


@acknowledge_router.post(
    '/problems/acknowledge/{eventid}',
    tags=["Zabbix - Problems(Alerts) - Acknowledge"],
    status_code=status.HTTP_200_OK,
    summary="Register a acknowledge in event, Ex: 34975081",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_problems_filter(eventid: str = "34975081", message: str = Form(max_length=2048), close: bool = Form(...),
                              current_user_session: CassiaUserSession = Depends(
                                  auth_service2.get_current_user_session), is_zabbix_event: bool = Form(default=1)):
    response = await acknowledge_service.register_ack(eventid, message, current_user_session, close, is_zabbix_event)
    return response


@acknowledge_router.get(
    '/problems/acknowledge/{eventid}',
    tags=["Zabbix - Problems(Alerts) - Acknowledge"],
    status_code=status.HTTP_200_OK,
    summary="Get acknowledges of one event, Ex: 34975081",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_problems_filter(eventid: str = "34975081", is_cassia_event: int = Query(0), db: DB = Depends(get_db)):
    response = await acknowledge_service.get_acks(eventid, is_cassia_event, db)
    return response
