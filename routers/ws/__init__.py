from fastapi import APIRouter
from .ws_router import ws_router
WS_router = APIRouter(prefix="/api/v1/zabbix/hosts")

WS_router.include_router(ws_router)
