from fastapi import FastAPI
from routers.user_router import auth_router
from routers.zabbix_router import zabbix_router
from middleware.error_handler import ErrorHandler
app = FastAPI()
app.add_middleware(ErrorHandler)
app.include_router(auth_router)
app.include_router(zabbix_router)
