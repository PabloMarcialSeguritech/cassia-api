from fastapi import FastAPI
from routers.user_router import auth_router
from routers.zabbix import zabbix_router
from middleware.error_handler import ErrorHandler
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

""" app.add_middleware(ErrorHandler) """
app.include_router(auth_router)
app.include_router(zabbix_router)
