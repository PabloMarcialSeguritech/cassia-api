from fastapi import APIRouter
from .users_routers import users_router
from .roles_router import roles_router
from .configurations_router import configuration_router
from .host_correlation_router import host_correlation_router
cassia_router = APIRouter(prefix="/api/v1/cassia")

cassia_router.include_router(users_router)
cassia_router.include_router(roles_router)
cassia_router.include_router(configuration_router)
cassia_router.include_router(host_correlation_router)
