from fastapi import APIRouter
from .alerts_router import alerts_router
from .groups_router import groups_router
from .hosts_router import hosts_router
from .layers_router import layers_router
from .host_management_router import host_management_router
from .acknowledge_router import acknowledge_router
from .proxies_router import proxies_router
zabbix_router = APIRouter(prefix="/api/v1/zabbix")


zabbix_router.include_router(alerts_router)
zabbix_router.include_router(groups_router)
zabbix_router.include_router(hosts_router)
zabbix_router.include_router(layers_router)
zabbix_router.include_router(host_management_router)
zabbix_router.include_router(acknowledge_router)
zabbix_router.include_router(proxies_router)
