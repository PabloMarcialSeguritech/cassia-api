from fastapi import APIRouter, Depends
from .maintenance_router import maintenance_router
from .users_routers import users_router
from .roles_router import roles_router
from .configurations_router import configuration_router
from .host_correlation_router import host_correlation_router
from .cis_router import cis_router
from .reports_router import reports_router
from .actions_router import actions_router
from .slack_notifications_router import notifications_router
from .diagnosta_router import diagnosta_router
from .exceptions_router import exceptions_router
from .reports_notifications_router import reports_notifications_router
from .auto_actions_router import auto_actions_router
from .cassia_technologies_router import cassia_technologies_router
from .cassia_events_config_router import cassia_events_config_router
from .cassia_criticalities_router import cassia_criticalities_router
from .cassia_services_techs_router import cassia_services_router
from .cassia_techs_router import cassia_techs_router
from .cassia_tech_devices_router import cassia_techs_devices_router
from .cassia_user_notification_types_router import cassia_user_notification_type_router
from .cassia_gs_tickets_router import cassia_gs_tickets_router
from .resets_router import resets_router
from .cassia_group_type_router import cassia_group_type_router
from .cassia_host_groups_router import cassia_host_groups_router
from .cassia_exports_routers import cassia_exports_router
from .cassia_technologies_devices_router import cassia_tech_devices_router
from .cassia_brand_router import cassia_brand_router
from .cassia_host_models_router import cassia_host_models_router
from .cassia_hosts_router import cassia_hosts_router
from .cassia_audit_router import cassia_audit_router
from .cassia_hosts_discovery_router import cassia_hosts_discovery_router
from .cassia_media_telegram_router import media_telegram_router
from .cassia_user_groups_router import cassia_user_groups_router
from infraestructure.database import DB
from dependencies import get_db
cassia_router = APIRouter(prefix="/api/v1/cassia")


@cassia_router.get("/db")
def greet_child(db: DB = Depends(get_db)):
    print(db)
    print(id(db))
    print(db.pool)
    return


""" def get_db(db: DB = Depends()):
    return db """


cassia_router.include_router(users_router)
cassia_router.include_router(roles_router)
cassia_router.include_router(configuration_router)
cassia_router.include_router(host_correlation_router)
cassia_router.include_router(cis_router)
cassia_router.include_router(reports_router)
cassia_router.include_router(actions_router)
cassia_router.include_router(notifications_router)
cassia_router.include_router(diagnosta_router)
cassia_router.include_router(exceptions_router)
cassia_router.include_router(reports_notifications_router)
cassia_router.include_router(auto_actions_router)
cassia_router.include_router(cassia_technologies_router)
cassia_router.include_router(cassia_events_config_router)
cassia_router.include_router(cassia_criticalities_router)
cassia_router.include_router(cassia_services_router)
cassia_router.include_router(cassia_techs_router)
cassia_router.include_router(cassia_techs_devices_router)
cassia_router.include_router(maintenance_router)
cassia_router.include_router(cassia_user_notification_type_router)
cassia_router.include_router(cassia_gs_tickets_router)
cassia_router.include_router(resets_router)
cassia_router.include_router(cassia_group_type_router)
cassia_router.include_router(cassia_host_groups_router)
cassia_router.include_router(cassia_exports_router)
cassia_router.include_router(cassia_tech_devices_router)
cassia_router.include_router(cassia_brand_router)
cassia_router.include_router(cassia_host_models_router)
cassia_router.include_router(cassia_hosts_router)
cassia_router.include_router(cassia_audit_router)
cassia_router.include_router(cassia_hosts_discovery_router)
cassia_router.include_router(media_telegram_router)
cassia_router.include_router(cassia_user_groups_router)
