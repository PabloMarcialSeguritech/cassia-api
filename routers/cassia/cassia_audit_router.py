from datetime import date, timedelta
from fastapi import APIRouter, Depends, status, Query
from services.cassia import cassia_audit_service
from infraestructure.database import DB
from dependencies import get_db
from services import auth_service2
from typing import Optional
from schemas import cassia_audit_schema
from datetime import datetime

cassia_audit_router = APIRouter(prefix="/audit")


@cassia_audit_router.get(
    "/",
    tags=["Cassia - Audit"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los audit logs",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_audit_logs(
    start_date: datetime = Query(..., description="Fecha y hora de inicio en formato ISO (YYYY-MM-DDTHH:MM:SS)", example="2024-01-01T00:00:00"),
    end_date: datetime = Query(..., description="Fecha y hora de fin en formato ISO (YYYY-MM-DDTHH:MM:SS)", example="2024-12-31T23:59:59"),
    user_email: Optional[str] = Query(None, description="Email del usuario"),
    module_id: Optional[int] = Query(None, description="ID del módulo"),
    db: DB = Depends(get_db)
):

    return await cassia_audit_service.get_audit_logs(start_date, end_date, user_email, module_id, db)


@cassia_audit_router.post(
    "/export",
    tags=["Cassia - Audit"],
    status_code=status.HTTP_200_OK,
    summary="Exporta los registros de auditoria de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_audit_data(export_data: cassia_audit_schema.CassiaAuditExportSchema, db: DB = Depends(get_db)):
    return await cassia_audit_service.export_audit_data(export_data, db)

@cassia_audit_router.post(
    "/",
    tags=["Cassia - Audit"],
    status_code=status.HTTP_200_OK,
    summary="Crea un registro de auditoria",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_audit_log(model_data: cassia_audit_schema.CassiaAuditSchema, db : DB = Depends(get_db)):
    return await cassia_audit_service.create_audit_log(model_data, db)
