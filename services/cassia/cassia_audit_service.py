from datetime import datetime
from fastapi import HTTPException,status
from infraestructure.database import DB
from infraestructure.cassia import  cassia_audit_repository
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export
from schemas.cassia_audit_schema import CassiaAuditSchema


async def get_audit_logs(start_date, end_date, user_email, module_id, db):
    try:
        # Validar que la fecha de inicio no sea mayor que la fecha de fin
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="La fecha de inicio no puede ser mayor que la fecha de fin.")

        audit_logs_df = await cassia_audit_repository.get_audit_logs(start_date, end_date, user_email,  module_id, db)
        return success_response(data=audit_logs_df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener todos logs de auditoria: {e}")


async def export_audit_data(export_data, db):
    if len(export_data.audit_ids) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Selecciona al menos un registro de auditoria")

    audit_ids = ",".join([str(audit_id) for audit_id in export_data.audit_ids])
    audit_data = await cassia_audit_repository.fetch_audit_by_ids(audit_ids, db)
    try:
        now = get_datetime_now_str_with_tz()
        export_file = await generate_file_export(audit_data, page_name='auditoría', filename=f"auditoría - {now}",
                                                 file_type=export_data.file_type)
        return export_file
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en export_audit_data {e}")


async def create_audit_log(model_data: CassiaAuditSchema, db: DB):
        try:

            audit_log = await cassia_audit_repository.create_audit_log(model_data, db)
            if audit_log.id:
                return success_response(message="Registro auditoria exitoso")
            else:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Excepción no se pudo crear el registro de autoria")

        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Excepción en create_audit_log {e}")



