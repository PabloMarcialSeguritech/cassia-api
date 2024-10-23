from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np
from schemas.cassia_audit_schema import CassiaAuditSchema
from models.cassia_audit import CassiaAuditLog
from  utils.traits import get_datetime_now_with_tz


async def get_audit_logs(start_date, end_date, user_email,  module_id, db: DB):
    audit_logs_df = None
    try:
        query_statement_get_audit_logs, params = DBQueries(
        ).builder_query_statement_get_audit_logs(start_date, end_date, user_email,  module_id)
        audit_logs_data = await db.run_query(query_statement_get_audit_logs, params)
        audit_logs_df = pd.DataFrame(
            audit_logs_data).replace(np.nan, None)
        return audit_logs_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener la auditoria: {e}")


async def fetch_audit_by_ids(audit_ids, db):
    audit_df = None
    try:
        query_statement_get_audits_by_ids = DBQueries(
        ).builder_query_statement_get_audits_by_ids(audit_ids)
        audits_data = await db.run_query(query_statement_get_audits_by_ids)
        audit_df = pd.DataFrame(
            audits_data).replace(np.nan, None)
        return audit_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en fetch_audit_by_ids: {e}")


async def create_audit_log(model_data: CassiaAuditSchema, db: DB):
    try:
        session = await db.get_session()
        model = CassiaAuditLog(
            user_name=model_data.user_name,
            user_email=model_data.user_email,
            summary=model_data.summary,
            timestamp= get_datetime_now_with_tz(),
            id_audit_action= model_data.id_audit_action,
            id_audit_module= model_data.id_audit_module
        )
        session.add(model)
        await session.commit()
        await session.refresh(model)
        return model
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Error en create_audit_log: {e}")


async def get_audit_modules(db):
    modules_df = None
    try:
        query_statement_get_modules = DBQueries(
        ).query_statement_get_audit_modules
        modules_data = await db.run_query(query_statement_get_modules)
        modules_df = pd.DataFrame(
            modules_data).replace(np.nan, None)
        return modules_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener los modulos: {e}")