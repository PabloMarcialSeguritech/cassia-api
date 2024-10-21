from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_audit_logs(start_date, end_date, user_email,  module_id,  page, page_size, db: DB):
    audit_logs_df = None
    skip = (page - 1) * page_size
    try:
        query_statement_get_audit_logs, params = DBQueries(
        ).builder_query_statement_get_audit_logs(start_date, end_date, user_email,  module_id,  skip, page_size)
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
