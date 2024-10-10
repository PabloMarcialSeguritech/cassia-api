from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_cassia_host_groups(db: DB) -> pd.DataFrame:
    host_groups_df = None
    try:
        query_statement_get_cassia_host_groups = DBQueries(
        ).query_statement_get_cassia_host_groups

        host_groups_data = await db.run_query(query_statement_get_cassia_host_groups)
        host_groups_df = pd.DataFrame(
            host_groups_data).replace(np.nan, None)
        return host_groups_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_groups: {e}")
