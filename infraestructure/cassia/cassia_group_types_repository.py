from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_cassia_group_types(db: DB) -> pd.DataFrame:
    group_types_df = None
    try:
        query_statement_get_cassia_group_types = DBQueries(
        ).query_statement_get_cassia_group_types

        group_types_data = await db.run_query(query_statement_get_cassia_group_types)
        group_types_df = pd.DataFrame(
            group_types_data).replace(np.nan, None)
        return group_types_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_group_types: {e}")


async def get_cassia_group_type_by_id(db: DB, type_id) -> pd.DataFrame:
    group_type_df = None
    try:
        query_statement_get_cassia_group_type_by_id = DBQueries(
        ).builder_query_statement_get_cassia_group_type_by_id(type_id)

        group_type_data = await db.run_query(query_statement_get_cassia_group_type_by_id)
        group_type_df = pd.DataFrame(
            group_type_data).replace(np.nan, None)
        return group_type_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_group_type_by_id: {e}")
