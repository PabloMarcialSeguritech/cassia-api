from fastapi import HTTPException, status
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np


async def get_proxies(db: DB) -> pd.DataFrame:
    try:
        query_statement_get_proxies = DBQueries().query_statement_get_proxies
        proxies_data = await db.run_query(query_statement_get_proxies)
        proxies_df = pd.DataFrame(proxies_data).replace(np.nan, None)
        return proxies_df
    except Exception as e:
        print(f"Excepcion en get_proxies: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_proxies: {e}")
