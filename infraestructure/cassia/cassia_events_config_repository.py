from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_technologies import CassiaTechnologiesModel
from schemas import cassia_technologies_schema
import pandas as pd
import numpy as np
from datetime import datetime


async def get_events_config() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_events_config = DBQueries(
        ).query_get_events_config
        await db_model.start_connection()
        cassia_events_config_data = await db_model.run_query(query_get_events_config)
        cassia_events_config_df = pd.DataFrame(
            cassia_events_config_data).replace(np.nan, None)
        return cassia_events_config_df

    except Exception as e:
        print(f"Excepcion en get_events_config: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_events_config: {e}")
    finally:
        await db_model.close_connection()
