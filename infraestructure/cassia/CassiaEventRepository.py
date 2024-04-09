from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_cassia_event(eventid) -> pd.DataFrame:
    db_model = DB()
    try:
        get_event_query = DBQueries().builder_query_statement_get_cassia_event(
            eventid)
        """ print("Query: ", get_event_query) """
        await db_model.start_connection()
        event_data = await db_model.run_query(get_event_query)
        event = pd.DataFrame(event_data).replace(np.nan, None)

        return event
    except Exception as e:
        print(f"Excepcion en get_cassia_event: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_cassia_event: {e}")
    finally:
        await db_model.close_connection()
