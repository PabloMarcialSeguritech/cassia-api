from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_config_ import CassiaConfig
from fastapi import HTTPException, status
from sqlalchemy import select
import pandas as pd
import numpy as np


async def get_config_ping_loss_message() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_ping_loss_mesasge = DBQueries(
        ).builder_query_statement_get_config_value_by_name(name='ping_loss_message')
        await db_model.start_connection()

        ping_loss_message = await db_model.run_query(query_get_ping_loss_mesasge)
        ping_loss_message_df = pd.DataFrame(ping_loss_message)
        if ping_loss_message_df.empty:
            ping_loss_message = "Unavailable by ICMP ping"
        else:
            ping_loss_message = ping_loss_message_df['value'][0]
        print(ping_loss_message)
        return ping_loss_message

    except Exception as e:
        print(f"Excepcion en get_config_ping_loss_message: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_config_ping_loss_message: {e}")
    finally:
        await db_model.close_connection()
