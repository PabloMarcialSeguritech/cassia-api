from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np
from fastapi import status, HTTPException
from schemas import cassia_media_telegram_schema


async def get_telegram_groups(db: DB) -> pd.DataFrame:
    telegram_groups_df = None
    try:
        query_statement_get_telegram_groups = DBQueries(
        ).query_statement_get_telegram_groups
        telegram_groups_data = await db.run_query(query_statement_get_telegram_groups)
        telegram_groups_df = pd.DataFrame(
            telegram_groups_data).replace(np.nan, None)
        return telegram_groups_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_telegram_groups: {e}")


async def link_telegram_group(group_data: cassia_media_telegram_schema.LinkTelegramGroupRequest, db: DB) -> pd.DataFrame:
    link_telegram_group_result = False
    try:
        query_insert_telegram_group = DBQueries(
        ).query_insert_telegram_group
        link_telegram_group_result = await db.run_query(query_insert_telegram_group, (group_data.name, group_data.groupid), True)
        if link_telegram_group_result:
            return True
        else:
            return False

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en link_telegram_group: {e}")


async def get_telegram_config(db: DB) -> pd.DataFrame:
    telegram_config_df = None
    try:
        query_statement_get_telegram_config = DBQueries(
        ).query_statement_get_telegram_config
        telegram_config_data = await db.run_query(query_statement_get_telegram_config)
        telegram_config_df = pd.DataFrame(
            telegram_config_data).replace(np.nan, None)
        return telegram_config_df

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_telegram_config: {e}")
