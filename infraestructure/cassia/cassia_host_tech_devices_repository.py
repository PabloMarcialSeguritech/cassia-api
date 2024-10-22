from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_host_tech_device_by_id(device_id, db):
    host_devices_df = None
    try:
        query_statement_get_cassia_host_device_by_id = DBQueries(
        ).query_statement_get_cassia_host_device_by_id

        host_devices_data = await db.run_query(query_statement_get_cassia_host_device_by_id, (f"{device_id}"))
        host_devices_df = pd.DataFrame(
            host_devices_data).replace(np.nan, None)
        return host_devices_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_tech_device_by_id: {e}")


async def get_host_devices(db):
    host_devices_df = None
    try:
        query_statement_get_cassia_host_groups = DBQueries(
        ).query_statement_get_cassia_host_devices

        host_devices_data = await db.run_query(query_statement_get_cassia_host_groups)
        host_devices_df = pd.DataFrame(
            host_devices_data).replace(np.nan, None)
        return host_devices_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_devices: {e}")


async def update_host_device_tech(tech_disp_id, tech_visible_name, db):
    try:
        query = DBQueries().builder_update_host_tech_device(
            tech_disp_id, tech_visible_name)
        await db.run_query(query)
        return True
    except Exception as e:
        print(f"Exception in update_technology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en update_technology {e}"
        )


async def get_technology_by_id(tech_disp_id, db):
    try:
        query = DBQueries().builder_get_host_device_by_id(tech_disp_id)
        tech_data = await db.run_query(query)
        tech_data_df = pd.DataFrame(tech_data).replace(np.nan, None)
        return tech_data_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_group_by_id: {e}")


async def get_cassia_tech_devices_by_ids(dispIds, db: DB) -> pd.DataFrame:
    technologies_df = None
    try:
        query_statement_get_technologies_by_ids = DBQueries(
        ).builder_query_statement_get_technologies_devices_by_ids(dispIds)
        technologies_data = await db.run_query(query_statement_get_technologies_by_ids)
        technologies_df = pd.DataFrame(
            technologies_data).replace(np.nan, None)
        return technologies_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_tech_devices_by_ids: {e}")
