from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_technologies import CassiaTechnologiesModel
from models.cassia_criticalities import CassiaCriticalitiesModel
from schemas import cassia_criticality_schema
import pandas as pd
import numpy as np
from datetime import datetime
import utils.traits as traits


async def get_criticality_level(level) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_criticality_level = DBQueries(
        ).builder_query_get_cassia_criticality_level(level)
        await db_model.start_connection()
        cassia_criticalities_level_data = await db_model.run_query(query_get_cassia_criticality_level)
        cassia_criticalities_level_df = pd.DataFrame(
            cassia_criticalities_level_data).replace(np.nan, None)
        return cassia_criticalities_level_df

    except Exception as e:
        print(f"Excepcion en get_criticality_level: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_criticality_level: {e}")
    finally:
        await db_model.close_connection()


async def get_criticalities() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_criticalities = DBQueries(
        ).query_get_cassia_criticalities
        await db_model.start_connection()
        cassia_criticalities_data = await db_model.run_query(query_get_cassia_criticalities)
        cassia_criticalities_df = pd.DataFrame(
            cassia_criticalities_data).replace(np.nan, None)
        return cassia_criticalities_df

    except Exception as e:
        print(f"Excepcion en get_criticalities: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_criticalities: {e}")
    finally:
        await db_model.close_connection()


async def get_criticality_by_id(cassia_criticality_id) -> pd.DataFrame:
    db_model = DB()
    try:

        query_get_cassia_criticality_by_id = DBQueries(
        ).builder_query_get_cassia_criticality_by_id(cassia_criticality_id)
        await db_model.start_connection()
        cassia_criticality_data = await db_model.run_query(query_get_cassia_criticality_by_id)
        cassia_criticality_df = pd.DataFrame(
            cassia_criticality_data).replace(np.nan, None)
        return cassia_criticality_df

    except Exception as e:
        print(f"Excepcion en get_criticality_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_criticality_by_id: {e}")
    finally:
        await db_model.close_connection()


async def create_criticality(criticality_data, file_dest, filename):
    db_model = DB()
    try:
        session = await db_model.get_session()
        now = traits.get_datetime_now_with_tz()
        cassia_criticality = CassiaCriticalitiesModel(
            level=criticality_data.level,
            name=criticality_data.name,
            description=criticality_data.description,
            icon=file_dest,
            updated_at=now,
            filename=filename
        )

        session.add(cassia_criticality)
        await session.commit()
        await session.refresh(cassia_criticality)
        return cassia_criticality

    except Exception as e:
        print(f"Excepcion en create_criticality: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_criticality: {e}")
    finally:
        await session.close()


async def update_technology(cassia_technology_id, tech_data) -> bool:
    db_model = DB()
    try:
        query_update_cassia_technology_by_id = DBQueries(
        ).builder_query_update_cassia_technology_by_id(cassia_technology_id, tech_data)
        await db_model.start_connection()
        updated_technology = await db_model.run_query(query_update_cassia_technology_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en update_technology: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_technology: {e}")
    finally:
        await db_model.close_connection()


async def update_criticality(cassia_criticality_id, criticality_data, file_dest, filename) -> bool:
    db_model = DB()
    try:
        query_update_cassia_criticality_by_id = DBQueries(
        ).builder_query_update_cassia_criticality_by_id(cassia_criticality_id, criticality_data, file_dest, filename)
        await db_model.start_connection()
        updated_criticality = await db_model.run_query(query_update_cassia_criticality_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en update_technology: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_technology: {e}")
    finally:
        await db_model.close_connection()


async def delete_criticality_by_id(cassia_criticality_id) -> bool:
    db_model = DB()
    try:
        query_delete_cassia_technology_by_id = DBQueries(
        ).builder_query_delete_cassia_criticality_by_id(cassia_criticality_id)
        await db_model.start_connection()
        deleted_tech = await db_model.run_query(query_delete_cassia_technology_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en delete_criticality_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_criticality_by_id: {e}")
    finally:
        await db_model.close_connection()


""" async def get_technology_devices(tech_ids) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_technology_devices_by_ids = DBQueries(
        ).builder_query_get_technology_devices_by_ids(tech_ids)
        await db_model.start_connection()
        devices_data = await db_model.run_query(query_get_technology_devices_by_ids)
        devices_df = pd.DataFrame(devices_data).replace(np.nan, None)
        return devices_df

    except Exception as e:
        print(f"Excepcion en get_technology_devices: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_technology_devices: {e}")
    finally:
        await db_model.close_connection() """


async def get_technology_devices(tech_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_technology_devices_by_ids = DBQueries(
        ).builder_query_get_technology_devices_by_tech_id(tech_id)
        await db_model.start_connection()
        devices_data = await db_model.run_query(query_get_technology_devices_by_ids)
        devices_df = pd.DataFrame(devices_data).replace(np.nan, None)
        return devices_df

    except Exception as e:
        print(f"Excepcion en get_technology_devices: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_technology_devices: {e}")
    finally:
        await db_model.close_connection()
