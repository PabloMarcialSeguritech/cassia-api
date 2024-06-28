from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_technologies import CassiaTechnologiesModel
from schemas import cassia_technologies_schema
import pandas as pd
import numpy as np
from datetime import datetime


async def get_technology_by_id(tech_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_technology_by_id = DBQueries(
        ).builder_query_get_cassia_technology_by_id(tech_id)
        await db_model.start_connection()
        cassia_tech_data = await db_model.run_query(query_get_cassia_technology_by_id)
        cassia_tech_df = pd.DataFrame(cassia_tech_data).replace(np.nan, None)
        return cassia_tech_df

    except Exception as e:
        print(f"Excepcion en get_technologies: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_technologies: {e}")
    finally:
        await db_model.close_connection()


""" async def get_technologies() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_technologies = DBQueries(
        ).query_get_cassia_technologies
        await db_model.start_connection()
        cassia_techs_data = await db_model.run_query(query_get_cassia_technologies)
        cassia_techs_df = pd.DataFrame(cassia_techs_data).replace(np.nan, None)
        return cassia_techs_df

    except Exception as e:
        print(f"Excepcion en get_technologies: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_technologies: {e}")
    finally:
        await db_model.close_connection() """


async def get_technologies() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_technologies = DBQueries(
        ).query_get_cassia_technologies
        await db_model.start_connection()
        cassia_techs_data = await db_model.run_query(query_get_cassia_technologies)
        cassia_techs_df = pd.DataFrame(cassia_techs_data).replace(np.nan, None)
        return cassia_techs_df

    except Exception as e:
        print(f"Excepcion en get_technologies: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_technologies: {e}")
    finally:
        await db_model.close_connection()


async def create_technology(tech_data: cassia_technologies_schema.CassiaTechnologySchema) -> bool:
    db_model = DB()
    try:
        session = await db_model.get_session()

        cassia_tech = CassiaTechnologiesModel(
            technology_name=tech_data.technology_name,
            sla=tech_data.sla,
            tech_group_ids=tech_data.tech_group_ids
        )

        session.add(cassia_tech)
        await session.commit()
        await session.refresh(cassia_tech)
        return cassia_tech

    except Exception as e:
        print(f"Excepcion en create_technology: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_technology: {e}")
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


async def delete_technology_by_id(cassia_technology_id) -> bool:
    db_model = DB()
    try:
        query_delete_cassia_technology_by_id = DBQueries(
        ).builder_query_delete_cassia_technology_by_id(cassia_technology_id)
        await db_model.start_connection()
        deleted_tech = await db_model.run_query(query_delete_cassia_technology_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en delete_technology_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_technology_by_id: {e}")
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
