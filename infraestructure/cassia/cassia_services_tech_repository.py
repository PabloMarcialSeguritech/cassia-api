from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_technologies import CassiaTechnologiesModel
from models.cassia_tech_services import CassiaTechServiceModel
from schemas import cassia_technologies_schema
from schemas import cassia_service_tech_schema
import pandas as pd
import numpy as np
import utils.traits as traits
from datetime import datetime


async def get_service_tech_by_name(service_id, tech_name) -> pd.DataFrame:
    db_model = DB()
    try:
        tech_name_lower = tech_name.lower()
        query_get_cassia_service_techs_by_name = DBQueries(
        ).builder_query_get_cassia_service_techs_by_name(service_id, tech_name_lower)
        await db_model.start_connection()
        cassia_service_tech_data = await db_model.run_query(query_get_cassia_service_techs_by_name)
        cassia_service_tech_df = pd.DataFrame(
            cassia_service_tech_data).replace(np.nan, None)
        return cassia_service_tech_df

    except Exception as e:
        print(f"Excepcion en get_service_tech_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_service_tech_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_service_tech_by_id(tech_service_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_service_tech_by_id = DBQueries(
        ).builder_query_get_cassia_service_tech_by_id(tech_service_id)
        await db_model.start_connection()
        cassia_service_tech_data = await db_model.run_query(query_get_cassia_service_tech_by_id)
        cassia_service_tech_df = pd.DataFrame(
            cassia_service_tech_data).replace(np.nan, None)
        return cassia_service_tech_df

    except Exception as e:
        print(f"Excepcion en get_service_tech_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_service_tech_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_services() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_tech_services = DBQueries(
        ).query_get_cassia_tech_services
        await db_model.start_connection()
        cassia_services_tech_data = await db_model.run_query(query_get_cassia_tech_services)
        cassia_services_tech_df = pd.DataFrame(
            cassia_services_tech_data).replace(np.nan, None)
        return cassia_services_tech_df

    except Exception as e:
        print(f"Excepcion en get_technologies: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_technologies: {e}")
    finally:
        await db_model.close_connection()


async def create_service(service_data: cassia_service_tech_schema.CassiaTechServiceSchema):
    db_model = DB()
    try:
        session = await db_model.get_session()
        now = traits.get_datetime_now_with_tz()
        cassia_service = CassiaTechServiceModel(
            service_name=service_data.service_name,
            description=service_data.description,
            cassia_criticality_id=service_data.cassia_criticality_id,
            created_at=now,
            updated_at=now,
        )

        session.add(cassia_service)
        await session.commit()
        await session.refresh(cassia_service)
        return cassia_service

    except Exception as e:
        print(f"Excepcion en create_service: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_service: {e}")
    finally:
        await session.close()


async def update_service(cassia_tech_service_id, service_data: cassia_service_tech_schema.CassiaTechServiceSchema):
    db_model = DB()
    try:
        query_update_cassia_service_tech_by_id = DBQueries(
        ).builder_query_update_cassia_service_tech_by_id(cassia_tech_service_id, service_data)
        print(query_update_cassia_service_tech_by_id)
        await db_model.start_connection()
        updated_service = await db_model.run_query(query_update_cassia_service_tech_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en get_technology_devices: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_technology_devices: {e}")
    finally:
        await db_model.close_connection()


async def delete_service_by_id(cassia_tech_service_id) -> bool:
    db_model = DB()
    try:
        query_delete_cassia_tech_service_by_id = DBQueries(
        ).builder_query_delete_cassia_tech_service_by_id(cassia_tech_service_id)
        await db_model.start_connection()
        deleted_service = await db_model.run_query(query_delete_cassia_tech_service_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en delete_service_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_service_by_id: {e}")
    finally:
        await db_model.close_connection()
