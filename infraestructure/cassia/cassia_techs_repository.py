from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_technologies import CassiaTechnologiesModel
from models.cassia_tech_services import CassiaTechServiceModel
from models.cassia_techs import CassiaTechModel
from schemas import cassia_technologies_schema
from schemas import cassia_service_tech_schema
from schemas import cassia_techs_schema
import pandas as pd
import numpy as np
import utils.traits as traits
from datetime import datetime


async def get_tech_by_id(tech_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_tech_by_id = DBQueries(
        ).builder_query_get_cassia_tech_by_id(tech_id)
        await db_model.start_connection()
        cassia_tech_data = await db_model.run_query(query_get_cassia_tech_by_id)
        cassia_tech_df = pd.DataFrame(
            cassia_tech_data).replace(np.nan, None)
        return cassia_tech_df

    except Exception as e:
        print(f"Excepcion en get_tech_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_tech_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_techs_by_service_id(service_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_techs_by_service_id = DBQueries(
        ).builder_query_get_techs_by_service_id(service_id)
        await db_model.start_connection()
        cassia_techs_data = await db_model.run_query(query_get_techs_by_service_id)
        cassia_techs_df = pd.DataFrame(
            cassia_techs_data).replace(np.nan, None)
        return cassia_techs_df

    except Exception as e:
        print(f"Excepcion en get_techs_by_service_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_techs_by_service_id: {e}")
    finally:
        await db_model.close_connection()


async def create_tech(tech_data: cassia_techs_schema.CassiaTechSchema):
    db_model = DB()
    try:
        session = await db_model.get_session()
        now = traits.get_datetime_now_with_tz()
        cassia_tech = CassiaTechModel(
            tech_name=tech_data.tech_name,
            tech_description=tech_data.tech_description,
            service_id=tech_data.service_id,
            cassia_criticality_id=tech_data.cassia_criticality_id,
            created_at=now,
            updated_at=now,
        )

        session.add(cassia_tech)
        await session.commit()
        await session.refresh(cassia_tech)
        return cassia_tech

    except Exception as e:
        print(f"Excepcion en create_tech: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_tech: {e}")
    finally:
        await session.close()


async def update_tech(cassia_tech_id, tech_data: cassia_techs_schema.CassiaTechSchema):
    db_model = DB()
    try:
        query_update_cassia_tech_by_id = DBQueries(
        ).builder_query_update_cassia_tech_by_id(cassia_tech_id, tech_data)

        await db_model.start_connection()
        updated_tech = await db_model.run_query(query_update_cassia_tech_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en update_tech: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_tech: {e}")
    finally:
        await db_model.close_connection()


async def delete_tech_by_id(cassia_tech_id) -> bool:
    db_model = DB()
    try:
        query_delete_cassia_tech_by_id = DBQueries(
        ).builder_query_delete_cassia_tech_by_id(cassia_tech_id)
        await db_model.start_connection()
        deleted_tech = await db_model.run_query(query_delete_cassia_tech_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en delete_tech_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_tech_by_id: {e}")
    finally:
        await db_model.close_connection()
