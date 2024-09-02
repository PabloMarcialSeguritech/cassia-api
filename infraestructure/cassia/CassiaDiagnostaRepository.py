from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_local_events_diagnosta_by_host(hostid) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        query_get_local_events_diagnosta_by_host = DBQueries(
        ).builder_query_statement_get_local_events_diagnosta_test(hostid)
        await db_model.start_connection()

        diagnosta_events_data = await db_model.run_query(query_get_local_events_diagnosta_by_host)
        diagnosta_events_df = pd.DataFrame(
            diagnosta_events_data).replace(np.nan, None)
        return diagnosta_events_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_local_events_diagnosta_by_host {e}")
    finally:
        await db_model.close_connection()


async def get_host_dependientes_pool(db) -> pd.DataFrame:
    try:
        # PINK
        sp_name_get_dependents_diagnostic_problems = DBQueries(
        ).stored_name_get_dependents_diagnostic_problems_test

        diagnosta_dependents_data = await db.run_stored_procedure(sp_name_get_dependents_diagnostic_problems, (0, ''))
        diagnosta_dependents_df = pd.DataFrame(
            diagnosta_dependents_data).replace(np.nan, None)
        return diagnosta_dependents_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_dependientes {e}")


async def get_host_dependientes() -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        sp_name_get_dependents_diagnostic_problems = DBQueries(
        ).stored_name_get_dependents_diagnostic_problems_test
        await db_model.start_connection()

        diagnosta_dependents_data = await db_model.run_stored_procedure(sp_name_get_dependents_diagnostic_problems, (0, ''))
        diagnosta_dependents_df = pd.DataFrame(
            diagnosta_dependents_data).replace(np.nan, None)
        return diagnosta_dependents_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_dependientes {e}")
    finally:
        await db_model.close_connection()


async def get_open_problems_diagnosta_pool(db) -> pd.DataFrame:
    try:
        # PINK
        query_get_open_problems = DBQueries(
        ).query_get_open_diagnosta_problems_test

        diagnosta_open_problems_data = await db.run_query(query_get_open_problems)
        diagnosta_open_problems_df = pd.DataFrame(
            diagnosta_open_problems_data).replace(np.nan, None)
        return diagnosta_open_problems_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_open_problems_diagnosta {e}")


async def get_open_problems_diagnosta() -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        query_get_open_problems = DBQueries(
        ).query_get_open_diagnosta_problems_test
        await db_model.start_connection()

        diagnosta_open_problems_data = await db_model.run_query(query_get_open_problems)
        diagnosta_open_problems_df = pd.DataFrame(
            diagnosta_open_problems_data).replace(np.nan, None)
        return diagnosta_open_problems_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_open_problems_diagnosta {e}")
    finally:
        await db_model.close_connection()


async def get_downs_origen_pool(municipalityId, tech_host_type, db) -> pd.DataFrame:

    try:
        # PINK
        sp_get_downs_origen = DBQueries(
        ).stored_name_diagnostic_problems_origen_1_test

        diagnosta_downs_origen_data = await db.run_stored_procedure(sp_get_downs_origen, (municipalityId, tech_host_type))
        diagnosta_downs_origen_df = pd.DataFrame(
            diagnosta_downs_origen_data).replace(np.nan, None)
        return diagnosta_downs_origen_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_downs_origen {e}")


async def get_downs_origen(municipalityId, tech_host_type) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        sp_get_downs_origen = DBQueries(
        ).stored_name_diagnostic_problems_origen_1_test
        await db_model.start_connection()

        diagnosta_downs_origen_data = await db_model.run_stored_procedure(sp_get_downs_origen, (municipalityId, tech_host_type))
        diagnosta_downs_origen_df = pd.DataFrame(
            diagnosta_downs_origen_data).replace(np.nan, None)
        return diagnosta_downs_origen_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_downs_origen {e}")
    finally:
        await db_model.close_connection()
