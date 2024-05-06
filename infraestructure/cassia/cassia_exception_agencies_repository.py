import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_exception_agency_async import CassiaExceptionAgencyAsync
from fastapi import status, HTTPException


async def get_cassia_exception_agencies() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_exception_agencies = DBQueries(
        ).query_get_cassia_exception_agencies
        await db_model.start_connection()
        exception_agencies_data = await db_model.run_query(query_get_exception_agencies)
        exception_agencies_df = pd.DataFrame(
            exception_agencies_data).replace(np.nan, None)
        return exception_agencies_df
    except Exception as e:
        print(f"Excepcion generada en get_cassia_exception_agencies: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_cassia_exception_agencies {e}"
        )
    finally:
        await db_model.close_connection()


async def create_cassia_exception_agency(exception_agency_data):
    db_model = DB()
    try:
        session = await db_model.get_session()
        exception_agency = CassiaExceptionAgencyAsync(
            name=exception_agency_data.name,
            created_at=datetime.now(pytz.timezone('America/Mexico_City')),
            updated_at=datetime.now(pytz.timezone('America/Mexico_City'))
        )
        session.add(exception_agency)
        await session.commit()
        await session.refresh(exception_agency)
        return exception_agency
    except Exception as e:
        print(f"Excepcion generada en create_cassia_exception_agency: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en create_cassia_exception_agency {e}"
        )
    finally:
        await session.close()


async def get_cassia_exception_agency_by_id(exception_agency_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_exception_agency = DBQueries(
        ).builder_query_statement_get_cassia_exception_agency_by_id(exception_agency_id)
        await db_model.start_connection()
        exception_agency_data = await db_model.run_query(query_get_exception_agency)
        exception_agency_df = pd.DataFrame(
            exception_agency_data).replace(np.nan, None)
        return exception_agency_df
    except Exception as e:
        print(f"Excepcion generada en get_cassia_exception_agency_by_id: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_cassia_exception_agency_by_id {e}"
        )
    finally:
        await db_model.close_connection()


async def update_cassia_exception_agency(exception_agency_id, exception_agency_data) -> pd.DataFrame:
    db_model = DB()
    try:
        current_time = datetime.now(pytz.timezone('America/Mexico_City'))
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        query_update_exception_agency = DBQueries(
        ).builder_query_statement_update_cassia_exception_agency(exception_agency_id, exception_agency_data.name, formatted_time)
        print(query_update_exception_agency)
        await db_model.start_connection()
        result = await db_model.run_query(query_update_exception_agency)
        print(result)
        return True
    except Exception as e:
        print(f"Excepcion generada en update_cassia_exception_agency: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en update_cassia_exception_agency {e}"
        )
    finally:
        await db_model.close_connection()


async def delete_cassia_exception_agency(exception_agency_id) -> pd.DataFrame:
    db_model = DB()
    try:
        current_time = datetime.now(pytz.timezone('America/Mexico_City'))
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        query_delete_exception_agency = DBQueries(
        ).builder_query_statement_logic_delete_cassia_exception_agency(exception_agency_id, formatted_time)

        await db_model.start_connection()
        result = await db_model.run_query(query_delete_exception_agency)
        print(result)
        return True
    except Exception as e:
        print(f"Excepcion generada en delete_cassia_exception_agency: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en delete_cassia_exception_agency {e}"
        )
    finally:
        await db_model.close_connection()
