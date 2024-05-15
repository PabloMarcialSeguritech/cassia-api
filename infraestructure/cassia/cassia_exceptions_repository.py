import pandas as pd
from sqlalchemy import text
import numpy as np
from datetime import datetime
import pytz
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_exceptions_async import CassiaExceptionsAsync
from fastapi import status, HTTPException


async def get_cassia_exceptions_count(municipalityId, dispId) -> pd.DataFrame:
    db_model = DB()
    try:
        sp_get_exceptions_count = DBQueries(
        ).stored_name_exceptions_count
        await db_model.start_connection()
        exceptions_count_data = await db_model.run_stored_procedure(sp_get_exceptions_count, (municipalityId, dispId))
        exceptions_count_df = pd.DataFrame(
            exceptions_count_data).replace(np.nan, None)
        return exceptions_count_df
    except Exception as e:
        print(f"Excepcion generada en get_cassia_exceptions_count: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_cassia_exceptions_count {e}"
        )
    finally:
        await db_model.close_connection()


async def get_cassia_exceptions() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_exceptions = DBQueries(
        ).query_get_cassia_exceptions
        await db_model.start_connection()
        exceptions_data = await db_model.run_query(query_get_exceptions)
        exceptions_df = pd.DataFrame(
            exceptions_data).replace(np.nan, None)
        return exceptions_df
    except Exception as e:
        print(f"Excepcion generada en get_cassia_exceptions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_cassia_exceptions {e}"
        )
    finally:
        await db_model.close_connection()


async def get_host_by_id(hostid) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_host = DBQueries(
        ).builder_query_statement_get_host_by_id(hostid)
        await db_model.start_connection()
        host_data = await db_model.run_query(query_get_host)
        host_df = pd.DataFrame(
            host_data).replace(np.nan, None)
        return host_df
    except Exception as e:
        print(f"Excepcion generada en get_host_by_id: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_host_by_id {e}"
        )
    finally:
        await db_model.close_connection()


async def create_cassia_exception(exception_data: dict):
    db_model = DB()
    try:
        session = await db_model.get_session()
        exception = CassiaExceptionsAsync(**exception_data)
        session.add(exception)
        await session.commit()
        await session.refresh(exception)
        return exception
    except Exception as e:
        print(f"Excepcion generada en create_cassia_exception: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en create_cassia_exception {e}"
        )
    finally:
        await session.close()


async def get_cassia_exception_by_id(exception_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_exception = DBQueries(
        ).builder_query_statement_get_exception_by_id(exception_id)
        await db_model.start_connection()
        exception_data = await db_model.run_query(query_get_exception)
        exception_df = pd.DataFrame(
            exception_data).replace(np.nan, None)
        return exception_df
    except Exception as e:
        print(f"Excepcion generada en get_cassia_exception_by_id: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_cassia_exception_by_id {e}"
        )
    finally:
        await db_model.close_connection()


async def close_cassia_exception_by_id(exception_id, date) -> pd.DataFrame:
    db_model = DB()
    try:
        """ current_time = datetime.now(pytz.timezone('America/Mexico_City'))
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S') """
        query_close_exception = DBQueries(
        ).builder_query_statement_close_exception_by_id(exception_id, date)
        await db_model.start_connection()
        result = await db_model.run_query(query_close_exception)
        return True
    except Exception as e:
        print(f"Excepcion generada en close_cassia_exception_by_id: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en close_cassia_exception_by_id {e}"
        )
    finally:
        await db_model.close_connection()
