from infraestructure.database_model import DB
from models.cassia_maintenance import CassiaMaintenance
from fastapi import status, HTTPException
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np


async def create_cassia_maintenance(maintenance_data: dict):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # PINK
        maintenance = CassiaMaintenance(**maintenance_data)
        session.add(maintenance)
        await session.commit()
        await session.refresh(maintenance)
        return maintenance
    except Exception as e:
        print(f"Excepcion generada en create_cassia_maintenance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en create_cassia_maintenance {e}"
        )
    finally:
        await session.close()


async def exist_maintenance(host_id, date_start: str, date_end: str) -> bool:
    db_model = DB()
    try:
        # Construir la consulta
        db_queries = DBQueries()
        query = db_queries.builder_query_statement_get_maintenance_between_dates_and_id(host_id,
                                                                                        date_start, date_end)

        # Iniciar conexión a la base de datos
        await db_model.start_connection()

        # Ejecutar la consulta
        maintenance_data = await db_model.run_query(query)

        # Crear DataFrame
        maintenance_df = pd.DataFrame(maintenance_data).replace(np.nan, None)

        # Verificar si el DataFrame tiene filas
        return not maintenance_df.empty

    except Exception as e:
        print(f"Excepción generada en exist_maintenance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepción generada en exist_maintenance: {e}"
        )
    finally:
        await db_model.close_connection()


async def get_maintenance_by_id(maintenance_id: int = None):
    db_model = DB()
    try:
        session = await db_model.get_session()
        exception = None
        if maintenance_id is not None:
            exception = await session.get(CassiaMaintenance, maintenance_id)
        return exception
    except Exception as e:
        print(f"Excepcion generada en get_maintenance_by_id: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_maintenance_by_id {e}"
        )
    finally:
        await db_model.close_connection()


async def delete_maintenance(maintenance_id, formatted_time, current_user_session):
    db_model = DB()
    try:
        query_delete_exception = DBQueries(
        ).builder_query_statement_logic_delete_maintenance(maintenance_id, formatted_time, current_user_session)

        await db_model.start_connection()
        await db_model.run_query(query_delete_exception)
        return True
    except Exception as e:
        print(f"Excepcion generada en delete_maintenance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en delete_cassia_exception {e}"
        )
    finally:
        await db_model.close_connection()


async def get_maintenances() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_maintenances = DBQueries().query_get_maintenances
        await db_model.start_connection()
        maintenances_data = await db_model.run_query(query_get_maintenances)
        maintenances_df = pd.DataFrame(
            maintenances_data).replace(np.nan, None)
        return maintenances_df
    except Exception as e:
        print(f"Excepcion generada en get_maintenances: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_maintenances {e}"
        )
    finally:
        await db_model.close_connection()


async def update_cassia_maintenance(maintenance):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # Asegurarse de que la instancia esté adjunta a la sesión
        maintenance = await session.merge(maintenance)
        await session.commit()
        await session.refresh(maintenance)
        return maintenance
    except Exception as e:
        print(f"Excepcion generada en update_cassia_maintenance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en update_cassia_maintenance {e}"
        )
    finally:
        await db_model.close_connection()
