from infraestructure.database_model import DB
from models.cassia_reset import CassiaReset
from fastapi import status, HTTPException
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np
from sqlalchemy import update, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from utils.traits import get_datetime_now_with_tz
import asyncio
from cryptography.fernet import Fernet
from utils.settings import Settings

settings = Settings()


async def create_cassia_reset(reset_data: dict):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # PINK
        reset = CassiaReset(**reset_data)
        session.add(reset)
        await session.commit()
        await session.refresh(reset)
        return reset
    except Exception as e:
        print(f"Excepcion generada en create_cassia_reset: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en create_cassia_reset {e}"
        )
    finally:
        await session.close()


async def update_cassia_reset(reset_data):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # Asegurarse de que la instancia esté adjunta a la sesión
        reset = await session.merge(reset_data)
        await session.commit()
        await session.refresh(reset)
        return reset
    except Exception as e:
        print(f"Excepcion generada en update_cassia_reset: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en update_cassia_reset {e}"
        )
    finally:
        await db_model.close_connection()


async def get_resets() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_resets = DBQueries().query_get_resets
        await db_model.start_connection()
        resets_data = await db_model.run_query(query_get_resets)
        resets_df = pd.DataFrame(
            resets_data).replace(np.nan, None)
        return resets_df
    except Exception as e:
        print(f"Excepcion generada en get_resets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_resets {e}"
        )
    finally:
        await db_model.close_connection()


# Método para actualizar múltiples resets en bloque
async def create_cassia_resets_bulk(resets_data: list):
    db_model = DB()
    inserted_count = 0
    try:
        session = await db_model.get_session()
        # Utilizar bulk_insert_mappings para inserciones masivas
        result = await session.execute(
            CassiaReset.__table__.insert(),
            resets_data
        )
        await session.commit()
        # Obtener el número de registros insertados
        inserted_count = result.rowcount

        return {"message": f"Resets inserted {inserted_count} rows", "inserted_count": inserted_count}

    except Exception as e:
        print(f"Excepción generada en create_cassia_resets_bulk: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepción generada en create_cassia_resets_bulk {e}"
        )
    finally:
        await session.close()


async def update_cassia_resets_bulk(resets_data: list):
    db_model = DB()
    session = None
    total_rows_affected = 0
    max_retries = 3
    retry_delay = 5  # seconds

    try:
        session = await db_model.get_session()

        # Create a temporary table
        await session.execute(text("""
            CREATE TEMPORARY TABLE temp_cassia_reset (
                affiliation VARCHAR(255) PRIMARY KEY,
                object_id VARCHAR(255),
                updated_at TIMESTAMP,
                imei VARCHAR(255)
            )
        """))

        # Insert data into the temporary table
        insert_stmt = text("""
            INSERT INTO temp_cassia_reset (affiliation, object_id, updated_at, imei)
            VALUES (:affiliation, :object_id, :updated_at, :imei)
            ON DUPLICATE KEY UPDATE
                object_id = VALUES(object_id),
                updated_at = VALUES(updated_at),
                imei = VALUES(imei)
        """)

        chunk_size = 1000
        current_time = get_datetime_now_with_tz()
        for i in range(0, len(resets_data), chunk_size):
            chunk = resets_data[i:i + chunk_size]
            await session.execute(insert_stmt, [
                {
                    'affiliation': data.get('affiliation'),
                    'object_id': data.get('object_id'),
                    'updated_at': current_time,
                    'imei': data.get('imei')
                }
                for data in chunk if data.get('affiliation')
            ])
            await session.commit()

        # Perform the bulk update in batches
        update_stmt = text("""
            UPDATE cassia_reset cr
            JOIN temp_cassia_reset t ON cr.affiliation = t.affiliation
            SET 
                cr.object_id = t.object_id,
                cr.updated_at = t.updated_at,
                cr.imei = t.imei
            WHERE 
                cr.object_id != t.object_id OR
                cr.imei != t.imei OR
                cr.updated_at < t.updated_at
        """)

        batch_size = 5000
        while True:
            for attempt in range(max_retries):
                try:
                    result = await session.execute(update_stmt)
                    rows_affected = result.rowcount
                    await session.commit()
                    total_rows_affected += rows_affected
                    if rows_affected == 0:
                        break  # No more rows to update
                    break  # Successful update, break the retry loop
                except OperationalError as oe:
                    if attempt < max_retries - 1:
                        await session.rollback()
                        await asyncio.sleep(retry_delay)
                    else:
                        raise  # Re-raise the exception if we've exhausted our retries
            else:
                # This will only execute if the inner loop didn't break (i.e., all retries failed)
                raise Exception("Max retries exceeded for update operation")

            if rows_affected == 0:
                break  # No more rows to update, exit the outer loop

        # Drop the temporary table
        await session.execute(text("DROP TEMPORARY TABLE IF EXISTS temp_cassia_reset"))
        await session.commit()

        return {"message": f"Resets updated {total_rows_affected} rows", "updated_count": total_rows_affected}

    except SQLAlchemyError as e:
        print(f"Exception generated in update_cassia_resets_bulk: {e}")
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Exception generated in update_cassia_resets_bulk: {str(e)}"
        )
    finally:
        if session:
            await session.close()


async def get_affiliations_by_hosts_ids(hosts_ids: list):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # RESETS
        query = text(
            "SELECT DISTINCT h.hostid, RIGHT(h.host, 16) AS affiliation, "
            "CASE WHEN cr.object_id IS NOT NULL THEN TRUE ELSE FALSE END as object_id "
            "FROM hosts h INNER JOIN host_inventory hi ON h.hostid = hi.hostid LEFT JOIN "
            "cassia_reset cr ON RIGHT(h.host, 16) = cr.affiliation WHERE h.hostid in :hostids ")
        affiliations = pd.DataFrame(await session.execute(
            query, {'hostids': hosts_ids})).replace(np.nan, "")
        return affiliations
    except Exception as e:
        print(f"Excepcion generada en get_affiliations_by_hosts_ids: {e}")
        affiliations = pd.DataFrame(columns=['hostid', 'affiliation', 'object_id'])
        return affiliations
    finally:
        await session.close()


async def get_devices_related_layer1(hostid):
    db_model = DB()
    session = None
    try:
        session = await db_model.get_session()
        sp_get_devices_related = DBQueries().storeProcedure_getDispositivosCapa1
        await db_model.start_connection()
        devices_related_data = await db_model.run_stored_procedure(sp_get_devices_related, (f"{hostid}",))
        devices_related_data_df = pd.DataFrame(
            devices_related_data).replace(np.nan, None)
        return devices_related_data_df
    except Exception as e:
        print(f"Excepcion generada en get_devices_related_layer1: {e}")
        affiliations = pd.DataFrame(columns=['hostid', 'affiliation', 'object_id'])
        return affiliations
    finally:
        await session.close()


async def get_credentials_for_proxy(ip):
    db_model = DB()
    session = None
    try:
        session = await db_model.get_session()
        sp_get_proxy_credential = DBQueries().stored_name_get_proxy_credential
        await db_model.start_connection()
        credential_data = await db_model.run_stored_procedure(sp_get_proxy_credential, (f"{ip}",))
        if credential_data:
            credential_data_df = pd.DataFrame(credential_data).replace(
                np.nan, "").to_dict(orient="records")
            ip_proxy = credential_data_df[0]['ip']
            user_proxy = decrypt(
                credential_data_df[0]['usr'], settings.ssh_key_gen).decode()
            password_proxy = decrypt(
                credential_data_df[0]['psswrd'], settings.ssh_key_gen).decode()
        else:
            raise ValueError(
                "El proxy no esta configurado o las credenciales configuradas son incorrectas")
    finally:
        await session.close()

    return ip_proxy, user_proxy, password_proxy


def decrypt(encripted_text, key):
    fernet = Fernet(key)
    return fernet.decrypt(encripted_text.encode())


async def get_reset_by_affiliation(affiliation) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_reset = DBQueries().builder_query_statement_get_reset_by_affiliation(affiliation)
        await db_model.start_connection()
        reset_data = await db_model.run_query(query_get_reset)
        reset_df = pd.DataFrame(
            reset_data).replace(np.nan, None)
        return reset_df
    except Exception as e:
        print(f"Excepcion generada en get_reset_by_affiliation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_reset_by_affiliation {e}"
        )
    finally:
        await db_model.close_connection()
