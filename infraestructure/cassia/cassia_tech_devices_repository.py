from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_technologies import CassiaTechnologiesModel
from models.cassia_tech_services import CassiaTechServiceModel
from models.cassia_techs import CassiaTechModel
from models.cassia_tech_devices import CassiaTechDeviceModel
from schemas import cassia_technologies_schema
from schemas import cassia_service_tech_schema
from schemas import cassia_techs_schema
from schemas import cassia_tech_device_schema
import pandas as pd
import numpy as np
import utils.traits as traits
from datetime import datetime


async def exist_devices(hostids):
    db_model = DB()
    try:
        query_get_created_devices_by_ids = DBQueries(
        ).builder_query_get_created_devices_by_ids(hostids)
        await db_model.start_connection()
        cassia_devices_data = await db_model.run_query(query_get_created_devices_by_ids)
        cassia_devices_df = pd.DataFrame(
            cassia_devices_data).replace(np.nan, None)
        return cassia_devices_df

    except Exception as e:
        print(f"Excepcion en get_downs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_downs: {e}")
    finally:
        await db_model.close_connection()


async def get_downs():
    db_model = DB()
    try:
        sp_get_host_validation_down = DBQueries(
        ).sp_get_host_validation_down
        await db_model.start_connection()
        cassia_downs_data = await db_model.run_stored_procedure(sp_get_host_validation_down, ())
        cassia_downs_df = pd.DataFrame(
            cassia_downs_data).replace(np.nan, None)
        return cassia_downs_df

    except Exception as e:
        print(f"Excepcion en get_downs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_downs: {e}")
    finally:
        await db_model.close_connection()


async def get_tech_device_by_id(cassia_tech_device_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_tech_device_by_id = DBQueries(
        ).builder_query_get_tech_device_by_id(cassia_tech_device_id)
        await db_model.start_connection()
        cassia_tech_device_data = await db_model.run_query(query_get_tech_device_by_id)
        cassia_tech_device_df = pd.DataFrame(
            cassia_tech_device_data).replace(np.nan, None)
        return cassia_tech_device_df

    except Exception as e:
        print(f"Excepcion en get_tech_device_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_tech_device_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_devices_by_tech_id(tech_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_devices_by_tech_id = DBQueries(
        ).builder_query_get_devices_by_tech_id(tech_id)
        await db_model.start_connection()
        cassia_tech_devices_data = await db_model.run_query(query_get_devices_by_tech_id)
        cassia_tech_devices_df = pd.DataFrame(
            cassia_tech_devices_data).replace(np.nan, None)
        return cassia_tech_devices_df

    except Exception as e:
        print(f"Excepcion en get_devices_by_tech_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_devices_by_tech_id: {e}")
    finally:
        await db_model.close_connection()


async def create_device(hostid, criticidad, device_data: cassia_tech_device_schema.CassiaTechDeviceSchema):
    db_model = DB()
    try:
        session = await db_model.get_session()
        now = traits.get_datetime_now_with_tz()
        cassia_tech = CassiaTechDeviceModel(
            criticality_id=criticidad,
            hostid=hostid,
            cassia_tech_id=device_data.cassia_tech_id,
            created_at=now,
            updated_at=now,
        )
        session.add(cassia_tech)
        await session.commit()
        await session.refresh(cassia_tech)
        return cassia_tech

    except Exception as e:
        print(f"Excepcion en create_device: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_device: {e}")
    finally:
        await session.close()


async def update_device(cassia_tech_device_id, device_data: cassia_tech_device_schema.UpdateCassiaTechDeviceSchema):
    db_model = DB()
    try:
        query_update_cassia_tech_device_by_id = DBQueries(
        ).builder_query_update_cassia_tech_device_by_id(cassia_tech_device_id, device_data)
        print(query_update_cassia_tech_device_by_id)
        await db_model.start_connection()
        updated_tech = await db_model.run_query(query_update_cassia_tech_device_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en update_device: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_device: {e}")
    finally:
        await db_model.close_connection()


async def delete_tech_device_by_ids(device_ids) -> bool:
    db_model = DB()
    try:
        query_delete_cassia_tech_device_by_ids = DBQueries(
        ).builder_query_delete_cassia_tech_device_by_ids(device_ids)
        await db_model.start_connection()
        deleted_tech = await db_model.run_query(query_delete_cassia_tech_device_by_ids)
        return True

    except Exception as e:
        print(f"Excepcion en delete_tech_device_by_ids: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_tech_device_by_ids: {e}")
    finally:
        await db_model.close_connection()
