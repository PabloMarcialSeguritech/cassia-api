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
import pytz


async def get_tech_ids_by_service_id(service_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_tech_ids_by_service_id = DBQueries(
        ).builder_query_get_tech_ids_by_service_id(service_id)
        await db_model.start_connection()
        cassia_tech_ids_data = await db_model.run_query(query_get_tech_ids_by_service_id)
        cassia_tech_ids_df = pd.DataFrame(
            cassia_tech_ids_data).replace(np.nan, None)
        return cassia_tech_ids_df

    except Exception as e:
        print(f"Excepcion en get_tech_ids_by_service_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_tech_ids_by_service_id: {e}")
    finally:
        await db_model.close_connection()


def calcular_promedio(lista):
    if len(lista) == 0:
        return "NA"
    return round(sum(lista) / len(lista), 2)


async def get_sla_by_service(service_id):
    tech_ids = await get_tech_ids_by_service_id(service_id)
    promedios = []
    for ind in tech_ids.index:
        sla_tech = await get_sla_by_tech(tech_ids['cassia_tech_id'][ind], tech_ids['sla_hours'][ind])
        promedios.append(sla_tech)
    numericos = [item for item in promedios if item != "NA"]
    return calcular_promedio(numericos)


async def get_sla_by_tech(tech_id, sla_hours):
    devices = await get_devices_by_tech_id(tech_id)
    if not devices.empty:
        devices['sla'] = 100
        hostids = ",".join([str(hostid)
                           for hostid in devices['hostid'].to_list()])
        down_events = await get_down_events_by_hostids(hostids)
        now = traits.get_datetime_now_with_tz()
        tz = pytz.timezone('America/Mexico_City')
        sla = sla_hours
        if not down_events.empty:
            for ind in devices.index:
                event = down_events.loc[down_events['hostid']
                                        == devices['hostid'][ind]]
                if not event.empty:
                    event = event.iloc[0].to_dict()
                    problem_date = tz.localize(event['created_at'])
                    tiempo = (now-problem_date).total_seconds()/3600
                    porcentaje_sla = (tiempo/sla)*100
                    porcentaje_sla_rounded = round(porcentaje_sla, 2)
                    sla_restante = 100-porcentaje_sla_rounded
                    sla_restante = sla_restante if sla_restante > 0 else 0
                    devices['sla'][ind] = sla_restante
        promedio_sla = round(devices['sla'].mean(), 2)
    else:
        promedio_sla = "NA"
    return promedio_sla


async def get_down_events_by_hostids(hostids) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_down_events_by_hostids = DBQueries(
        ).builder_query_get_down_events_by_hostids(hostids)
        await db_model.start_connection()
        cassia_down_devices_data = await db_model.run_query(query_get_down_events_by_hostids)
        cassia_down_devices_df = pd.DataFrame(
            cassia_down_devices_data).replace(np.nan, None)
        return cassia_down_devices_df

    except Exception as e:
        print(f"Excepcion en get_down_events_by_hostids: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_down_events_by_hostids: {e}")
    finally:
        await db_model.close_connection()


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
