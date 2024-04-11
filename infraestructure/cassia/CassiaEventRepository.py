from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_cassia_event(eventid) -> pd.DataFrame:
    db_model = DB()
    try:
        get_event_query = DBQueries().builder_query_statement_get_cassia_event(
            eventid)
        """ print("Query: ", get_event_query) """
        await db_model.start_connection()
        event_data = await db_model.run_query(get_event_query)
        event = pd.DataFrame(event_data).replace(np.nan, None)

        return event
    except Exception as e:
        print(f"Excepcion en get_cassia_event: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_cassia_event: {e}")
    finally:
        await db_model.close_connection()


async def get_global_alerts_by_tech(tech_id, tipo):
    db_model = DB()
    try:
        sp_name_get_cassia_events_by_tech = DBQueries(
        ).builder_query_statement_get_global_cassia_events_by_tech(tech_id)
        await db_model.start_connection()
        cassia_alerts_data = await db_model.run_query(tech_id)
        cassia_alerts_df = pd.DataFrame(
            cassia_alerts_data).replace(np.nan, None)
        if not cassia_alerts_df.empty:
            cassia_alerts_df['r_eventid'] = ''
            cassia_alerts_df['TimeRecovery'] = ''
            cassia_alerts_df['Ack'] = 0
            cassia_alerts_df['Ack_message'] = ''
            cassia_alerts_df["manual_close"] = 0
            cassia_alerts_df['alert_type'] = tipo
            cassia_alerts_df['local'] = 1
            cassia_alerts_df['dependents'] = 0
            cassia_alerts_df.rename(columns={'created_at': 'Time',
                                             'hostname': 'Host',
                                             'message': 'Problem',
                                             'status': 'Estatus',
                                             'cassia_arch_traffic_events_id': 'eventid'}, inplace=True)
        return cassia_alerts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_global_alerts_by_tech {e}")
    finally:
        await db_model.close_connection()


async def get_municipality_alerts_by_tech(municipality, tech_id, tipo):
    db_model = DB()
    try:
        query_get_alerts_by_municipality_and_tech = DBQueries(
        ).builder_query_statement_get_cassia_events_by_tech_and_municipality(municipality, tech_id)
        await db_model.start_connection()
        cassia_alerts_data = await db_model.run_query(query_get_alerts_by_municipality_and_tech)
        cassia_alerts_df = pd.DataFrame(
            cassia_alerts_data).replace(np.nan, None)
        if not cassia_alerts_df.empty:
            cassia_alerts_df['r_eventid'] = ''
            cassia_alerts_df['TimeRecovery'] = ''
            cassia_alerts_df['Ack'] = 0
            cassia_alerts_df['Ack_message'] = ''
            cassia_alerts_df["manual_close"] = 0
            cassia_alerts_df['alert_type'] = tipo
            cassia_alerts_df['local'] = 1
            cassia_alerts_df['dependents'] = 0
            cassia_alerts_df.rename(columns={'created_at': 'Time',
                                             'hostname': 'Host',
                                             'message': 'Problem',
                                             'status': 'Estatus',
                                             'cassia_arch_traffic_events_id': 'eventid'}, inplace=True)
        return cassia_alerts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_municipality_alerts_by_tech {e}")
    finally:
        await db_model.close_connection()


async def get_cassia_events_acknowledges():
    db_model = DB()
    try:
        query_get_cassia_events_acknowledges = DBQueries(
        ).query_statement_get_cassia_events_acknowledges
        await db_model.start_connection()
        cassia_alerts_acknowledges_data = await db_model.run_query(query_get_cassia_events_acknowledges)
        cassia_alerts_acknowledges_df = pd.DataFrame(
            cassia_alerts_acknowledges_data).replace(np.nan, None)
        return cassia_alerts_acknowledges_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_events_acknowledges {e}")
    finally:
        await db_model.close_connection()


async def get_cassia_events_with_hosts_filter(hostids: str):
    db_model = DB()
    try:
        query_get_cassia_events_with_hosts_filter = DBQueries(
        ).builder_query_statement_get_cassia_events_with_hosts_filter(hostids)
        await db_model.start_connection()
        cassia_alerts_data = await db_model.run_query(query_get_cassia_events_with_hosts_filter)
        cassia_alerts_df = pd.DataFrame(
            cassia_alerts_data).replace(np.nan, None)
        return cassia_alerts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_events_with_hosts_filter {e}")
    finally:
        await db_model.close_connection()
