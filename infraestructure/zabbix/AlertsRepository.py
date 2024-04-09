import pandas as pd
from utils.db import DB_Zabbix, DB_Prueba
from sqlalchemy import text
import numpy as np
from datetime import datetime
import pytz
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from infraestructure.cassia import CassiaConfigRepository
from infraestructure.cassia import CassiaDiagnostaRepository
from fastapi import status, HTTPException


async def get_exceptions_async(hostids: list):
    db_model = DB()
    try:
        session = await db_model.get_session()
        query = text(
            "select ce.*,cea.name as agency_name from cassia_exceptions ce inner join cassia_exception_agencies cea on cea.exception_agency_id=ce.exception_agency_id where hostid in :hostids and closed_at is null")
        exceptions = pd.DataFrame(await session.execute(
            query, {'hostids': hostids})).replace(np.nan, "")
        if exceptions.empty:
            exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                               'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid', 'agency_name', 'exception_message'])
        else:
            now = datetime.now(pytz.timezone('America/Mexico_City'))
            exceptions['created_at'] = pd.to_datetime(exceptions['created_at'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
                pytz.timezone('America/Mexico_City'))
            exceptions['diferencia'] = now-exceptions['created_at']
            exceptions['dias'] = exceptions['diferencia'].dt.days
            exceptions['horas'] = exceptions['diferencia'].dt.components.hours
            exceptions['minutos'] = exceptions['diferencia'].dt.components.minutes
            exceptions = exceptions.drop(columns=['diferencia',])
            exceptions['diferencia'] = exceptions.apply(
                lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
            exceptions['exception_message'] = exceptions.apply(
                lambda x: f"Agencia: {x['agency_name']} --- Afectado desde: {x['diferencia']} --- Nota: {x['description']}", axis=1)
            exceptions = exceptions.drop(
                columns=['diferencia', 'dias', 'horas', 'minutos'])
        return exceptions
    except Exception as e:
        print(f"Excepcion generada en get_exceptions_async: {e}")
        exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                           'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid', 'agency_name', 'exception_message'])
        return exceptions
    finally:
        await session.close()


async def get_exceptions(hostids: list, session):
    try:
        query = text(
            "select ce.*,cea.name as agency_name from cassia_exceptions ce inner join cassia_exception_agencies cea on cea.exception_agency_id=ce.exception_agency_id where hostid in :hostids and closed_at is null")

        exceptions = pd.DataFrame(session.execute(
            query, {'hostids': hostids})).replace(np.nan, "")

        if exceptions.empty:
            exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                               'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid', 'agency_name', 'exception_message'])
        else:
            now = datetime.now(pytz.timezone('America/Mexico_City'))
            exceptions['created_at'] = pd.to_datetime(exceptions['created_at'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
                pytz.timezone('America/Mexico_City'))
            exceptions['diferencia'] = now-exceptions['created_at']
            exceptions['dias'] = exceptions['diferencia'].dt.days
            exceptions['horas'] = exceptions['diferencia'].dt.components.hours
            exceptions['minutos'] = exceptions['diferencia'].dt.components.minutes
            exceptions = exceptions.drop(columns=['diferencia',])
            exceptions['diferencia'] = exceptions.apply(
                lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
            exceptions['exception_message'] = exceptions.apply(
                lambda x: f"Agencia: {x['agency_name']} --- Afectado desde: {x['diferencia']} --- Nota: {x['description']}", axis=1)
            exceptions = exceptions.drop(
                columns=['diferencia', 'dias', 'horas', 'minutos'])
        return exceptions
    except Exception as e:
        print(f"Excepcion generada en get_exceptions: {e}")
        exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                           'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid', 'agency_name', 'exception_message'])
        return exceptions


async def get_host_alerts(hostid):
    try:
        zabbix_alerts = await get_host_zabbix_alerts(hostid)

        ping_loss_message = await CassiaConfigRepository.get_config_ping_loss_message()

        zabbix_alerts = await normalizar_alertas_zabbix(zabbix_alerts, ping_loss_message)
        cassia_alerts = await get_host_cassia_alerts(hostid)

        problems = zabbix_alerts

        if not cassia_alerts.empty:
            diagnosta_events = await CassiaDiagnostaRepository.get_local_events_diagnosta_by_host(
                hostid)

            cassia_alerts = await process_cassia_alerts(cassia_alerts)
            problems = pd.concat([cassia_alerts, problems],
                                 ignore_index=True).replace(np.nan, "")

            if not diagnosta_events.empty:
                problems.loc[problems['eventid'].isin(
                    diagnosta_events['local_eventid'].to_list()), 'tipo'] = 1

        dependientes = await CassiaDiagnostaRepository.get_host_dependientes()

        if not dependientes.empty:
            problems = await process_dependientes_data(problems, ping_loss_message, dependientes)

        problemas_sincronizados_diagnosta = await CassiaDiagnostaRepository.get_open_problems_diagnosta()

        if not problemas_sincronizados_diagnosta.empty:

            if not problems.empty:

                problems = await process_sincronizados_data(problems, ping_loss_message, problemas_sincronizados_diagnosta)

        if not problems.empty:
            problems = await procesar_fechas_problemas(problems)

        if not problems.empty:
            exceptions = await get_exceptions_async(problems['hostid'].to_list())
            problems = pd.merge(problems, exceptions, on='hostid',
                                how='left').replace(np.nan, None)

        return problems
    except Exception as e:
        print(f"Excepcion en get_host_alerts: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_alerts {e}")


async def get_host_zabbix_alerts(hositd) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_zabbix_alerts = DBQueries(
        ).builder_query_statement_get_hots_zabbix_alerts(hositd)

        await db_model.start_connection()
        zabbix_alerts_data = await db_model.run_query(query_get_zabbix_alerts)
        zabbix_alerts_df = pd.DataFrame(
            zabbix_alerts_data).replace(np.nan, None)
        if zabbix_alerts_df.empty:
            zabbix_alerts_df = pd.DataFrame(columns=['Time', 'severity', 'hostid', 'Host', 'latitude',
                                                     'longitude', 'ip', 'Problem', 'Estatus', 'r_eventid', 'Ack_message', 'local', 'tipo', 'dependents', 'alert_type'])
        return zabbix_alerts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_zabbix_alerts {e}")
    finally:
        await db_model.close_connection()


async def normalizar_alertas_zabbix(zabbix_alerts: pd.DataFrame, ping_loss_message):
    if not zabbix_alerts.empty:
        zabbix_alerts['local'] = 0
        zabbix_alerts['tipo'] = 0
        zabbix_alerts.loc[zabbix_alerts['Problem'] ==
                          ping_loss_message, 'tipo'] = 1
        zabbix_alerts['dependents'] = 0
        zabbix_alerts['alert_type'] = ""
    return zabbix_alerts


async def get_host_cassia_alerts(hositd) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_cassia_alerts = DBQueries(
        ).builder_query_statement_get_hots_cassia_alerts(hositd)

        await db_model.start_connection()
        cassia_alerts_data = await db_model.run_query(query_get_cassia_alerts)
        cassia_alerts_df = pd.DataFrame(
            cassia_alerts_data).replace(np.nan, None)
        return cassia_alerts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_cassia_alerts {e}")
    finally:
        await db_model.close_connection()


async def process_cassia_alerts(cassia_alerts) -> pd.DataFrame:

    cassia_alerts['r_eventid'] = [
        '' for i in range(len(cassia_alerts))]
    cassia_alerts['Ack'] = [0 for i in range(len(cassia_alerts))]

    cassia_alerts['manual_close'] = [
        0 for i in range(len(cassia_alerts))]
    cassia_alerts['dependents'] = [
        0 for i in range(len(cassia_alerts))]
    cassia_alerts['local'] = [
        1 for i in range(len(cassia_alerts))]

    cassia_alerts['tipo'] = [
        0 for i in range(len(cassia_alerts))]
    cassia_alerts.drop(columns={'updated_at', 'tech_id'}, inplace=True)
    cassia_alerts['created_at'] = pd.to_datetime(
        cassia_alerts['created_at'])
    cassia_alerts["created_at"] = cassia_alerts['created_at'].dt.strftime(
        '%d/%m/%Y %H:%M:%S')
    cassia_alerts.rename(columns={
        'created_at': 'Time',
        'closed_at': 'TimeRecovery',
        'hostname': 'Host',
        'message': 'Problem',
        'status': 'Estatus',
        'cassia_arch_traffic_events_id': 'eventid',
    }, inplace=True)
    return cassia_alerts


async def process_dependientes_data(problems, ping_loss_message, dependientes) -> pd.DataFrame:
    indexes = problems[problems['Problem'] == ping_loss_message]
    indexes = indexes[indexes['hostid'].isin(
        dependientes['hostid'].to_list())]
    problems.loc[problems.index.isin(
        indexes.index.to_list()), 'tipo'] = 0
    return problems


async def process_sincronizados_data(problems, ping_loss_message, sincronizados: pd.DataFrame):

    for ind in problems.index:

        if problems['Problem'][ind] == ping_loss_message:
            dependientes = sincronizados[sincronizados['hostid_origen']
                                         == problems['hostid'][ind]]

            """ dependientes['depends_hostid'] = dependientes['depends_hostid'].astype(
                'int') """
            dependientes = dependientes[dependientes['depends_hostid'] != None]

            dependientes = dependientes.drop_duplicates(
                subset=['depends_hostid'])
            problems.loc[problems.index == ind,
                         'dependents'] = len(dependientes)

    return problems


async def procesar_fechas_problemas(problems):
    now = datetime.now(pytz.timezone('America/Mexico_City'))
    problems['fecha'] = pd.to_datetime(problems['Time'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
        pytz.timezone('America/Mexico_City'))
    problems['diferencia'] = now-problems['fecha']
    problems['dias'] = problems['diferencia'].dt.days
    problems['horas'] = problems['diferencia'].dt.components.hours
    problems['minutos'] = problems['diferencia'].dt.components.minutes
    problems.loc[problems['alert_type'].isin(
        ['rfid', 'lpr']), 'Problem'] = problems.loc[problems['alert_type'].isin(['rfid', 'lpr']), ['dias', 'horas', 'minutos']].apply(lambda x:
                                                                                                                                      f"Este host no ha tenido lecturas por más de {x['dias']} dias {x['horas']} hrs {x['minutos']} min" if x['dias'] > 0
                                                                                                                                      else f"Este host no ha tenido lecturas por más de {x['horas']} hrs {x['minutos']} min" if x['horas'] > 0
                                                                                                                                      else f"Este host no ha tenido lecturas por más de {x['minutos']} min", axis=1)
    problems = problems.drop(columns=['diferencia'])
    problems['diferencia'] = problems.apply(
        lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
    problems = problems.sort_values(by='fecha', ascending=False)
    return problems
