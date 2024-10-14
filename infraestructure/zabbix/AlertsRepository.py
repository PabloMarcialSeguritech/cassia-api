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
from infraestructure.cassia import CassiaEventRepository
from infraestructure.cassia import CassiaResetRepository
from infraestructure.cassia import cassia_gs_tickets_repository
from fastapi import status, HTTPException
import time
import asyncio


async def get_exceptions_async_pool(hostids: list, db):

    try:
        # PINK
        hostids_str = "0"
        if len(hostids) > 0:
            hostids_str = ",".join([str(hostid) for hostid in hostids])
        query = f"select ce.*,cea.name as agency_name from cassia_exceptions_test ce inner join cassia_exception_agencies cea on cea.exception_agency_id=ce.exception_agency_id where hostid in ({hostids_str}) and closed_at is null"
        exceptions = pd.DataFrame(await db.run_query(query)).replace(np.nan, "")
        if exceptions.empty:
            exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                               'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid',
                                               'agency_name', 'exception_message'])
        else:
            now = datetime.now(pytz.timezone('America/Mexico_City'))
            exceptions['created_at'] = pd.to_datetime(exceptions['created_at'],
                                                      format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
                pytz.timezone('America/Mexico_City'))
            exceptions['diferencia'] = now - exceptions['created_at']
            exceptions['dias'] = exceptions['diferencia'].dt.days
            exceptions['horas'] = exceptions['diferencia'].dt.components.hours
            exceptions['minutos'] = exceptions['diferencia'].dt.components.minutes
            exceptions = exceptions.drop(columns=['diferencia', ])
            exceptions['diferencia'] = exceptions.apply(
                lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
            exceptions['exception_message'] = exceptions.apply(
                lambda
                x: f"Agencia: {x['agency_name']} --- Afectado desde: {x['diferencia']} --- Nota: {x['description']}",
                axis=1)
            exceptions = exceptions.drop(
                columns=['diferencia', 'dias', 'horas', 'minutos'])
        return exceptions
    except Exception as e:
        print(f"Excepcion generada en get_exceptions_async: {e}")
        exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                           'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid',
                                           'agency_name', 'exception_message'])
        return exceptions


async def get_exceptions_async(hostids: list):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # PINK
        query = text(
            "select ce.*,cea.name as agency_name from cassia_exceptions_test ce inner join cassia_exception_agencies cea on cea.exception_agency_id=ce.exception_agency_id where hostid in :hostids and closed_at is null")
        exceptions = pd.DataFrame(await session.execute(
            query, {'hostids': hostids})).replace(np.nan, "")
        if exceptions.empty:
            exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                               'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid',
                                               'agency_name', 'exception_message'])
        else:
            now = datetime.now(pytz.timezone('America/Mexico_City'))
            exceptions['created_at'] = pd.to_datetime(exceptions['created_at'],
                                                      format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
                pytz.timezone('America/Mexico_City'))
            exceptions['diferencia'] = now - exceptions['created_at']
            exceptions['dias'] = exceptions['diferencia'].dt.days
            exceptions['horas'] = exceptions['diferencia'].dt.components.hours
            exceptions['minutos'] = exceptions['diferencia'].dt.components.minutes
            exceptions = exceptions.drop(columns=['diferencia', ])
            exceptions['diferencia'] = exceptions.apply(
                lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
            exceptions['exception_message'] = exceptions.apply(
                lambda
                x: f"Agencia: {x['agency_name']} --- Afectado desde: {x['diferencia']} --- Nota: {x['description']}",
                axis=1)
            exceptions = exceptions.drop(
                columns=['diferencia', 'dias', 'horas', 'minutos'])
        return exceptions
    except Exception as e:
        print(f"Excepcion generada en get_exceptions_async: {e}")
        exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                           'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid',
                                           'agency_name', 'exception_message'])
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
                                               'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid',
                                               'agency_name', 'exception_message'])
        else:
            now = datetime.now(pytz.timezone('America/Mexico_City'))
            exceptions['created_at'] = pd.to_datetime(exceptions['created_at'],
                                                      format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
                pytz.timezone('America/Mexico_City'))
            exceptions['diferencia'] = now - exceptions['created_at']
            exceptions['dias'] = exceptions['diferencia'].dt.days
            exceptions['horas'] = exceptions['diferencia'].dt.components.hours
            exceptions['minutos'] = exceptions['diferencia'].dt.components.minutes
            exceptions = exceptions.drop(columns=['diferencia', ])
            exceptions['diferencia'] = exceptions.apply(
                lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
            exceptions['exception_message'] = exceptions.apply(
                lambda
                x: f"Agencia: {x['agency_name']} --- Afectado desde: {x['diferencia']} --- Nota: {x['description']}",
                axis=1)
            exceptions = exceptions.drop(
                columns=['diferencia', 'dias', 'horas', 'minutos'])
        return exceptions
    except Exception as e:
        print(f"Excepcion generada en get_exceptions: {e}")
        exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                           'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid',
                                           'agency_name', 'exception_message'])
        return exceptions


async def get_host_alerts(hostid):
    zabbix_alerts = await get_host_zabbix_alerts(hostid)
    ping_loss_message = await CassiaConfigRepository.get_config_ping_loss_message()
    zabbix_alerts = await normalizar_alertas_zabbix(zabbix_alerts, ping_loss_message)
    zabbix_alerts = await eliminar_downs_zabbix(zabbix_alerts, ping_loss_message)
    cassia_alerts = await get_host_cassia_alerts(hostid)
    problems = zabbix_alerts
    print("***************ANTES")
    print(cassia_alerts.columns)
    print(cassia_alerts)

    if not cassia_alerts.empty:
        diagnosta_events = await CassiaDiagnostaRepository.get_local_events_diagnosta_by_host(
            hostid)
        cassia_alerts = await process_cassia_alerts(cassia_alerts)
        problems = pd.concat([cassia_alerts, problems],
                             ignore_index=True).replace(np.nan, "")
        if not diagnosta_events.empty:
            problems.loc[problems['eventid'].isin(
                diagnosta_events['local_eventid'].to_list()), 'tipo'] = 1
    print("***************DESPUES")
    print(cassia_alerts.columns)
    print(cassia_alerts)
    dependientes = await CassiaDiagnostaRepository.get_host_dependientes()
    if dependientes.empty:
        dependientes = pd.DataFrame(columns=['hostid'])
    if 'hostid' in dependientes.columns:
        problems = await process_dependientes_data(problems, ping_loss_message, dependientes)
    problemas_sincronizados_diagnosta = await CassiaDiagnostaRepository.get_open_problems_diagnosta()
    if not problemas_sincronizados_diagnosta.empty:
        if not problems.empty:
            problems = await process_sincronizados_data(problems, ping_loss_message,
                                                        problemas_sincronizados_diagnosta)
    if not problems.empty:
        problems = await procesar_fechas_problemas(problems)
    if not problems.empty:
        exceptions = await get_exceptions_async(problems['hostid'].to_list())
        print(exceptions)
        if not exceptions.empty:
            exceptions['created_at'] = pd.to_datetime(
                exceptions['created_at'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize(None)
            problems = pd.merge(problems, exceptions, on='hostid',
                                how='left').replace(np.nan, None)
            problems['acknowledges_concatenados'] = problems['exception_message'].fillna(
                '')
        else:
            problems['acknowledges_concatenados'] = None
    if not problems.empty:
        problems_zabbix = problems[problems['local'] == 0]
        problems_cassia = problems[problems['local'] == 1]

        # Obtener acknowledgements
        zabbix_eventids = ','.join(problems_zabbix['eventid'].astype(
            str).tolist()) if not problems_zabbix.empty else '0'
        cassia_eventids = ','.join(problems_cassia['eventid'].astype(
            str).tolist()) if not problems_cassia.empty else '0'
        task_2 = {
            'acks_zabbix_df': get_zabbix_acks(zabbix_eventids),
            'acks_cassia_df': get_cassia_acks(cassia_eventids)
        }
        results2 = await asyncio.gather(*task_2.values())
        acks_zabbix = results2[0]
        acks_cassia = results2[1]

        # Concatenar acknowledges
        if not acks_zabbix.empty:
            acks_zabbix_mensajes = acks_zabbix.groupby(
                'eventid')['message'].agg(' | '.join).reset_index()
            problems_zabbix = pd.merge(
                problems_zabbix, acks_zabbix_mensajes, on='eventid', how='left')

        if not acks_cassia.empty:
            acks_cassia_mensajes = acks_cassia.groupby(
                'eventid')['message'].agg(' | '.join).reset_index()
            problems_cassia = pd.merge(
                problems_cassia, acks_cassia_mensajes, on='eventid', how='left')

        # Concatenar problemas zabbix y cassia
        problems = pd.concat([problems_zabbix, problems_cassia]).sort_values(
            by='fecha', ascending=False)

        print(problems.columns)
        print(problems)
        if 'message' in problems.columns:
            problems['acknowledges_concatenados'] = problems['acknowledges_concatenados'] + \
                problems['message'].fillna('')
    if 'acknowledges_concatenados' in problems.columns:
        problems['acknowledges_concatenados'] = problems['acknowledges_concatenados'].replace(
            '', None)
    affiliations_df = await CassiaResetRepository.get_affiliations_by_hosts_ids(problems['hostid'].tolist())
    if not affiliations_df.empty:
        # Realizar el merge para agregar las columnas de affiliations_df a problems
        problems = pd.merge(problems, affiliations_df,
                            on='hostid', how='left').replace(np.nan, None)
    hostids_str_2 = ",".join(
        [str(hostid) for hostid in problems['hostid'].tolist()]) if not problems.empty else "0"
    serial_numbers_df = await cassia_gs_tickets_repository.get_serial_numbers_by_host_ids(hostids_str_2)
    if not serial_numbers_df.empty:
        # Realizar el merge para agregar las columnas de serial_numbers_df a problems
        problems = pd.merge(problems, serial_numbers_df,
                            on='hostid', how='left').replace(np.nan, None)
    problems['create_ticket'] = 0
    suscriptores_id = await CassiaConfigRepository.get_config_value_by_name('suscriptores_id')
    suscriptores_id = suscriptores_id['value'][0] if not suscriptores_id.empty else 11
    if all(col in problems.columns for col in ['affiliation', 'no_serie']):
        problems['create_ticket'] = problems.apply(
            lambda row: assign_value(row, suscriptores_id), axis=1)
        print("Ambas columnas existen en el DataFrame.")
    last_error_tickets = await cassia_gs_tickets_repository.get_last_error_tickets()
    problems['has_ticket'] = False
    problems['ticket_id'] = None
    problems['ticket_error'] = None
    problems['ticket_status'] = None
    """ print("***************************PROBLEMS")
    print(problems.columns)
    print(problems) """

    if not problems.empty:
        problems['tech_id'] = problems['tech_id'].astype(str)
        suscriptores_id_str = str(suscriptores_id)
        active_tickets = await cassia_gs_tickets_repository.get_active_tickets()
        if not last_error_tickets.empty:
            # Crear una máscara para las filas que cumplen con las condiciones
            mask = (
                (problems['affiliation'].isin(last_error_tickets['afiliacion'])) &
                (problems['tech_id'] == suscriptores_id_str)
            )
            # Actualizar las columnas 'ticket_error' y 'ticket_status' basadas en la máscara
            problems.loc[mask, 'ticket_error'] = last_error_tickets.set_index(
                'afiliacion').loc[problems.loc[mask, 'affiliation'], 'error'].values
            problems.loc[mask, 'ticket_status'] = 'error'

        if not active_tickets.empty:
            active_tickets['ticket_active_id'] = pd.to_numeric(active_tickets['ticket_active_id'], errors='coerce').astype(
                'Int64')
            print("****************ACTIVE TICKETS")
            print(active_tickets)
            # Asegúrate de que ambos DataFrames tengan una columna en común para poder unirlos, por ejemplo, 'affiliation'.
            merged_df = problems.merge(active_tickets[['afiliacion', 'created_at_ticket', 'ticket_active_status', 'ticket_active_id']],
                                       left_on='affiliation',
                                       right_on='afiliacion',
                                       how='left')
            merged_df['Time'] = pd.to_datetime(
                merged_df['Time'], errors='coerce')
            merged_df['created_at_ticket'] = pd.to_datetime(
                merged_df['created_at_ticket'], errors='coerce')

            """ print("*********************TIPOS*******************")
            print(merged_df[['created_at_ticket']].dtypes)
            print("*********************TIPOS*******************")
            print(merged_df[['Time']].dtypes) """
            # Ahora puedes hacer la comparación ya que ambas columnas ('Time' y 'created_at') están en el mismo DataFrame
            mask = merged_df['Time'] <= merged_df['created_at_ticket']
            # Aplicar la máscara y realizar la actualización
            merged_df.loc[mask, 'has_ticket'] = True
            merged_df.loc[mask, 'ticket_id'] = merged_df['ticket_active_id']
            merged_df.loc[mask, 'ticket_error'] = None
            merged_df.loc[mask,
                          'ticket_status'] = merged_df['ticket_active_status']
            merged_df.drop(
                columns=['ticket_active_id', 'ticket_active_status', 'created_at_ticket', 'afiliacion'], inplace=True)
            problems = merged_df

    """ if not problems.empty:
        for index in last_error_tickets.index:
            problems.loc[(problems['affiliation'] == last_error_tickets['afiliacion'][index]) & (
                problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_error'] = last_error_tickets['error'][index]
            problems.loc[(problems['affiliation'] == last_error_tickets['afiliacion']
                          [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_status'] = 'error'
        active_tickets = await cassia_gs_tickets_repository.get_active_tickets()
        for index in active_tickets.index:
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                          [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'has_ticket'] = True
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                          [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_id'] = active_tickets['ticket_id'][index]
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                          [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_error'] = None
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                          [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_status'] = active_tickets['status'][index] """
    """ for index in last_error_tickets.index:
        problems.loc[problems['affiliation'] == last_error_tickets['afiliacion']
                     [index], 'ticket_error'] = last_error_tickets['error'][index]
        problems.loc[problems['affiliation'] == last_error_tickets['afiliacion']
                     [index], 'ticket_status'] = 'error'
    active_tickets = await cassia_gs_tickets_repository.get_active_tickets()
    for index in active_tickets.index:
        problems.loc[problems['affiliation'] == active_tickets['afiliacion']
                     [index], 'has_ticket'] = True
        problems.loc[problems['affiliation'] == active_tickets['afiliacion']
                     [index], 'ticket_id'] = active_tickets['ticket_id'][index]
        problems.loc[problems['affiliation'] == active_tickets['afiliacion']
                     [index], 'ticket_error'] = None
        problems.loc[problems['affiliation'] == active_tickets['afiliacion']
                     [index], 'ticket_status'] = active_tickets['status'][index] """
    return problems


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
                                                     'longitude', 'ip', 'Problem', 'Estatus', 'r_eventid',
                                                     'Ack_message', 'local', 'tipo', 'dependents', 'alert_type'])
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


async def eliminar_downs_zabbix(zabbix_alerts: pd.DataFrame, ping_loss_message):
    print("elminiar fun")
    if not zabbix_alerts.empty:
        zabbix_alerts = zabbix_alerts[zabbix_alerts['Problem']
                                      != ping_loss_message]
    return zabbix_alerts


async def get_host_cassia_alerts(hositd) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        query_get_cassia_alerts = DBQueries(
        ).builder_query_statement_get_hots_cassia_alerts_test(hositd)

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
        1 for i in range(len(cassia_alerts))]
    cassia_alerts['dependents'] = [
        0 for i in range(len(cassia_alerts))]
    cassia_alerts['local'] = [
        1 for i in range(len(cassia_alerts))]

    cassia_alerts['tipo'] = [
        0 for i in range(len(cassia_alerts))]
    cassia_alerts.drop(columns={'updated_at', }, inplace=True)
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
    problems.loc[~problems.index.isin(
        indexes.index.to_list()), 'tipo'] = 1
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
    problems['diferencia'] = now - problems['fecha']
    problems['dias'] = problems['diferencia'].dt.days
    problems['horas'] = problems['diferencia'].dt.components.hours
    problems['minutos'] = problems['diferencia'].dt.components.minutes
    problems.loc[problems['alert_type'].isin(
        ['rfid', 'lpr']), 'Problem'] = problems.loc[
        problems['alert_type'].isin(['rfid', 'lpr']), ['dias', 'horas', 'minutos']].apply(lambda x:
                                                                                          f"Este host no ha tenido lecturas por más de {x['dias']} dias {x['horas']} hrs {x['minutos']} min" if
                                                                                          x['dias'] > 0
                                                                                          else f"Este host no ha tenido lecturas por más de {x['horas']} hrs {x['minutos']} min" if
                                                                                          x['horas'] > 0
                                                                                          else f"Este host no ha tenido lecturas por más de {x['minutos']} min",
                                                                                          axis=1)
    problems = problems.drop(columns=['diferencia'])
    problems['diferencia'] = problems.apply(
        lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
    problems = problems.sort_values(by='fecha', ascending=False)
    return problems


async def get_alerts_pool(municipalityId, tech_host_type, subtype, severities, db):

    try:
        sp_name_get_alerts = DBQueries(
        ).stored_name_get_alerts

        cassia_alerts_data = await db.run_stored_procedure(sp_name_get_alerts,
                                                           (municipalityId, tech_host_type, subtype, severities))
        cassia_alerts_df = pd.DataFrame(
            cassia_alerts_data).replace(np.nan, None)
        return cassia_alerts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_alerts {e}")


async def get_alerts(municipalityId, tech_host_type, subtype, severities):
    db_model = DB()
    try:
        sp_name_get_alerts = DBQueries(
        ).stored_name_get_alerts

        await db_model.start_connection()
        cassia_alerts_data = await db_model.run_stored_procedure(sp_name_get_alerts,
                                                                 (municipalityId, tech_host_type, subtype, severities))
        cassia_alerts_df = pd.DataFrame(
            cassia_alerts_data).replace(np.nan, None)
        return cassia_alerts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_alerts {e}")
    finally:
        await db_model.close_connection()


async def process_alerts_local_pool(data, municipalityId, tech_id, severities, tipo, ping_loss_message, db):
    if municipalityId == '0':
        print("AAAAAA")
        # PINK

        alertas = await CassiaEventRepository.get_global_alerts_by_tech_pool(tech_id, tipo, db)
        print("BBBBB")
        if not alertas.empty:
            print("Entre a not alertas.empty")
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
            print("CCCCC")
    else:
        municipios = await CassiaConfigRepository.get_city_catalog_pool(db)
        municipio_value = ''
        if not municipios.empty:
            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio_value = municipio['name'].item()
        alertas = await CassiaEventRepository.get_municipality_alerts_by_tech_pool(municipio_value, tech_id, tipo, db)

        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
    if not alertas.empty:

        acks = await CassiaEventRepository.get_cassia_events_acknowledges_pool(db)
        if not acks.empty:
            alertas = pd.merge(alertas, acks, left_on='eventid',
                               right_on='eventid', how='left')
            alertas.drop(columns=['Ack_message'], inplace=True)
            alertas.rename(columns={'message': 'Ack_message'}, inplace=True)

    data = pd.concat([alertas, data],
                     ignore_index=True).replace(np.nan, "")
    return data


async def process_alerts_local(data, municipalityId, tech_id, severities, tipo, ping_loss_message):
    if municipalityId == '0':
        print("AAAAAA")
        # PINK

        alertas = await CassiaEventRepository.get_global_alerts_by_tech(tech_id, tipo)
        print("BBBBB")
        if not alertas.empty:
            print("Entre a not alertas.empty")
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
            print("CCCCC")
    else:
        municipios = await CassiaConfigRepository.get_city_catalog()
        municipio_value = ''
        if not municipios.empty:
            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio_value = municipio['name'].item()
        alertas = await CassiaEventRepository.get_municipality_alerts_by_tech(municipio_value, tech_id, tipo)

        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
    if not alertas.empty:

        acks = await CassiaEventRepository.get_cassia_events_acknowledges()
        if not acks.empty:
            alertas = pd.merge(alertas, acks, left_on='eventid',
                               right_on='eventid', how='left')
            alertas.drop(columns=['Ack_message'], inplace=True)
            alertas.rename(columns={'message': 'Ack_message'}, inplace=True)

    data = pd.concat([alertas, data],
                     ignore_index=True).replace(np.nan, "")
    return data


async def process_and_filter_alerts(alertas, severities, ping_loss_message):
    alertas['Time'] = pd.to_datetime(alertas['Time'])
    alertas["Time"] = alertas['Time'].dt.strftime(
        '%d/%m/%Y %H:%M:%S')
    if severities != "":
        severities = severities.split(',')
        severities = [int(severity) for severity in severities]
    else:
        severities = [1, 2, 3, 4, 5]
    if 6 in severities:
        alertas = alertas[alertas['Problem']
                          == ping_loss_message]
    else:
        alertas = alertas[alertas['severity'].isin(
            severities)]
    return alertas


async def normalizar_eventos_cassia(data, data_problems, severities, ping_loss_message):
    """ data_problems['TimeRecovery'] = [
                '' for i in range(len(data_problems))] """
    data_problems['r_eventid'] = [
        '' for i in range(len(data_problems))]
    data_problems['Ack'] = [0 for i in range(len(data_problems))]
    """ data_problems['Ack_message'] = [
                '' for i in range(len(data_problems))] """
    data_problems['manual_close'] = [
        1 for i in range(len(data_problems))]
    data_problems['dependents'] = [
        0 for i in range(len(data_problems))]
    data_problems['local'] = [
        1 for i in range(len(data_problems))]
    data_problems['tipo'] = [
        1 for i in range(len(data_problems))]
    data_problems.drop(columns={'updated_at', 'tech_id'}, inplace=True)
    data_problems['created_at'] = pd.to_datetime(
        data_problems['created_at'])
    data_problems["created_at"] = data_problems['created_at'].dt.strftime(
        '%d/%m/%Y %H:%M:%S')
    data_problems.rename(columns={
        'created_at': 'Time',
        'closed_at': 'TimeRecovery',
        'hostname': 'Host',
        'message': 'Problem',
        'status': 'Estatus',
        'cassia_arch_traffic_events_id': 'eventid',
    }, inplace=True)

    if severities != "":
        severities = severities.split(',')
        severities = [int(severity) for severity in severities]
    else:
        severities = [1, 2, 3, 4, 5, 6]
    if 6 in severities:
        downs = data_problems[data_problems['Problem']
                              == ping_loss_message]
    data_problems = data_problems[data_problems['severity'].isin(
        severities)]
    if 6 in severities:
        data_problems = pd.concat([data_problems, downs],
                                  ignore_index=True).replace(np.nan, "")

    data = pd.concat([data_problems, data],
                     ignore_index=True).replace(np.nan, "")
    return data

""" 
async def process_open_diagnosta_events(problems, sincronizados: pd.DataFrame, ping_loss_message):
    for ind in problems.index:
        if problems['Problem'][ind] == ping_loss_message:
            dependientes = sincronizados[sincronizados['hostid_origen']
                                         == problems['hostid'][ind]]
            dependientes['depends_hostid'] = dependientes['depends_hostid'].replace(
                np.nan, 0).fillna(0)
            dependientes['depends_hostid'] = dependientes['depends_hostid'].astype(
                'int')
            dependientes = dependientes[dependientes['depends_hostid'] != 0]
            dependientes = dependientes.drop_duplicates(
                subset=['depends_hostid'])
            problems.loc[problems.index == ind,
                         'dependents'] = len(dependientes)
    return problems """


async def process_open_diagnosta_events(problems, sincronizados: pd.DataFrame, ping_loss_message):
    # Filtrar los problemas que coinciden con el mensaje de pérdida de ping
    problems_filtered = problems[problems['Problem'] == ping_loss_message]

    # Reemplazar NaN con 0 y convertir a entero
    sincronizados['depends_hostid'] = sincronizados['depends_hostid'].fillna(
        0).astype('int')

    # Eliminar duplicados para evitar cálculos repetidos
    sincronizados = sincronizados.drop_duplicates(subset=['depends_hostid'])

    # Crear un diccionario para almacenar los contadores de dependientes por hostid
    dependents_count = {}
    print(len(problems_filtered['hostid'].unique()))
    for hostid in problems_filtered['hostid'].unique():
        # Filtrar los dependientes relevantes
        dependientes = sincronizados[sincronizados['hostid_origen'] == hostid]
        dependientes = dependientes[dependientes['depends_hostid'] != 0]
        dependents_count[hostid] = len(dependientes)

    # Actualizar el DataFrame original 'problems' con los valores de dependientes
    problems['dependents'] = problems['hostid'].map(
        dependents_count).fillna(0).astype(int)

    return problems


async def get_problems_filter_backup(municipalityId, tech_host_type=0, subtype="", severities=""):
    if subtype == "0":
        subtype = ""
    if tech_host_type == "-1":
        tech_host_type = ''
    rfid_df = await CassiaConfigRepository.get_config_value_by_name('rfid_id')
    rfid_id = rfid_df['value'][0] if not rfid_df.empty else '9'
    lpr_df = await CassiaConfigRepository.get_config_value_by_name('lpr_id')
    lpr_id = lpr_df['value'][0] if not lpr_df.empty else '9'
    ping_loss_message = await CassiaConfigRepository.get_config_ping_loss_message()
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    if subtype != "" and tech_host_type == "":
        tech_host_type = "0"
    switch_df = await CassiaConfigRepository.get_config_value_by_name('switch_id')
    switch_id = switch_df['value'][0] if not switch_df.empty else '12'
    metric_switch_df = await CassiaConfigRepository.get_config_value_by_name('switch_throughtput')
    metric_switch_val = metric_switch_df['value'][
        0] if not metric_switch_df.empty else 'Interface Bridge-Aggregation_: Bits'
    if subtype == metric_switch_val:
        subtype = ""
    problems = await get_alerts(
        municipalityId, tech_host_type, subtype, severities)
    problems = await normalizar_alertas_zabbix(problems, ping_loss_message)
    if tech_host_type == lpr_id or tech_host_type == '':
        problems = await process_alerts_local(
            problems, municipalityId, lpr_id, severities, 'lpr')
    if tech_host_type == rfid_id or tech_host_type == '':
        problems = await process_alerts_local(
            problems, municipalityId, rfid_id, severities, 'rfid')
    downs_origen = await CassiaDiagnostaRepository.get_downs_origen(municipalityId, tech_host_type)
    if not downs_origen.empty:
        hostids = downs_origen['hostid'].tolist()
        hostids_str = ",".join([str(host) for host in hostids])
        data_problems = await CassiaEventRepository.get_cassia_events_with_hosts_filter(hostids_str)
        if not data_problems.empty:
            problems = await normalizar_eventos_cassia(problems, data_problems, severities, ping_loss_message)
    dependientes = await CassiaDiagnostaRepository.get_host_dependientes()
    if not dependientes.empty:
        if not problems.empty:
            indexes = problems[problems['Problem'] == ping_loss_message]
            indexes = indexes[indexes['hostid'].isin(
                dependientes['hostid'].to_list())]
            if not problems.empty:
                problems.loc[problems.index.isin(
                    indexes.index.to_list()), 'tipo'] = 0
    sincronizados = await CassiaDiagnostaRepository.get_open_problems_diagnosta()
    if not sincronizados.empty:
        if not problems.empty:
            problems = await process_open_diagnosta_events(problems, sincronizados, ping_loss_message)
    if not problems.empty:
        problems = await procesar_fechas_problemas(problems)
        problems.drop_duplicates(
            subset=['hostid', 'Problem'], inplace=True)
    if not problems.empty:

        exceptions = await get_exceptions_async(
            problems['hostid'].to_list())
        if not exceptions.empty:
            exceptions['created_at'] = pd.to_datetime(
                exceptions['created_at'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize(None)
        problems = pd.merge(problems, exceptions, on='hostid',
                            how='left').replace(np.nan, None)
    if not problems.empty:
        problems_zabbix = problems[problems['local'] == 0]
        problems_zabbix_ids = problems_zabbix['eventid']
        zabbix_eventids = '0'
        if not problems_zabbix.empty:
            zabbix_eventids = ','.join(
                problems_zabbix_ids.astype('str').to_list())
        acks_zabbix = await get_zabbix_acks(zabbix_eventids)
        if not acks_zabbix.empty:
            acks_zabbix_mensajes = acks_zabbix.groupby('eventid')[
                'message'].agg(lambda x: ' | '.join(x)).reset_index()
            acks_zabbix_mensajes.columns = [
                'eventid', 'acknowledges_concatenados']
        else:
            acks_zabbix_mensajes = pd.DataFrame(
                columns=['eventid', 'acknowledges_concatenados'])
        problems_zabbix = pd.merge(
            problems_zabbix, acks_zabbix_mensajes, how='left', on='eventid').replace(np.nan, None)
        problems_cassia = problems[problems['local'] == 1]
        problems_cassia_ids = problems_cassia['eventid']
        cassia_eventids = '0'
        if not problems_cassia.empty:
            cassia_eventids = ','.join(
                problems_cassia_ids.astype('str').to_list())
        acks_cassia = await get_cassia_acks(cassia_eventids)
        if not acks_cassia.empty:
            acks_cassia_mensajes = acks_cassia.groupby('eventid')[
                'message'].agg(lambda x: ' | '.join(x)).reset_index()
            acks_cassia_mensajes.columns = [
                'eventid', 'acknowledges_concatenados']
        else:
            acks_cassia_mensajes = pd.DataFrame(
                columns=['eventid', 'acknowledges_concatenados'])
        problems_cassia = pd.merge(
            problems_cassia, acks_cassia_mensajes, how='left', on='eventid').replace(np.nan, None)
        problems = pd.concat([problems_zabbix, problems_cassia])
        problems = problems.sort_values(by='fecha', ascending=False)
    print(len(problems))
    print(tech_host_type)
    print(type(tech_host_type))
    print(tech_host_type == '')
    return problems


async def get_problems_filter(municipalityId, tech_host_type=0, subtype="", severities=""):
    print("get_problems_filter func")
    if subtype == "0":
        subtype = ""
    if tech_host_type == "-1":
        tech_host_type = ''
    rfid_df = await CassiaConfigRepository.get_config_value_by_name('rfid_id')
    rfid_id = rfid_df['value'][0] if not rfid_df.empty else '9'
    lpr_df = await CassiaConfigRepository.get_config_value_by_name('lpr_id')
    lpr_id = lpr_df['value'][0] if not lpr_df.empty else '9'
    ping_loss_message = await CassiaConfigRepository.get_config_ping_loss_message()

    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    if subtype != "" and tech_host_type == "":
        tech_host_type = "0"

    switch_df = await CassiaConfigRepository.get_config_value_by_name('switch_id')

    switch_id = switch_df['value'][0] if not switch_df.empty else '12'
    metric_switch_df = await CassiaConfigRepository.get_config_value_by_name('switch_throughtput')

    metric_switch_val = metric_switch_df['value'][
        0] if not metric_switch_df.empty else 'Interface Bridge-Aggregation_: Bits'
    if subtype == metric_switch_val:
        subtype = ""

    problems = await get_alerts(
        municipalityId, tech_host_type, subtype, severities)

    problems = await normalizar_alertas_zabbix(problems, ping_loss_message)

    problems = await eliminar_downs_zabbix(problems, ping_loss_message)
    if tech_host_type == lpr_id or tech_host_type == '':
        problems = await process_alerts_local(
            problems, municipalityId, lpr_id, severities, 'lpr', ping_loss_message)
    if tech_host_type == rfid_id or tech_host_type == '':
        problems = await process_alerts_local(
            problems, municipalityId, rfid_id, severities, 'rfid', ping_loss_message)

    entro1 = False
    entro2 = False

    if tech_host_type == '' and subtype == '':

        entro1 = True
        problems = await get_cassia_events(problems, municipalityId, severities, ping_loss_message)

    if tech_host_type != '' and subtype == '':

        entro2 = True
        problems = await get_cassia_events_by_tech_id(problems, municipalityId, tech_host_type, severities,
                                                      ping_loss_message)

    downs_origen = await CassiaDiagnostaRepository.get_downs_origen(municipalityId, tech_host_type)

    if not downs_origen.empty:
        hostids = downs_origen['hostid'].tolist()
        hostids_str = ",".join([str(host) for host in hostids])
        data_problems = await CassiaEventRepository.get_cassia_events_with_hosts_filter(hostids_str)
        if not data_problems.empty:
            problems = await normalizar_eventos_cassia(problems, data_problems, severities, ping_loss_message)

    dependientes = await CassiaDiagnostaRepository.get_host_dependientes()

    if not dependientes.empty:
        if not problems.empty:
            indexes = problems[problems['Problem'] == ping_loss_message]
            indexes = indexes[indexes['hostid'].isin(
                dependientes['hostid'].to_list())]
            if not problems.empty:
                problems.loc[problems.index.isin(
                    indexes.index.to_list()), 'tipo'] = 0
                problems.loc[~problems.index.isin(
                    indexes.index.to_list()), 'tipo'] = 1

    sincronizados = await CassiaDiagnostaRepository.get_open_problems_diagnosta()

    if not sincronizados.empty:
        if not problems.empty:
            problems = await process_open_diagnosta_events(problems, sincronizados, ping_loss_message)
    if not problems.empty:
        problems = await procesar_fechas_problemas(problems)
        problems.drop_duplicates(
            subset=['hostid', 'Problem'], inplace=True)
    if not problems.empty:

        exceptions = await get_exceptions_async(
            problems['hostid'].to_list())
        if not exceptions.empty:
            exceptions['created_at'] = pd.to_datetime(
                exceptions['created_at'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize(None)
        problems = pd.merge(problems, exceptions, on='hostid',
                            how='left').replace(np.nan, None)
    if not problems.empty:
        problems_zabbix = problems[problems['local'] == 0]
        problems_zabbix_ids = problems_zabbix['eventid']
        zabbix_eventids = '0'
        if not problems_zabbix.empty:
            zabbix_eventids = ','.join(
                problems_zabbix_ids.astype('str').to_list())
        # AQUI
        acks_zabbix = await get_zabbix_acks(zabbix_eventids)
        if not acks_zabbix.empty:
            acks_zabbix_mensajes = acks_zabbix.groupby('eventid')[
                'message'].agg(lambda x: ' | '.join(x)).reset_index()
            acks_zabbix_mensajes.columns = [
                'eventid', 'acknowledges_concatenados']
        else:
            acks_zabbix_mensajes = pd.DataFrame(
                columns=['eventid', 'acknowledges_concatenados'])
        problems_zabbix = pd.merge(
            problems_zabbix, acks_zabbix_mensajes, how='left', on='eventid').replace(np.nan, None)
        problems_cassia = problems[problems['local'] == 1]
        problems_cassia_ids = problems_cassia['eventid']
        cassia_eventids = '0'
        if not problems_cassia.empty:
            cassia_eventids = ','.join(
                problems_cassia_ids.astype('str').to_list())

        acks_cassia = await get_cassia_acks(cassia_eventids)
        if not acks_cassia.empty:
            acks_cassia_mensajes = acks_cassia.groupby('eventid')[
                'message'].agg(lambda x: ' | '.join(x)).reset_index()
            acks_cassia_mensajes.columns = [
                'eventid', 'acknowledges_concatenados']
        else:
            acks_cassia_mensajes = pd.DataFrame(
                columns=['eventid', 'acknowledges_concatenados'])
        problems_cassia = pd.merge(
            problems_cassia, acks_cassia_mensajes, how='left', on='eventid').replace(np.nan, None)
        problems = pd.concat([problems_zabbix, problems_cassia])
        problems = problems.sort_values(by='fecha', ascending=False)
    print(problems)

    if not problems.empty:
        affiliations_df = await CassiaResetRepository.get_affiliations_by_hosts_ids(problems['hostid'].tolist())
        if not affiliations_df.empty:
            # Realizar el merge para agregar las columnas de affiliations_df a problems
            problems = pd.merge(problems, affiliations_df,
                                on='hostid', how='left').replace(np.nan, None)
    hostids_str_2 = ",".join(
        [str(hostid) for hostid in problems['hostid'].tolist()]) if not problems.empty else "0"
    serial_numbers_df = await cassia_gs_tickets_repository.get_serial_numbers_by_host_ids(hostids_str_2)
    if not serial_numbers_df.empty:
        # Realizar el merge para agregar las columnas de serial_numbers_df a problems
        problems = pd.merge(problems, serial_numbers_df,
                            on='hostid', how='left').replace(np.nan, None)
    problems['create_ticket'] = 0
    suscriptores_id = await CassiaConfigRepository.get_config_value_by_name('suscriptores_id')
    suscriptores_id = suscriptores_id['value'][0] if not suscriptores_id.empty else 11
    if all(col in problems.columns for col in ['affiliation', 'no_serie']):
        problems['create_ticket'] = problems.apply(
            lambda row: assign_value_bck(row), axis=1)

        print("Ambas columnas existen en el DataFrame.")
    last_error_tickets = await cassia_gs_tickets_repository.get_last_error_tickets()
    problems['has_ticket'] = False
    problems['ticket_id'] = None
    problems['ticket_error'] = None
    problems['ticket_status'] = None
    if not problems.empty:
        for index in last_error_tickets.index:
            problems.loc[(problems['affiliation'] == last_error_tickets['afiliacion'][index]) & (
                problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_error'] = last_error_tickets['error'][index]
            problems.loc[(problems['affiliation'] == last_error_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_status'] = 'error'
        active_tickets = await cassia_gs_tickets_repository.get_active_tickets()
        for index in active_tickets.index:
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'has_ticket'] = True
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_id'] = active_tickets['ticket_id'][index]
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_error'] = None
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_status'] = active_tickets['status'][index]
    if not problems.empty:
        problems = problems.drop_duplicates(subset=['eventid', 'local'])
    print("BANDERAS************************************")
    """ print(entro1)
    print(entro2)
    print(ping_loss_message) """
    hast = problems[problems['has_ticket'] == True]
    print(hast)
    return problems


def assign_value(row, suscriptores_id):
    if (row['affiliation'] is not None and row['affiliation'] != '') and (row['no_serie'] is not None and row['no_serie'] != '') and (str(row['tech_id']) == str(suscriptores_id)):
        return True
    else:
        return False


def assign_value_bck(row):
    if (row['affiliation'] is not None and row['affiliation'] != '') and (row['no_serie'] is not None and row['no_serie'] != ''):
        return True
    else:
        return False


async def get_zabbix_acks_pool(eventids, db) -> pd.DataFrame:

    try:
        query_get_zabbix_acks = DBQueries(
        ).builder_query_statement_get_zabbix_acks(eventids)

        zabbix_alerts_acks_data = await db.run_query(query_get_zabbix_acks)
        zabbix_alerts_acks_df = pd.DataFrame(
            zabbix_alerts_acks_data).replace(np.nan, None)
        return zabbix_alerts_acks_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_zabbix_acks {e}")


async def get_zabbix_acks(eventids) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_zabbix_acks = DBQueries(
        ).builder_query_statement_get_zabbix_acks(eventids)
        await db_model.start_connection()
        zabbix_alerts_acks_data = await db_model.run_query(query_get_zabbix_acks)
        zabbix_alerts_acks_df = pd.DataFrame(
            zabbix_alerts_acks_data).replace(np.nan, None)
        return zabbix_alerts_acks_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_zabbix_acks {e}")
    finally:
        await db_model.close_connection()


async def get_cassia_acks_pool(eventids, db) -> pd.DataFrame:

    try:
        # PINK
        query_get_cassia_acks = DBQueries(
        ).builder_query_statement_get_cassia_acks_test(eventids)

        cassia_alerts_acks_data = await db.run_query(query_get_cassia_acks)
        cassia_alerts_acks_df = pd.DataFrame(
            cassia_alerts_acks_data).replace(np.nan, None)
        return cassia_alerts_acks_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_acks {e}")


async def get_cassia_acks(eventids) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        query_get_cassia_acks = DBQueries(
        ).builder_query_statement_get_cassia_acks_test(eventids)
        await db_model.start_connection()
        cassia_alerts_acks_data = await db_model.run_query(query_get_cassia_acks)
        cassia_alerts_acks_df = pd.DataFrame(
            cassia_alerts_acks_data).replace(np.nan, None)
        return cassia_alerts_acks_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_acks {e}")
    finally:
        await db_model.close_connection()


async def get_downs_without_municipality() -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        get_hosts_events_withou_municipality = DBQueries(
        ).query_statement_get_hosts_events_withou_municipality
        await db_model.start_connection()
        hosts_data = await db_model.run_query(get_hosts_events_withou_municipality)
        hosts_df = pd.DataFrame(
            hosts_data).replace(np.nan, None)
        return hosts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_downs_without_municipality {e}")
    finally:
        await db_model.close_connection()


async def get_cassia_events_pool(data, municipalityId,  severities, ping_loss_message, db):
    if municipalityId == '0':
        print("AAAAAA")
        # PINK

        alertas = await CassiaEventRepository.get_global_alerts_local_pool(db)
        print("BBBBB")
        if not alertas.empty:
            print(alertas)
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
            print("CCCCC")
            print(alertas)
    else:
        municipios = await CassiaConfigRepository.get_city_catalog_pool(db)
        municipio_value = ''
        if not municipios.empty:
            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio_value = municipio['name'].item()
        alertas = await CassiaEventRepository.get_municipality_alerts_local_pool(municipio_value, db)

        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
    if not alertas.empty:

        acks = await CassiaEventRepository.get_cassia_events_acknowledges_pool(db)
        if not acks.empty:
            alertas = pd.merge(alertas, acks, left_on='eventid',
                               right_on='eventid', how='left')
            alertas.drop(columns=['Ack_message'], inplace=True)
            alertas.rename(columns={'message': 'Ack_message'}, inplace=True)

    data = pd.concat([alertas, data],
                     ignore_index=True).replace(np.nan, "")
    return data


async def get_cassia_events(data, municipalityId,  severities, ping_loss_message):
    if municipalityId == '0':
        print("AAAAAA")
        # PINK
        alertas = await CassiaEventRepository.get_global_alerts_local()

        print("BBBBB")
        if not alertas.empty:
            print(alertas)
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
            print("CCCCC")
            print(alertas)
    else:
        municipios = await CassiaConfigRepository.get_city_catalog()
        municipio_value = ''
        if not municipios.empty:
            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio_value = municipio['name'].item()
        alertas = await CassiaEventRepository.get_municipality_alerts_local(municipio_value)

        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
    if not alertas.empty:

        acks = await CassiaEventRepository.get_cassia_events_acknowledges()
        if not acks.empty:
            alertas = pd.merge(alertas, acks, left_on='eventid',
                               right_on='eventid', how='left')
            alertas.drop(columns=['Ack_message'], inplace=True)
            alertas.rename(columns={'message': 'Ack_message'}, inplace=True)

    data = pd.concat([alertas, data],
                     ignore_index=True).replace(np.nan, "")
    return data


async def get_cassia_events_by_tech_id_pool(data, municipalityId, tech_host_id, severities, ping_loss_message, db):
    if municipalityId == '0':
        print("AAAAAA")
        # PINK
        alertas = await CassiaEventRepository.get_global_alerts_local_by_tech_id_pool(tech_host_id, db)
        print("BBBBB")
        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
            print("CCCCC")
    else:
        municipios = await CassiaConfigRepository.get_city_catalog_pool(db)
        municipio_value = ''
        if not municipios.empty:
            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio_value = municipio['name'].item()
        alertas = await CassiaEventRepository.get_municipality_alerts_by_tech_pool(municipio_value, tech_host_id,
                                                                                   ping_loss_message, db)

        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
    if not alertas.empty:

        acks = await CassiaEventRepository.get_cassia_events_acknowledges_pool(db)
        if not acks.empty:
            alertas = pd.merge(alertas, acks, left_on='eventid',
                               right_on='eventid', how='left')
            alertas.drop(columns=['Ack_message'], inplace=True)
            alertas.rename(columns={'message': 'Ack_message'}, inplace=True)

    data = pd.concat([alertas, data],
                     ignore_index=True).replace(np.nan, "")
    return data


async def get_cassia_events_by_tech_id(data, municipalityId, tech_host_id, severities, ping_loss_message):
    if municipalityId == '0':
        print("AAAAAA")
        # PINK
        alertas = await CassiaEventRepository.get_global_alerts_local_by_tech_id(tech_host_id)
        print("BBBBB")
        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
            print("CCCCC")
    else:
        municipios = await CassiaConfigRepository.get_city_catalog()
        municipio_value = ''
        if not municipios.empty:
            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio_value = municipio['name'].item()
        alertas = await CassiaEventRepository.get_municipality_alerts_by_tech(municipio_value, tech_host_id,
                                                                              ping_loss_message)

        if not alertas.empty:
            alertas = await process_and_filter_alerts(alertas, severities, ping_loss_message)
    if not alertas.empty:

        acks = await CassiaEventRepository.get_cassia_events_acknowledges()
        if not acks.empty:
            alertas = pd.merge(alertas, acks, left_on='eventid',
                               right_on='eventid', how='left')
            alertas.drop(columns=['Ack_message'], inplace=True)
            alertas.rename(columns={'message': 'Ack_message'}, inplace=True)

    data = pd.concat([alertas, data],
                     ignore_index=True).replace(np.nan, "")
    return data


async def get_problems_filter_pool_backup(municipalityId, tech_host_type=0, subtype="", severities="", db=None, marcas=[]):

    if subtype == "0":
        subtype = ""
    if tech_host_type == "-1":
        tech_host_type = ''
    init = time.time()
    rfid_df = await CassiaConfigRepository.get_config_value_by_name_pool('rfid_id', db)
    marcas.append({"problems.rfid_df": time.time()-init})
    init = time.time()
    rfid_id = rfid_df['value'][0] if not rfid_df.empty else '9'
    lpr_df = await CassiaConfigRepository.get_config_value_by_name_pool('lpr_id', db)
    marcas.append({"problems.lpr_df": time.time()-init})
    init = time.time()
    lpr_id = lpr_df['value'][0] if not lpr_df.empty else '9'
    ping_loss_message = await CassiaConfigRepository.get_config_ping_loss_message_pool(db)
    print("******************PING LOSS MESSAGE**************")
    marcas.append({"problems.ping_loss_df": time.time()-init})
    init = time.time()
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    if subtype != "" and tech_host_type == "":
        tech_host_type = "0"

    switch_df = await CassiaConfigRepository.get_config_value_by_name_pool('switch_id', db)
    marcas.append({"problems.switch_df": time.time()-init})
    init = time.time()

    switch_id = switch_df['value'][0] if not switch_df.empty else '12'
    metric_switch_df = await CassiaConfigRepository.get_config_value_by_name_pool('switch_throughtput', db)
    marcas.append({"problems.metric_swtich_df": time.time()-init})
    init = time.time()

    metric_switch_val = metric_switch_df['value'][
        0] if not metric_switch_df.empty else 'Interface Bridge-Aggregation_: Bits'
    if subtype == metric_switch_val:
        subtype = ""

    problems = await get_alerts_pool(
        municipalityId, tech_host_type, subtype, severities, db)
    marcas.append({"problems.get_zabbix_alerts": time.time()-init})
    init = time.time()

    problems = await normalizar_alertas_zabbix(problems, ping_loss_message)

    problems = await eliminar_downs_zabbix(problems, ping_loss_message)
    if tech_host_type == lpr_id or tech_host_type == '':
        problems = await process_alerts_local_pool(
            problems, municipalityId, lpr_id, severities, 'lpr', ping_loss_message, db)
        marcas.append({"problems.alerts_local_lpr": time.time()-init})
    init = time.time()

    if tech_host_type == rfid_id or tech_host_type == '':
        problems = await process_alerts_local_pool(
            problems, municipalityId, rfid_id, severities, 'rfid', ping_loss_message, db)
        marcas.append({"problems.alerts_local_rfid": time.time()-init})
    init = time.time()

    entro1 = False
    entro2 = False

    if tech_host_type == '' and subtype == '':

        entro1 = True
        problems = await get_cassia_events_pool(problems, municipalityId, severities, ping_loss_message, db)
        marcas.append({"problems.get_cassia_events": time.time()-init})
    init = time.time()

    if tech_host_type != '' and subtype == '':

        entro2 = True
        problems = await get_cassia_events_by_tech_id_pool(problems, municipalityId, tech_host_type, severities,
                                                           ping_loss_message, db)
        marcas.append({"problems.get_cassia_events_by_tech": time.time()-init})
    init = time.time()

    downs_origen = await CassiaDiagnostaRepository.get_downs_origen_pool(municipalityId, tech_host_type, db)
    marcas.append({"problems.downs_origen": time.time()-init})
    init = time.time()

    if not downs_origen.empty:
        hostids = downs_origen['hostid'].tolist()
        hostids_str = ",".join([str(host) for host in hostids])
        data_problems = await CassiaEventRepository.get_cassia_events_with_hosts_filter_pool(hostids_str, db)
        marcas.append(
            {"problems.cassia_events_with_host_filter": time.time()-init})
        init = time.time()

        if not data_problems.empty:
            problems = await normalizar_eventos_cassia(problems, data_problems, severities, ping_loss_message)
    print("******************BANDERAS**************")
    print(entro1)
    print(entro2)
    dependientes = await CassiaDiagnostaRepository.get_host_dependientes_pool(db)
    marcas.append({"problems.get_dependientes": time.time()-init})
    init = time.time()

    if not dependientes.empty:
        print("******************ENTRA A DEPENDIENTES**************")
        if not problems.empty:
            print("******************ENTRA A PROBLEMS**************")
            indexes = problems[problems['Problem'] == ping_loss_message]
            print("******************INDEXES**************")
            print(indexes)
            indexes = indexes[indexes['hostid'].isin(
                dependientes['hostid'].to_list())]
            print("******************INDEXES 2**************")
            print(indexes)

            if not problems.empty:
                print("******************AQUI**************")
                problems.loc[problems.index.isin(
                    indexes.index.to_list()), 'tipo'] = 0

                problems.loc[~problems.index.isin(
                    indexes.index.to_list()), 'tipo'] = 1
                print(problems[problems['tipo'] == 0])
                print(problems[problems['tipo'] == 1])

    sincronizados = await CassiaDiagnostaRepository.get_open_problems_diagnosta_pool(db)
    marcas.append({"problems.get_sincronizados": time.time()-init})
    init = time.time()

    if not sincronizados.empty:
        if not problems.empty:
            problems = await process_open_diagnosta_events(problems, sincronizados, ping_loss_message)
            marcas.append(
                {"problems.process_open_diagnosta_events": time.time()-init})
            init = time.time()
    if not problems.empty:
        problems = await procesar_fechas_problemas(problems)
        marcas.append({"problems.procesar_fechas_problemas": time.time()-init})
        init = time.time()
        problems.drop_duplicates(
            subset=['hostid', 'Problem'], inplace=True)
    if not problems.empty:

        exceptions = await get_exceptions_async_pool(
            problems['hostid'].to_list(), db)
        marcas.append({"problems.get_exceptions_async": time.time()-init})
        init = time.time()
        if not exceptions.empty:
            exceptions['created_at'] = pd.to_datetime(
                exceptions['created_at'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize(None)
        problems = pd.merge(problems, exceptions, on='hostid',
                            how='left').replace(np.nan, None)
    if not problems.empty:
        problems_zabbix = problems[problems['local'] == 0]
        problems_zabbix_ids = problems_zabbix['eventid']
        zabbix_eventids = '0'
        if not problems_zabbix.empty:
            zabbix_eventids = ','.join(
                problems_zabbix_ids.astype('str').to_list())
        # AQUI
        acks_zabbix = await get_zabbix_acks_pool(zabbix_eventids, db)
        marcas.append({"problems.get_zabbix_acks": time.time()-init})
        init = time.time()
        if not acks_zabbix.empty:
            acks_zabbix_mensajes = acks_zabbix.groupby('eventid')[
                'message'].agg(lambda x: ' | '.join(x)).reset_index()
            acks_zabbix_mensajes.columns = [
                'eventid', 'acknowledges_concatenados']
        else:
            acks_zabbix_mensajes = pd.DataFrame(
                columns=['eventid', 'acknowledges_concatenados'])
        problems_zabbix = pd.merge(
            problems_zabbix, acks_zabbix_mensajes, how='left', on='eventid').replace(np.nan, None)
        problems_cassia = problems[problems['local'] == 1]
        problems_cassia_ids = problems_cassia['eventid']
        cassia_eventids = '0'
        if not problems_cassia.empty:
            cassia_eventids = ','.join(
                problems_cassia_ids.astype('str').to_list())

        acks_cassia = await get_cassia_acks_pool(cassia_eventids, db)
        marcas.append({"problems.get_cassia_acks": time.time()-init})
        init = time.time()

        if not acks_cassia.empty:
            acks_cassia_mensajes = acks_cassia.groupby('eventid')[
                'message'].agg(lambda x: ' | '.join(x)).reset_index()
            acks_cassia_mensajes.columns = [
                'eventid', 'acknowledges_concatenados']
        else:
            acks_cassia_mensajes = pd.DataFrame(
                columns=['eventid', 'acknowledges_concatenados'])
        problems_cassia = pd.merge(
            problems_cassia, acks_cassia_mensajes, how='left', on='eventid').replace(np.nan, None)
        problems = pd.concat([problems_zabbix, problems_cassia])
        problems = problems.sort_values(by='fecha', ascending=False)

    if not problems.empty:
        affiliations_df = await CassiaResetRepository.get_affiliations_by_hosts_ids_pool(problems['hostid'].tolist(), db)
        marcas.append({"problems.get_affiliations": time.time()-init})
        init = time.time()
        if not affiliations_df.empty:
            # Realizar el merge para agregar las columnas de affiliations_df a problems
            problems = pd.merge(problems, affiliations_df,
                                on='hostid', how='left').replace(np.nan, None)
    hostids_str_2 = ",".join(
        [str(hostid) for hostid in problems['hostid'].tolist()]) if not problems.empty else "0"
    serial_numbers_df = await cassia_gs_tickets_repository.get_serial_numbers_by_host_ids_pool(hostids_str_2, db)
    marcas.append({"problems.get_serial_numbers": time.time()-init})
    init = time.time()
    if not serial_numbers_df.empty:
        # Realizar el merge para agregar las columnas de serial_numbers_df a problems
        problems = pd.merge(problems, serial_numbers_df,
                            on='hostid', how='left').replace(np.nan, None)
    problems['create_ticket'] = 0
    suscriptores_id = await CassiaConfigRepository.get_config_value_by_name_pool('suscriptores_id', db)
    suscriptores_id = suscriptores_id['value'][0] if not suscriptores_id.empty else 11
    if all(col in problems.columns for col in ['affiliation', 'no_serie']):
        problems['create_ticket'] = problems.apply(
            lambda row: assign_value(row, suscriptores_id), axis=1)

        print("Ambas columnas existen en el DataFrame.")
    last_error_tickets = await cassia_gs_tickets_repository.get_last_error_tickets_pool(db)
    marcas.append({"problems.get_last_error_tickets": time.time()-init})
    init = time.time()
    problems['has_ticket'] = False
    problems['ticket_id'] = None
    problems['ticket_error'] = None
    problems['ticket_status'] = None
    if not problems.empty:
        for index in last_error_tickets.index:
            problems.loc[(problems['affiliation'] == last_error_tickets['afiliacion'][index]) & (
                problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_error'] = last_error_tickets['error'][index]
            problems.loc[(problems['affiliation'] == last_error_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_status'] = 'error'
        active_tickets = await cassia_gs_tickets_repository.get_active_tickets()
        for index in active_tickets.index:
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'has_ticket'] = True
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_id'] = active_tickets['ticket_id'][index]
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_error'] = None
            problems.loc[(problems['affiliation'] == active_tickets['afiliacion']
                         [index]) & (problems['tech_id'].astype(str) == str(suscriptores_id)), 'ticket_status'] = active_tickets['status'][index]
    if not problems.empty:
        problems = problems.drop_duplicates(subset=['eventid', 'local'])
    """ print("BANDERAS************************************")
    print(entro1)
    print(entro2)
    print(ping_loss_message)
    hast = problems[problems['create_ticket'] == True]
    print(hast) """
    return problems


async def get_problems_filter_pool(municipalityId, tech_host_type=0, subtype="", severities="", db=None, marcas=[]):
    init = time.time()
    tasks = {
        'rfid_id_df': asyncio.create_task(CassiaConfigRepository.get_config_value_by_name_pool('rfid_id', db)),
        'lpr_df': asyncio.create_task(CassiaConfigRepository.get_config_value_by_name_pool('lpr_id', db)),
        'ping_loss_message_df': asyncio.create_task(CassiaConfigRepository.get_config_ping_loss_message_pool(db)),
        'switch_df': asyncio.create_task(CassiaConfigRepository.get_config_value_by_name_pool('switch_id', db)),
        'metric_switch_df': asyncio.create_task(CassiaConfigRepository.get_config_value_by_name_pool('switch_throughtput', db)),
        'problems_df': asyncio.create_task(get_alerts_pool(municipalityId, tech_host_type, subtype, severities, db)),
        'downs_origen_df': asyncio.create_task(CassiaDiagnostaRepository.get_downs_origen_pool(municipalityId, tech_host_type, db)),
        'dependientes_df': asyncio.create_task(CassiaDiagnostaRepository.get_host_dependientes_pool(db)),
        'sincronizados_df': asyncio.create_task(CassiaDiagnostaRepository.get_open_problems_diagnosta_pool(db)),
        'suscriptores_id_df': asyncio.create_task(CassiaConfigRepository.get_config_value_by_name_pool('suscriptores_id', db)),
        'last_errors_tickets_df': asyncio.create_task(cassia_gs_tickets_repository.get_last_error_tickets_pool(db)),
        'active_tickets_df': asyncio.create_task(cassia_gs_tickets_repository.get_active_tickets())
    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    marcas.append({"problems.gather": time.time()-init})
    if subtype == "0":
        subtype = ""
    if tech_host_type == "-1":
        tech_host_type = ''
    init = time.time()
    # A
    rfid_df = dfs['rfid_id_df']
    marcas.append({"problems.rfid_df": time.time()-init})
    init = time.time()
    rfid_id = rfid_df['value'][0] if not rfid_df.empty else '9'
    # A
    lpr_df = dfs['lpr_df']
    marcas.append({"problems.lpr_df": time.time()-init})
    init = time.time()
    lpr_id = lpr_df['value'][0] if not lpr_df.empty else '9'
    # A
    ping_loss_message = dfs['ping_loss_message_df']
    print("******************PING LOSS MESSAGE**************")
    marcas.append({"problems.ping_loss_df": time.time()-init})
    init = time.time()
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    if subtype != "" and tech_host_type == "":
        tech_host_type = "0"

    # A
    switch_df = dfs['switch_df']
    marcas.append({"problems.switch_df": time.time()-init})
    init = time.time()

    switch_id = switch_df['value'][0] if not switch_df.empty else '12'
    # A
    metric_switch_df = dfs['metric_switch_df']
    marcas.append({"problems.metric_swtich_df": time.time()-init})
    init = time.time()

    metric_switch_val = metric_switch_df['value'][
        0] if not metric_switch_df.empty else 'Interface Bridge-Aggregation_: Bits'
    if subtype == metric_switch_val:
        subtype = ""

    problems = dfs['problems_df']  # A
    marcas.append({"problems.get_zabbix_alerts": time.time()-init})
    init = time.time()

    problems = await normalizar_alertas_zabbix(problems, ping_loss_message)

    problems = await eliminar_downs_zabbix(problems, ping_loss_message)
    if tech_host_type in [lpr_id, '']:
        problems = await process_alerts_local_pool(
            problems, municipalityId, lpr_id, severities, 'lpr', ping_loss_message, db)
        marcas.append({"problems.alerts_local_lpr": time.time()-init})
    init = time.time()

    if tech_host_type in [rfid_id, '']:
        problems = await process_alerts_local_pool(
            problems, municipalityId, rfid_id, severities, 'rfid', ping_loss_message, db)
        marcas.append({"problems.alerts_local_rfid": time.time()-init})
    init = time.time()
    entro1 = False
    entro2 = False

    if tech_host_type == '' and subtype == '':

        entro1 = True
        problems = await get_cassia_events_pool(problems, municipalityId, severities, ping_loss_message, db)
        marcas.append({"problems.get_cassia_events": time.time()-init})
    init = time.time()

    if tech_host_type != '':

        entro2 = True
        problems = await get_cassia_events_by_tech_id_pool(problems, municipalityId, tech_host_type, severities,
                                                           ping_loss_message, db)
        marcas.append({"problems.get_cassia_events_by_tech": time.time()-init})
        print("********************************ACAAAAAAAA****************************************")
        print(problems)
        print(problems.columns)
    init = time.time()

    downs_origen = dfs['downs_origen_df']
    print("*******************ORIGENES")
    print(downs_origen)
    marcas.append({"problems.downs_origen": time.time()-init})
    init = time.time()

    if not downs_origen.empty:

        hostids_str = ",".join(map(str, downs_origen['hostid'].tolist()))
        data_problems = await CassiaEventRepository.get_cassia_events_with_hosts_filter_pool(hostids_str, db)
        if not data_problems.empty:
            problems = await normalizar_eventos_cassia(problems, data_problems, severities, ping_loss_message)
    print("******************BANDERAS**************")
    print(entro1)
    print(entro2)
    dependientes = dfs['dependientes_df']
    if dependientes.empty:
        dependientes = pd.DataFrame(columns=['hostid'])
    marcas.append({"problems.get_dependientes": time.time()-init})
    init = time.time()

    if 'hostid' in dependientes.columns:
        print("******************ENTRA A DEPENDIENTES**************")
        if not problems.empty:
            dependientes_hostids = dependientes['hostid'].tolist()
            index_dependientes = problems[(
                problems['Problem'] == ping_loss_message) & problems['hostid'].isin(dependientes_hostids)].index
            problems.loc[index_dependientes, 'tipo'] = 0
            problems.loc[~problems.index.isin(index_dependientes), 'tipo'] = 1

    sincronizados = dfs['sincronizados_df']
    marcas.append({"problems.get_sincronizados": time.time()-init})
    init = time.time()

    if not sincronizados.empty and not problems.empty:
        problems = await process_open_diagnosta_events(problems, sincronizados, ping_loss_message)

    if not problems.empty:
        problems = await procesar_fechas_problemas(problems)
        marcas.append({"problems.procesar_fechas_problemas": time.time()-init})
        init = time.time()
        problems.drop_duplicates(
            subset=['hostid', 'Problem'], inplace=True)
    # Procesar excepciones
    exceptions = await get_exceptions_async_pool(problems['hostid'].tolist(), db) if not problems.empty else pd.DataFrame()

    if not exceptions.empty:
        exceptions['created_at'] = pd.to_datetime(
            exceptions['created_at'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize(None)
        problems = pd.merge(problems, exceptions, on='hostid',
                            how='left').replace(np.nan, None)
        problems['acknowledges_concatenados'] = problems['exception_message'].fillna(
            '')

    else:
        problems['acknowledges_concatenados'] = None

    if not problems.empty:

        problems_zabbix = problems[problems['local'] == 0]
        problems_cassia = problems[problems['local'] == 1]

        # Obtener acknowledgements
        zabbix_eventids = ','.join(problems_zabbix['eventid'].astype(
            str).tolist()) if not problems_zabbix.empty else '0'
        cassia_eventids = ','.join(problems_cassia['eventid'].astype(
            str).tolist()) if not problems_cassia.empty else '0'
        task_2 = {
            'acks_zabbix_df': get_zabbix_acks_pool(zabbix_eventids, db),
            'acks_cassia_df': get_cassia_acks_pool(cassia_eventids, db)
        }
        results2 = await asyncio.gather(*task_2.values())
        acks_zabbix = results2[0]
        acks_cassia = results2[1]

        # Concatenar acknowledges
        if not acks_zabbix.empty:
            acks_zabbix_mensajes = acks_zabbix.groupby(
                'eventid')['message'].agg(' | '.join).reset_index()
            problems_zabbix = pd.merge(
                problems_zabbix, acks_zabbix_mensajes, on='eventid', how='left')

        if not acks_cassia.empty:
            acks_cassia_mensajes = acks_cassia.groupby(
                'eventid')['message'].agg(' | '.join).reset_index()
            problems_cassia = pd.merge(
                problems_cassia, acks_cassia_mensajes, on='eventid', how='left')

        # Concatenar problemas zabbix y cassia
        problems = pd.concat([problems_zabbix, problems_cassia]).sort_values(
            by='fecha', ascending=False)

        if 'message' in problems.columns:

            problems['acknowledges_concatenados'] = problems['acknowledges_concatenados'].fillna(
                '')
            problems['message'] = problems['message'].fillna('')

            problems['acknowledges_concatenados'] = problems['acknowledges_concatenados'] + \
                problems['message']

            problems['message'] = problems['acknowledges_concatenados'].replace(
                '', None)

    if 'acknowledges_concatenados' in problems.columns:

        problems['acknowledges_concatenados'] = problems['acknowledges_concatenados'].replace(
            '', None)

    # Procesar afiliaciones y números de serie
    if not problems.empty:
        hostids_str_2 = ','.join(
            map(str, problems['hostid'].tolist())) if not problems.empty else '0'
        task_3 = {
            'affiliations_df': CassiaResetRepository.get_affiliations_by_hosts_ids_pool(problems['hostid'].tolist(), db),
            'serial_numbers_df': cassia_gs_tickets_repository.get_serial_numbers_by_host_ids_pool(hostids_str_2, db)
        }
        results3 = await asyncio.gather(*task_3.values())
        affiliations_df = results3[0]
        serial_numbers_df = results3[1]

        if not affiliations_df.empty:
            problems = pd.merge(problems, affiliations_df,
                                on='hostid', how='left').replace(np.nan, None)

        if not serial_numbers_df.empty:
            problems = pd.merge(problems, serial_numbers_df,
                                on='hostid', how='left').replace(np.nan, None)

    problems['create_ticket'] = 0
    suscriptores_id = dfs['suscriptores_id_df']
    suscriptores_id = suscriptores_id['value'][0] if not suscriptores_id.empty else 11
    print(problems)
    print(problems.columns)
    if all(col in problems.columns for col in ['affiliation', 'no_serie']):
        problems['create_ticket'] = problems.apply(
            lambda row: assign_value(row, suscriptores_id), axis=1)

        print("Ambas columnas existen en el DataFrame.")
    last_error_tickets = dfs['last_errors_tickets_df']
    marcas.append({"problems.get_last_error_tickets": time.time()-init})
    init = time.time()
    problems['has_ticket'] = False
    problems['ticket_id'] = None
    problems['ticket_error'] = None
    problems['ticket_status'] = None
    if not 'tech_id' in problems.columns and not problems.empty:
        problems['tech_id'] = tech_host_type
    if not problems.empty:
        problems['tech_id'] = problems['tech_id'].astype(str)
        suscriptores_id_str = str(suscriptores_id)
        if not last_error_tickets.empty:
            # Crear una máscara para las filas que cumplen con las condiciones
            mask = (
                (problems['affiliation'].isin(last_error_tickets['afiliacion'])) &
                (problems['tech_id'] == suscriptores_id_str)
            )
            # Actualizar las columnas 'ticket_error' y 'ticket_status' basadas en la máscara
            problems.loc[mask, 'ticket_error'] = last_error_tickets.set_index(
                'afiliacion').loc[problems.loc[mask, 'affiliation'], 'error'].values
            problems.loc[mask, 'ticket_status'] = 'error'

        active_tickets = dfs['active_tickets_df']
        if not active_tickets.empty:
            active_tickets['ticket_active_id'] = pd.to_numeric(active_tickets['ticket_active_id'], errors='coerce').astype(
                'Int64')
            print("****************ACTIVE TICKETS")
            print(active_tickets)
            # Asegúrate de que ambos DataFrames tengan una columna en común para poder unirlos, por ejemplo, 'affiliation'.
            merged_df = problems.merge(active_tickets[['afiliacion', 'created_at_ticket', 'ticket_active_status', 'ticket_active_id']],
                                       left_on='affiliation',
                                       right_on='afiliacion',
                                       how='left')
            merged_df['Time_event'] = pd.to_datetime(
                merged_df['Time'], errors='coerce')
            merged_df['created_at_ticket'] = pd.to_datetime(
                merged_df['created_at_ticket'], errors='coerce')

            """ print("*********************TIPOS*******************")
            print(merged_df[['created_at_ticket']].dtypes)
            print("*********************TIPOS*******************")
            print(merged_df[['Time']].dtypes) """
            # Ahora puedes hacer la comparación ya que ambas columnas ('Time' y 'created_at') están en el mismo DataFrame
            mask = merged_df['Time_event'] <= merged_df['created_at_ticket']
            # Aplicar la máscara y realizar la actualización
            merged_df.loc[mask, 'has_ticket'] = True
            merged_df.loc[mask, 'ticket_id'] = merged_df['ticket_active_id']
            merged_df.loc[mask, 'ticket_error'] = None
            merged_df.loc[mask,
                          'ticket_status'] = merged_df['ticket_active_status']
            merged_df.drop(
                columns=['ticket_active_id', 'ticket_active_status', 'created_at_ticket', 'afiliacion', 'Time_event'], inplace=True)
            problems = merged_df

    return problems
