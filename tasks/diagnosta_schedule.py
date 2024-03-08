import pandas as pd
from models.cassia_lpr_events import CassiaLprEvent as CassiaEventModel
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
from models.cassia_arch_traffic_events_2 import CassiaArchTrafficEvent2

from rocketry import Grouper
from utils.settings import Settings
from utils.db import DB_Zabbix, DB_Syslog
from sqlalchemy import text
import numpy as np
from datetime import datetime, timedelta
import pytz
import time
import httpx
# Creating the Rocketry app
diagnosta_schedule = Grouper()

# Creating some tasks
SETTINGS = Settings()
diagnosta = SETTINGS.cassia_diagnosta
diagnosta_api_url = SETTINGS.cassia_diagnosta_api_url
traffic_syslog = SETTINGS.cassia_traffic_syslog


@diagnosta_schedule.cond('diagnosta')
def is_diagnosta():
    return diagnosta


@diagnosta_schedule.task(("every 60 seconds & diagnosta"), execution="thread")
async def process_problems():
    db_zabbix = DB_Zabbix()
    with db_zabbix.Session() as session:
        statement_zabbix = text(
            f"call sp_viewProblem('0','','','6')")
        problems_zabbix = pd.DataFrame(session.execute(statement_zabbix))
        statement_local = text(
            f"""SELECT * FROM cassia_arch_traffic_events cate 
WHERE cate.closed_at is NULL and 
severity =5
and message='Unavailable by ICMP ping'
and alert_type='diagnosta'""")
        problems_local = pd.DataFrame(session.execute(statement_local))
        sync_zabbix = text(
            f"""SELECT * FROM cassia_diagnostic_problems where closed_at is null and local=0""")
        sync_zabbix = pd.DataFrame(session.execute(sync_zabbix))
        sync_local = text(
            f"""SELECT * FROM cassia_diagnostic_problems where closed_at is null and local=1""")
        sync_local = pd.DataFrame(session.execute(sync_local))
        if not problems_zabbix.empty:
            if not sync_zabbix.empty:
                """  aqui deberia de ir event_id en la izquierda"""
                no_sync_zabbix = problems_zabbix[~problems_zabbix['eventid'].isin(
                    sync_zabbix['eventid'])]
            else:
                no_sync_zabbix = problems_zabbix
        if not problems_local.empty:
            if not sync_local.empty:
                no_sync_local = problems_local[~problems_local['cassia_arch_traffic_events_id'].isin(
                    sync_local['eventid'])]
            else:
                no_sync_local = problems_local
        else:
            no_sync_local = pd.DataFrame(
                columns=['cassia_arch_traffic_events_id',
                         'hostid', 'created_at',
                         'closed_at', 'severity',
                         'message', 'status', 'updated_at', 'latitude', 'longitude', 'municipality', 'ip',
                         'hostname', 'tech_id'])
            problems_local = pd.DataFrame(
                columns=['cassia_arch_traffic_events_id',
                         'hostid', 'created_at',
                         'closed_at', 'severity',
                         'message', 'status', 'updated_at', 'latitude', 'longitude', 'municipality', 'ip',
                         'hostname', 'tech_id'])
        if not no_sync_zabbix.empty:
            no_sync_zabbix['synced'] = [0 for i in range(len(no_sync_zabbix))]
        if not no_sync_local.empty:
            no_sync_local['synced'] = [0 for i in range(len(no_sync_local))]
        a_sincronizar = pd.DataFrame(
            columns=['eventid', 'depends_eventid', 'status', 'closed_at', 'local', 'hostid'])
        problemas_creados = pd.DataFrame(columns=['eventid', 'hostid'])
        problemas_sincronizados_activos = pd.DataFrame(
            columns=['eventid'])
        repeticiones = 0
        rep_ind = 0
        sync_rep = 2
        len_arr = len(no_sync_zabbix)
        sync_indice = 0
        ranges = 30
        range_low = 0
        range_up = 0

        for ind in no_sync_zabbix.index:
            async with httpx.AsyncClient() as client:
                range_low = ind*ranges
                if ranges*(ind+1) < len(no_sync_zabbix):
                    range_up = ranges*(ind+1)
                else:
                    range_up = len(no_sync_zabbix)+1
                if range_low >= len(no_sync_zabbix):
                    break
                """ print(range_low)
                print(range_up) """
                """ print(no_sync_zabbix.to_string()) """
                """ todos_ids = no_sync_zabbix['hostid'].astype(
                    'str').to_list() """
                ids = pd.DataFrame(columns=['hostid'], data=no_sync_zabbix['hostid'].astype(
                    'str')[range_low:range_up])
                """ print(ids)
                print(type(ids)) """
                for index in ids.index:
                    """ print(no_sync_zabbix.to_string()) """
                    row_is_present = (no_sync_zabbix[['hostid', 'synced']] == [
                        ids['hostid'][index], 1]).all(axis=1).any()
                    if row_is_present:
                        ids.drop(index)
                if no_sync_zabbix['synced'][ind] == 1:
                    continue
                if ids.empty:
                    continue
                ids_str = ",".join(ids['hostid'].to_list())
                """ print(ids) """
                """ print(ids) """
                """ return """
                """ print(ids_str) """
                response = await client.get(f"{diagnosta_api_url}/analisisM/{ids_str}", timeout=120)
                status = response.status_code
                if status == 500 or status == '500':
                    continue
                """ print(status) """
                response_json = response.json()
                if len(response_json) > 0:
                    for index in ids.index:
                        hostid = ids['hostid'][index]
                        if hostid in response_json:
                            res_host = response_json[hostid]
                            if 'capaGeneral' in res_host:
                                procesed = await process_layer(
                                    'capaGeneral', res_host, no_sync_zabbix, no_sync_local, a_sincronizar, session, problemas_creados, problems_local, problemas_sincronizados_activos)
                                """ print(ind) """
                                a_sincronizar = procesed
            repeticiones += 1
            rep_ind += 1

            """ if rep_ind == sync_rep or range_up >= len_arr: """
        print(f"entra {repeticiones}")
        print(f"indice {sync_indice}")
        a_sincronizar = a_sincronizar.drop_duplicates(
            subset='eventid')
        a_sincronizar.reset_index(drop=True, inplace=True)
        """ print(a_sincronizar)
        sincronizacion_parcial = a_sincronizar.iloc[:] """
        """ print(sincronizacion_parcial) """
        now_a = datetime.now(pytz.timezone('America/Mexico_City'))
        a_sincronizar['created_at'] = now_a
        if not a_sincronizar.empty:
            sql = a_sincronizar.to_sql('cassia_diagnostic_problems', con=db_zabbix.engine,
                                       if_exists='append', index=False)
        session.commit()
        """ rep_ind = 0
        if a_sincronizar.empty:
            sync_indice = len(a_sincronizar)
        else:
            sync_indice = len(a_sincronizar) """

        now = datetime.now(pytz.timezone('America/Mexico_City'))
        if not a_sincronizar.empty:
            if not sync_zabbix.empty:
                a_cerrar_zabbix = sync_zabbix[~sync_zabbix['eventid'].isin(
                    a_sincronizar['eventid'].to_list())]
                if not a_cerrar_zabbix.empty:
                    statement = text(f"""
                UPDATE cassia_diagnostic_problems
                set closed_at=:date,
                status=:status
                where eventid in :ids
                and local=0
                """)
                    session.execute(statement, params={
                        'date': now, 'status': 'Cerrado', 'ids': a_cerrar_zabbix['eventid'].to_list()})

            if not sync_local.empty:
                a_cerrar_local = sync_local[~sync_local['eventid'].isin(
                    a_sincronizar['eventid'].to_list()+problemas_sincronizados_activos['eventid'].to_list())]
                if not a_cerrar_local.empty:
                    statement = text(f"""
                        UPDATE cassia_diagnostic_problems
                        set closed_at=:date,
                        status=:status
                        where eventid in :ids
                        and local=1
                        """)
                    session.execute(statement, params={
                        'date': now, 'status': 'Cerrado', 'ids': a_cerrar_local['eventid'].to_list()})
                    statement2 = text(f"""
                        UPDATE cassia_arch_traffic_events
                        set closed_at=:date,
                        status=:status
                        where cassia_arch_traffic_events_id in :ids
                        and alert_type='diagnosta'
                        """)
                    session.execute(statement2, params={
                        'date': now, 'status': 'RESOLVED', 'ids': a_cerrar_local['eventid'].to_list()})

        session.commit()

    return


""" @diagnosta_schedule.task(("every 60 seconds & diagnosta"), execution="thread") """


async def close_problems():
    with DB_Zabbix().Session() as session:
        statement = text(
            f"call sp_viewProblem('','','','')")
        problems = pd.DataFrame(session.execute(statement))
        statement2 = text(f"""SELECT * FROM cassia_diagnostic""")
        sync = pd.DataFrame(session.execute(statement2))
        to_close = sync[~sync['eventid'].isin(problems['eventid'])]
        now = datetime.now(pytz.timezone("America/Mexico"))
        statement = f("""UPDATE cassia_diagnostic
        set closed_at = :date
        where eventid in :eventids""")
        update = session.execute(
            statement, {'date': now, 'eventids': to_close['eventids'].to_list()})
        session.commit()


async def process_layer(capa_name: str, response_capa, no_sync_zabbix: pd.DataFrame, no_sync_local: pd.DataFrame, a_sincronizar: pd.DataFrame, session, problemas_creados: pd.DataFrame, problems_local: pd.DataFrame, problemas_sincronizados_activos: pd.DataFrame):

    capa = response_capa[capa_name]
    if not "dispositivo_analizado" in capa and not "dispositivo_problematico" in capa:
        return a_sincronizar

    analizado = capa['dispositivo_analizado']
    problematico = capa['dispositivo_problematico']
    if not problematico:
        """ print("null") """
        return a_sincronizar
    if len(analizado) and not len(problematico):
        no_sync_zabbix.loc[no_sync_zabbix['hostid'].astype(int) ==
                           analizado[0], 'synced'] = 1

    if len(analizado) and len(problematico):

        if analizado[0] == problematico[0]:
            """ ENTRA SI EL ANALIZADO ES IGUAL AL PROBLEMATICO, ENTONCES SUS DEPENDIENTES NO NOS IMPORTAN PARA CREAR PROBLEMAS PERO SI PARA SALTARLOS """
            filas_asociadas = no_sync_zabbix[no_sync_zabbix['hostid'].astype(int)
                                             == problematico[0]]
            if filas_asociadas.empty:
                """ ENTRA SI NO HAY FILAS DE PROBLEMAS ASOCIADAS AL PROBLEMATICO """

                incorporar_local = no_sync_local[no_sync_local['hostid'].astype(
                    int) == analizado[0]]

                existe_local = problems_local[problems_local['hostid'].astype(
                    int) == analizado[0]]

                if not existe_local.empty:
                    if problemas_sincronizados_activos.empty:
                        problemas_sincronizados_activos.loc[len(
                            problemas_sincronizados_activos)+1] = [
                            existe_local['cassia_arch_traffic_events_id'][existe_local.index.max()]]
                    else:
                        problemas_sincronizados_activos.loc[problemas_sincronizados_activos.index.max(
                        )+1] = [existe_local['cassia_arch_traffic_events_id'][existe_local.index.max()]]
                    """ problemas_sincronizados_activos[len(problemas_sincronizados_activos)] = [
                        existe_local['cassia_arch_traffic_events_id'][existe_local.index.max()]] """
                if incorporar_local.empty and existe_local.empty:
                    """ SI NO EXISTE EN LOS NO SINCRONIZADOS LOCALES NI EN LOS PROBLEMAS LOCALES ENTRA """
                    row_is_present = (
                        problemas_creados[['hostid']] == analizado[0]).all(axis=1).any()
                    sincronizado = (a_sincronizar[['hostid']] == analizado[0]).all(
                        axis=1).any()
                    if not row_is_present and not sincronizado:
                        """ SI NO FUE CREADO YA EL PROBLEMA ENTRA """
                        host_padre = text(f"""
SELECT h.hostid ,h.host as hostname,hi.location_lat as latitude,hi.location_lon as longitude,
i.ip, cm.name as municipality,hi.device_id as tech_id  FROM hosts h 
INNER JOIN host_inventory hi  on h.hostid=hi.hostid 
inner join interface i on h.hostid =i.hostid
INNER JOIN hosts_groups hg on h.hostid= hg.hostid 
inner join cat_municipality cm on hg.groupid =cm.groupid 
where h.hostid={analizado[0]}
""")
                        host_padre = pd.DataFrame(session.execute(host_padre))
                        if not host_padre.empty:
                            now_a = datetime.now(
                                pytz.timezone('America/Mexico_City'))
                            problem_local = CassiaArchTrafficEvent(
                                hostid=host_padre['hostid'][0],
                                severity=5,
                                message='Unavailable by ICMP ping',
                                status='PROBLEM',
                                latitude=host_padre['latitude'][0],
                                longitude=host_padre['longitude'][0],
                                municipality=host_padre['municipality'][0],
                                hostname=host_padre['hostname'][0],
                                ip=host_padre['ip'][0],
                                tech_id=host_padre['tech_id'][0],
                                created_at=now_a,
                                updated_at=now_a,
                                alert_type='diagnosta')
                            session.add(problem_local)
                            session.commit()
                            problemas_creados.loc[len(problemas_creados)+1] = [
                                problem_local.cassia_arch_traffic_events_id, problem_local.hostid]

                            if a_sincronizar.empty:
                                a_sincronizar.loc[len(
                                    a_sincronizar)+1] = [problem_local.cassia_arch_traffic_events_id,
                                                         None,
                                                         'Sincronizado',
                                                         None, 1, problem_local.hostid]
                            else:
                                a_sincronizar.loc[a_sincronizar.index.max()+1] = [problem_local.cassia_arch_traffic_events_id,
                                                                                  None,
                                                                                  'Sincronizado',
                                                                                  None, 1, problem_local.hostid]

            for fila in filas_asociadas.index:
                """ SI EXISTEN FILAS ASOCIADAS ENTRA"""
                a_incorporar = [filas_asociadas['eventid'][fila],
                                None, 'Sincronizado', None, 0, filas_asociadas['hostid'][fila]]
                row_is_present = (a_sincronizar[['eventid', 'depends_eventid']] == [
                    a_incorporar[0], a_incorporar[1]]).all(axis=1).any()
                """ sincronizado = (no_sync_zabbix[['hostid']] == analizado[0]).all(
                    axis=1).any() """

                """ SI NO EXISTE EL PROBLEMA YA EN LOS QUE SE VAN A SINCRONIZAR LOS METE"""
                if not row_is_present:
                    if a_sincronizar.empty:
                        a_sincronizar.loc[len(a_sincronizar)+1] = a_incorporar
                    else:
                        a_sincronizar.loc[a_sincronizar.index.max(
                        )+1] = a_incorporar

                no_sync_zabbix.loc[no_sync_zabbix['eventid'].astype(int) ==
                                   a_incorporar[0], 'synced'] = 1
            if "desconectados_dependientes" in capa:
                """ MARCA LOS DEPENDIENTES COMO YA SINCRONIZADOS PARA NO ESTAR VERIFICANDO TODOS """
                dependientes = capa["desconectados_dependientes"]
                dependientes_ids = [dependiente[0]
                                    for dependiente in dependientes]
                if len(dependientes_ids):
                    no_sync_zabbix.loc[no_sync_zabbix['hostid'].isin(
                        dependientes_ids), 'synced'] = 1
        else:
            """ ENTRA SI HAY DESCONECTADOS DEPENDIENTES Y EL ANALIZADO ES DIFERENTE AL PROBLEMATICO """
            if "desconectados_dependientes" in capa:
                response_dependientes = capa["desconectados_dependientes"]

                dependientes = response_dependientes

                filas_asociadas = no_sync_zabbix[no_sync_zabbix['hostid'].astype(int)
                                                 == problematico[0]]

                if filas_asociadas.empty:

                    incorporar_local = no_sync_local[no_sync_local['hostid'].astype(
                        int) == problematico[0]]

                    existe_local = problems_local[problems_local['hostid'].astype(
                        int) == problematico[0]]
                    if not existe_local.empty:
                        if problemas_sincronizados_activos.empty:
                            problemas_sincronizados_activos.loc[len(
                                problemas_sincronizados_activos)+1] = [
                                existe_local['cassia_arch_traffic_events_id'][existe_local.index.max()]]
                        else:
                            problemas_sincronizados_activos.loc[problemas_sincronizados_activos.index.max(
                            )+1] = [existe_local['cassia_arch_traffic_events_id'][existe_local.index.max()]]
                        """ problemas_sincronizados_activos[len(problemas_sincronizados_activos)] = [
                            existe_local['cassia_arch_traffic_events_id'][existe_local.index.max()]] """
                    if incorporar_local.empty and existe_local.empty:
                        row_is_present = (
                            problemas_creados[['hostid']] == problematico[0]).all(axis=1).any()
                        sincronizado = (a_sincronizar[['hostid']] == analizado[0]).all(
                            axis=1).any()
                        if not row_is_present and not sincronizado:
                            host_padre = text(f"""
SELECT h.hostid ,h.host as hostname,hi.location_lat as latitude,hi.location_lon as longitude,
i.ip, cm.name as municipality,hi.device_id as tech_id  FROM hosts h 
INNER JOIN host_inventory hi  on h.hostid=hi.hostid 
inner join interface i on h.hostid =i.hostid
INNER JOIN hosts_groups hg on h.hostid= hg.hostid 
inner join cat_municipality cm on hg.groupid =cm.groupid 
where h.hostid={problematico[0]}
""")
                            host_padre = pd.DataFrame(
                                session.execute(host_padre))
                            if not host_padre.empty:
                                now_a = datetime.now(
                                    pytz.timezone('America/Mexico_City'))
                                problem_local = CassiaArchTrafficEvent(
                                    hostid=host_padre['hostid'][0],
                                    severity=5,
                                    message='Unavailable by ICMP ping',
                                    status='PROBLEM',
                                    latitude=host_padre['latitude'][0],
                                    longitude=host_padre['longitude'][0],
                                    municipality=host_padre['municipality'][0],
                                    hostname=host_padre['hostname'][0],
                                    ip=host_padre['ip'][0],
                                    tech_id=host_padre['tech_id'][0],
                                    created_at=now_a,
                                    updated_at=now_a,
                                    alert_type='diagnosta')
                                session.add(problem_local)
                                session.commit()
                                problemas_creados.loc[len(problemas_creados)+1] = [
                                    problem_local.cassia_arch_traffic_events_id, problem_local.hostid]
                                print(
                                    f"se creo el evento {problem_local.cassia_arch_traffic_events_id}")
                                if a_sincronizar.empty:
                                    a_sincronizar.loc[len(
                                        a_sincronizar)+1] = [problem_local.cassia_arch_traffic_events_id,
                                                             None,
                                                             'Sincronizado',
                                                             None, 1, problem_local.hostid]
                                else:
                                    a_sincronizar.loc[a_sincronizar.index.max()+1] = [problem_local.cassia_arch_traffic_events_id,
                                                                                      None,
                                                                                      'Sincronizado',
                                                                                      None, 1, problem_local.hostid]
                                """ print(a_sincronizar) """

                for fila in filas_asociadas.index:
                    a_incorporar = [filas_asociadas['eventid'][fila],
                                    None, 'Sincronizado', None, 0, filas_asociadas['hostid'][fila]]

                    row_is_present = (a_sincronizar[['eventid', 'depends_eventid']] == [
                        a_incorporar[0], a_incorporar[1]]).all(axis=1).any()
                    if not row_is_present:
                        if a_sincronizar.empty:
                            a_sincronizar.loc[len(
                                a_sincronizar)+1] = a_incorporar
                        else:
                            a_sincronizar.loc[a_sincronizar.index.max(
                            )+1] = a_incorporar
                    no_sync_zabbix.loc[no_sync_zabbix['eventid'] ==
                                       a_incorporar[0], 'synced'] = 1

                dependientes_ids = [dependiente[0]
                                    for dependiente in dependientes]
                if len(dependientes_ids):
                    no_sync_zabbix.loc[no_sync_zabbix['hostid'].isin(
                        dependientes_ids), 'synced'] = 1

    return a_sincronizar
