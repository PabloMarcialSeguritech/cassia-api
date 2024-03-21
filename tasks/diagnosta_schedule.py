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


@diagnosta_schedule.task(("every 20 seconds & diagnosta"), execution="thread")
async def process_problems():
    db_zabbix = DB_Zabbix()
    with db_zabbix.Session() as session:
        downs = get_downs(session)
        cassia_events = get_cassia_events(session)
        downs_totales = get_df_down_totales(downs, cassia_events)
        sincronizados = get_sincronizados(session)
        a_diagnosticar = get_a_diagnosticar(downs_totales, sincronizados)
        low = 0
        len_iteracion = 30
        up = len_iteracion
        a_sincronizar = pd.DataFrame(columns=['hostid_origen',
                                     'depends_hostid', 'status', 'closed_at', 'local', 'hostid', 'dependents', 'local_eventid'])
        for ind in a_diagnosticar.index:
            init = datetime.now()
            low = ind*len_iteracion
            if len_iteracion*(ind+1) < len(a_diagnosticar):
                up = len_iteracion*(ind+1)
            else:
                up = len(a_diagnosticar)+1
            if low >= len(a_diagnosticar):
                break
            ids = pd.DataFrame(columns=['hostid'], data=a_diagnosticar['hostid'].astype(
                'str')[low:up])

            """ ids = ids.loc[~ids['hostid'].isin(
                a_diagnosticar['hostid', a_diagnosticar['diagnostico'] == 1].to_list())] """

            await get_diagnostico(ids, a_sincronizar, a_diagnosticar,
                                  downs_totales, cassia_events, session)
            end = datetime.now()
            total = end - init
            """ print(a_sincronizar.to_string())
            print(f"-----------total : {total}--------------------") """
        a_sincronizar = a_sincronizar.drop_duplicates(
            subset=['hostid_origen', 'depends_hostid'])
        a_sincronizar.reset_index(drop=True, inplace=True)
        now_a = datetime.now(pytz.timezone('America/Mexico_City'))
        a_sincronizar['created_at'] = now_a
        if not a_sincronizar.empty:
            sql = a_sincronizar.to_sql('cassia_diagnostic_problems_2', con=db_zabbix.engine,
                                       if_exists='append', index=False)
        session.commit()


def get_df_down_totales(downs: pd.DataFrame, cassia_events: pd.DataFrame):
    downs['local'] = 0
    downs_totales = downs
    downs_totales.loc[downs_totales['hostid'].isin(
        cassia_events['hostid'].to_list()), 'local'] = 1
    downs_locales = downs_totales.loc[downs_totales['local'] == 1]
    downs_totales.drop(downs_locales.index.to_list())

    downs_locales = pd.merge(
        downs_locales, cassia_events, how='left', on=['hostid', 'ip'])
    if not downs_locales.empty:
        downs_locales.drop(columns=['eventid'], inplace=True)
        downs_locales.rename(
            columns={'cassia_arch_traffic_events_id': 'eventid'}, inplace=True)

    downs_totales = pd.concat(
        [downs_locales, downs_totales], ignore_index=True)
    if not downs_totales.empty:
        downs_totales['diagnostico'] = 0
    return downs_totales


def get_cassia_events(session):
    cassia_events = pd.DataFrame(session.execute(text("""SELECT cassia_arch_traffic_events_id ,cate.hostid ,cate.ip,cate.message  FROM cassia_arch_traffic_events_2 cate
    where closed_at is null and alert_type ='diagnosta'""")))
    return pd.DataFrame(
        columns=['cassia_arch_traffic_events_id', 'hostid', 'ip', 'message']) if cassia_events.empty else cassia_events


def get_downs(session):
    return pd.DataFrame(session.execute(
        text("CALL sp_validationDown();")))


def get_sincronizados(session):
    sincronizados = pd.DataFrame(session.execute(
        text("SELECT * FROM cassia_diagnostic_problems_2 where closed_at is null")))

    return pd.DataFrame(
        columns=['diagnostic_problem_id', 'hostid_origen', 'depends_hostid', 'status', 'closed_at', 'local', 'hostid', 'created_at', 'dependents', 'local_eventid']) if sincronizados.empty else sincronizados


def get_a_diagnosticar(downs_totales: pd.DataFrame, sincronizados: pd.DataFrame):
    return downs_totales[~downs_totales['hostid'].isin(
        sincronizados['hostid'].to_list())]


async def get_diagnostico(ids: pd.DataFrame, a_sincronizar: pd.DataFrame, a_diagnosticar: pd.DataFrame, downs: pd.DataFrame, cassia_events: pd.DataFrame, session):
    async with httpx.AsyncClient() as client:
        ids_str = ",".join(ids['hostid'].to_list())
        response = await client.get(f"{diagnosta_api_url}/analisisM/{ids_str}", timeout=120)
        status = response.status_code

        if status == 500 or status == '500':
            return (None, None)
        """ print(status) """
        response_json = response.json()
        if len(response_json) > 0:
            for index in ids.index:
                hostid = ids['hostid'][index]
                if hostid in response_json:
                    res_host = response_json[hostid]

                    if 'capaGeneral' in res_host:
                        a_sincronizar = process_diagnostico(a_sincronizar,
                                                            res_host, a_diagnosticar, downs, cassia_events, session)


def process_diagnostico(a_sincronizar: pd.DataFrame, res_host, a_diagnosticar: pd.DataFrame, downs: pd.DataFrame, cassia_events: pd.DataFrame, session):
    res_host = res_host['capaGeneral']
    if not "dispositivo_analizado" in res_host and not "dispositivo_problematico" in res_host:
        return a_sincronizar
    analizado = res_host['dispositivo_analizado']
    problematico = res_host['dispositivo_problematico']
    if not problematico:
        """ print("null") """
        return a_sincronizar
    if len(problematico):
        """ print(res_host)
        print(problematico)
        print("AAAAAAAAAAAAAAAAAAA") """
        """ print(problematico) """
        if not problematico[9]:
            """ print("BBBBBBBBB") """
            sincronizado = a_sincronizar.loc[a_sincronizar['hostid']
                                             == problematico[0]]

            if not sincronizado.empty:
                return a_sincronizar
            """ print("F3") """
            es_down = downs.loc[downs['hostid'].astype('str')
                                == str(problematico[0])]
            if es_down.empty:
                return a_sincronizar
            """ print("F4") """
            tiene_eventos = downs[downs['hostid'] == problematico[0]]
            """ print(tiene_eventos) """
            tiene_eventos = tiene_eventos[tiene_eventos['eventid']
                                          == np.nan]
            """ print(tiene_eventos) """
            if tiene_eventos.empty:
                host_padre = text(f"""
    SELECT h.hostid ,h.host as hostname,hi.location_lat as latitude,hi.location_lon as longitude,
    i.ip, cm.name as municipality,hi.device_id as tech_id  FROM hosts h 
    INNER JOIN host_inventory hi  on h.hostid=hi.hostid 
    inner join interface i on h.hostid =i.hostid
    INNER JOIN hosts_groups hg on h.hostid= hg.hostid 
    inner join cat_municipality cm on hg.groupid =cm.groupid 
    where h.hostid={problematico[0]}
    """)
                host_padre = pd.DataFrame(session.execute(host_padre))
                if not host_padre.empty:
                    now_a = datetime.now(
                        pytz.timezone('America/Mexico_City'))
                    problem_local = CassiaArchTrafficEvent2(
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

                    dependientes_caidos = 0
                    if "desconectados_dependientes" in res_host:

                        dependientes = res_host["desconectados_dependientes"]
                        dependientes_caidos = [dependiente[9]
                                               for dependiente in dependientes]

                        dependientes_ids = []
                        for dependiente in dependientes:
                            if not dependiente[9]:
                                dependientes_ids.append(dependiente[0])

                        dependientes_caidos = len(dependientes_ids)
                        for id_dependiente in dependientes_ids:
                            dependiente_sincronizado = (a_sincronizar[['depends_hostid']] == [
                                id_dependiente]).all(axis=1).any()
                            dependientes_es_down = (downs[['hostid']] == [
                                id_dependiente]).all(axis=1).any()

                            if not dependiente_sincronizado and dependientes_es_down:
                                if a_sincronizar.empty:
                                    a_sincronizar.loc[len(
                                        a_sincronizar)+1] = [problem_local.hostid,
                                                             id_dependiente,
                                                             'Sincronizado',
                                                             None, 0, id_dependiente, 0, None]
                                else:
                                    a_sincronizar.loc[a_sincronizar.index.max(
                                    )+1] = [problem_local.hostid,
                                            id_dependiente,
                                            'Sincronizado',
                                            None, 0, id_dependiente, 0, None]

                            downs.loc[downs['hostid'].astype(int) ==
                                      id_dependiente, 'diagnostico'] = 1

                    if a_sincronizar.empty:
                        a_sincronizar.loc[len(
                            a_sincronizar)+1] = [problem_local.hostid,
                                                 None,
                                                 'Sincronizado',
                                                 None, 1, problem_local.hostid, dependientes_caidos, problem_local.cassia_arch_traffic_events_id]
                    else:
                        a_sincronizar.loc[a_sincronizar.index.max()+1] = [problem_local.hostid,
                                                                          None,
                                                                          'Sincronizado',
                                                                          None, 1, problem_local.hostid, dependientes_caidos, problem_local.cassia_arch_traffic_events_id]
            else:

                if "desconectados_dependientes" in res_host:
                    dependientes = res_host["desconectados_dependientes"]
                    dependientes_caidos = [dependiente[9]
                                           for dependiente in dependientes]

                    dependientes_ids = []
                    for dependiente in dependientes:
                        if not dependiente[9]:
                            dependientes_ids.append(dependiente[0])
                    dependientes_caidos = len(dependientes_ids)
                    for id_dependiente in dependientes_ids:
                        dependiente_sincronizado = (a_sincronizar[['depends_hostid']] == [
                            id_dependiente]).all(axis=1).any()
                        dependientes_es_down = (downs[['hostid']] == [
                            id_dependiente]).all(axis=1).any()

                        if not dependiente_sincronizado and dependientes_es_down:
                            if a_sincronizar.empty:
                                a_sincronizar.loc[len(
                                    a_sincronizar)+1] = [problematico[0],
                                                         id_dependiente,
                                                         'Sincronizado',
                                                         None, 0, id_dependiente, 0, None]
                            else:
                                a_sincronizar.loc[a_sincronizar.index.max(
                                )+1] = [problematico[0],
                                        id_dependiente,
                                        'Sincronizado',
                                        None, 0, id_dependiente, 0, None]

                        downs.loc[downs['hostid'].astype(int) ==
                                  id_dependiente, 'diagnostico'] = 1

                    if a_sincronizar.empty:
                        a_sincronizar.loc[len(
                            a_sincronizar)+1] = [problematico[0],
                                                 None,
                                                 'Sincronizado',
                                                 None, 1, problematico[0], dependientes_caidos, None]
                    else:
                        a_sincronizar.loc[a_sincronizar.index.max()+1] = [problematico[0],
                                                                          None,
                                                                          'Sincronizado',
                                                                          None, 1, problematico[0], None]
        return a_sincronizar


@diagnosta_schedule.task(("every 60 seconds & diagnosta"), execution="thread")
async def close_problems():
    db_zabbix = DB_Zabbix()
    with db_zabbix.Session() as session:
        downs = get_downs(session)
        cassia_events = get_cassia_events(session)
        downs_totales = get_df_down_totales(downs, cassia_events)
        sincronizados = get_sincronizados(session)
        a_cerrar = sincronizados[~sincronizados['hostid'].isin(
            downs['hostid'].to_list())]
        """ print(a_cerrar.to_string()) """
        if a_cerrar.empty:
            ids = [0]
        else:
            ids = a_cerrar['diagnostic_problem_id'].to_list()
        now = datetime.now(pytz.timezone('America/Mexico_City'))

        statement = text(f"""UPDATE cassia_diagnostic_problems_2
        set closed_at=:date,
        status=:status
        where diagnostic_problem_id in :ids""")
        """ print(statement) """
        session.execute(statement, params={
                        'date': now, 'status': 'Cerrado', 'ids': ids})
        statement = text(f"""
        UPDATE cassia_arch_traffic_events_2
        set closed_at=:now,
        status=:status               
        where cassia_arch_traffic_events_id in :ids""")
        ids = a_cerrar[a_cerrar['local'] == 1]
        ids = ids[ids['local_eventid'] != np.nan]
        if ids.empty:
            ids = [0]
        else:
            ids = ids['local_eventid'].to_list()
        session.execute(statement, params={'now': now, 'status': 'RESOLVED',
                        'ids': ids})
        session.commit()

        sincronizados = get_sincronizados(session)
        """ if not sincronizados.empty:
            sincronizados['local_eventid']=sincronizados['local_eventid'].astype('int') """
        sincronizados_locales = sincronizados[sincronizados['local'] == 1]
        """ print("Sincronizados locales:", sincronizados_locales.to_string()) """
        if not downs.empty:
            downs['eventid'].replace(np.nan, 0, inplace=True)
            downs['eventid'] = downs['eventid'].astype('int')
        downs_evento_zabbix = downs[downs['eventid'] != 0]
        """ print("Downs de zabbix:", downs_evento_zabbix.to_string()) """
        """ print(type(downs['eventid'][140]))
        print("Downs de zabbix:", downs_evento_zabbix.to_string()) """
        a_cerrar_locales = sincronizados_locales[sincronizados_locales['hostid'].isin(
            downs_evento_zabbix['hostid'])]
        """ print("A cerrar locales:", a_cerrar_locales.to_string()) """
        if a_cerrar_locales.empty:
            ids = [0]
        else:
            ids = a_cerrar_locales['diagnostic_problem_id'].to_list()
        now = datetime.now(pytz.timezone('America/Mexico_City'))
        statement = text(f"""
        UPDATE cassia_diagnostic_problems_2
        set closed_at= :now,
        status= :status
        where diagnostic_problem_id in :ids""")
        session.execute(statement, params={'now': now, 'status': 'Cerrado',
                        'ids': ids})
        statement = text(f"""
        UPDATE cassia_arch_traffic_events_2
        set closed_at=:now,
        status=:status               
        where cassia_arch_traffic_events_id in :ids""")
        ids = a_cerrar_locales[a_cerrar_locales['local'] == 1]
        ids = ids[ids['local_eventid'] != np.nan]
        """ print(ids.to_string()) """
        if ids.empty:
            ids = [0]
        else:
            ids = ids['local_eventid'].astype(int).to_list()
        """ print(ids) """
        session.execute(statement, params={'now': now, 'status': 'RESOLVED',
                        'ids': ids})
        session.commit()

        sincronizados = get_sincronizados(session)
        sincronizados_locales = sincronizados[sincronizados['local'] == 1]
        if not sincronizados_locales.empty:
            sincronizados_locales['local_eventid'] = sincronizados_locales['local_eventid'].astype(
                'int')
        sincronizados_locales['duplicates'] = sincronizados_locales.groupby('hostid')[
            'hostid'].transform('count')
        sincronizados_locales = sincronizados_locales[sincronizados_locales['duplicates'] > 1]

        a_cerrar_sincronizados = [0]
        eventos_a_cerrar = [0]
        hostid = 0
        if not sincronizados_locales.empty:
            for ind in sincronizados_locales.index:
                if hostid != sincronizados_locales['hostid'][ind]:
                    hostid = sincronizados_locales['hostid'][ind]
                    continue
                a_cerrar_sincronizados.append(
                    sincronizados_locales['diagnostic_problem_id'][ind])
                eventos_a_cerrar.append(
                    sincronizados_locales['local_eventid'][ind])

            statement = text(f"""
        UPDATE cassia_diagnostic_problems_2
        set closed_at= :now,
        status= :status
        where diagnostic_problem_id in :ids""")
            session.execute(statement, params={'now': now, 'status': 'Cerrado',
                            'ids': a_cerrar_sincronizados})
            statement = text(f"""
            UPDATE cassia_arch_traffic_events_2
            set closed_at=:now,
            status=:status               
            where cassia_arch_traffic_events_id in :ids""")
            session.execute(statement, params={'now': now, 'status': 'RESOLVED',
                                               'ids': eventos_a_cerrar})
            session.commit()


def get_zabbix_events(downs):
    return downs['eventid'] != np.nan


def a():
    pass
    sincronizados = get_sincronizados(session)
    sincronizados_locales = sincronizados[sincronizados['local'] == 1]
    downs_evento_zabbix = downs[downs['eventid'] != np.nan]
    a_cerrar_locales = sincronizados_locales[sincronizados_locales['hostid'].isin(
        downs_evento_zabbix['hostid'])]
    if a_cerrar_locales.empty:
        ids = [0]
    else:
        ids = a_cerrar_locales['diagnostic_problem_id'].to_list()
    now = datetime.now(pytz.timezone('America/Mexico_City'))
    statement = text(f"""
    UPDATE cassia_diagnostic_problems_2
    set closed_at= :now,
    status= :status
    where diagnostic_problem_id in :ids""")
    session.execute(statement, params={'now': now, 'status': 'Cerrado',
                    'ids': ids})
    statement = text(f"""
    UPDATE cassia_arch_traffic_events_2
    set closed_at=:now,
    status=:status               
    where cassia_arch_traffic_events_id in :ids""")
    ids = a_cerrar_locales[a_cerrar_locales['local'] == 1]
    ids = ids[ids['local_eventid'] != np.nan]
    if ids.empty:
        ids = [0]
    else:
        ids = ids['local_eventid'].to_list()
    session.execute(statement, params={'now': now, 'status': 'RESOLVED',
                    'ids': ids})
    session.commit()
