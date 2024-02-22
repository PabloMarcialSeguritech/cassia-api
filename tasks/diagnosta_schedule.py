import pandas as pd
from models.cassia_lpr_events import CassiaLprEvent as CassiaEventModel
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
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
        statement = text(
            f"call sp_viewProblem('0','','','6')")
        problems = pd.DataFrame(session.execute(statement))
        """ print(problems.to_string()) """
        problematicos = pd.DataFrame(columns=['eventid'])
        dependencias = pd.DataFrame(columns=['eventid'])

        statement2 = text(f"""SELECT * FROM cassia_diagnostic_problems""")
        sync = pd.DataFrame(session.execute(statement2))
        if not sync.empty:
            """  aqui deberia de ir event_id en la derecha"""
            no_sync = problems[problems['hostid'].isin(sync['eventid'])]
        else:
            no_sync = problems
        if not no_sync.empty:
            no_sync['synced'] = [0 for i in range(len(no_sync))]
        a_sincronizar = pd.DataFrame(
            columns=['eventid', 'depends_eventid', 'status', 'closed_at'])
        """ no_sync = no_sync[no_sync['hostid'] == 19834]
        print(no_sync) """
        repeticiones = 0
        for ind in no_sync.index:
            async with httpx.AsyncClient() as client:
                sinced = no_sync[no_sync['synced'] == 1]
                """ print(f"Sincronizados: {len(sinced)}") """
                if no_sync['synced'][ind] == 1:
                    """ print("saltara") """
                    continue
                """ print("prueba") """
                response = await client.get(f"{diagnosta_api_url}/analisis/{no_sync['hostid'][ind]}", timeout=90)

                response_json = response.json()

                if len(response_json) > 0:
                    for response in response_json:
                        """ print(response)
                        print(no_sync['hostid'][ind]) """
                        response = dict(response)
                        if 'capa1' in response:
                            procesed = await process_layer(
                                'capa1', response, no_sync, a_sincronizar, response_json, problematicos, dependencias)
                            a_sincronizar = procesed['a_sincronizar']
                            problematicos = procesed['problematicos']
                            dependencias = procesed['dependencias']
                        """ if 'capa2' in response:
                            a_sincronizar = await process_layer(
                                'capa2', response, no_sync, a_sincronizar, response_json) """

            repeticiones += 1
            print(repeticiones)

        """ print(a_sincronizar.to_string()) """
        """ len1 = len(problematicos) """
        problematicos = problematicos.drop_duplicates()
        a_guardar = pd.DataFrame()
        if not problematicos.empty:
            if not no_sync.empty:
                a_guardar = problematicos[~problematicos['eventid'].isin(
                    no_sync['eventid'].to_list())]
            else:
                a_guardar = problematicos
        a_cerrar = pd.DataFrame()
        if not sync.empty:
            if not problematicos.empty:
                a_cerrar = sync[~sync['eventid'].isin(
                    problematicos['eventid'].to_list())]
            else:
                a_cerrar = sync
        now = datetime.now(pytz.timezone('America/Mexico_City'))
        if not a_guardar.empty:
            a_guardar['depends_eventid'] = [
                None for i in range(len(a_guardar))]
            a_guardar['status'] = [
                'Sincronizado' for i in range(len(a_guardar))]
            a_guardar['closed_at'] = [None for i in range(len(a_guardar))]
            if not sync.empty:
                a_guardar = a_guardar[~a_guardar['eventid'].isin(
                    sync['eventid'].to_list())]
            sql = a_guardar.to_sql('cassia_diagnostic_problems', con=db_zabbix.engine,
                                   if_exists='append', index=False)
        if not a_cerrar.empty:

            statement = text(f"""
UPDATE cassia_diagnostic_problems
set closed_at=:date,
status=:status
where eventid in :ids
""")
            session.execute(statement, params={
                            'date': now, 'status': 'Cerrado', 'ids': a_cerrar['eventid'].to_list()})
        session.commit()
        """ print(a_guardar.to_string())
        print(a_cerrar.to_string()) """

        """ len2 = len(problematicos)
        print(problematicos.to_string())
        print(f"len 1 {len1},   len 2 : {len2}")
         """
        """ len1 = len(dependencias)
        dependencias = dependencias.drop_duplicates()
        len2 = len(dependencias)
        print(dependencias.to_string())
        print(f"len 1 {len1},   len 2 : {len2}") """
        """ print(dependencias.to_string()) """
        """ print(repeticiones) """

    pass


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


async def process_layer(capa_name: str, response_capa, no_sync: pd.DataFrame, a_sincronizar: pd.DataFrame, response_json, problematicos: pd.DataFrame, dependencias: pd.DataFrame):

    capa = response_capa[capa_name]
    analizado = capa['dispositivo_analizado']
    problematico = capa['dispositivo_problematico']
    if len(analizado) and not len(problematico):
        analizado = analizado[0]
        incorporar = no_sync[no_sync['hostid'].astype(int) == analizado[0]]
        for fila in incorporar.index:
            a_incorporar = [incorporar['hostid'][fila],
                            None,
                            'Sincronizado',
                            None]
            problematicos.loc[len(problematicos)] = a_incorporar[0]
            row_is_present = (a_sincronizar[['eventid', 'depends_eventid']] == [
                a_incorporar[0], a_incorporar[1]]).all(axis=1).any()
            if not row_is_present:
                a_sincronizar.loc[len(
                    a_sincronizar)] = a_incorporar
    if len(analizado) and len(problematico):

        if capa_name == 'capa1':
            analizado = analizado[0]
            problematico = problematico[0]
        if capa_name == 'capa2':
            analizado = analizado[0]
            problematico = problematico

        if analizado[0] == problematico[0]:

            filas_asociadas = no_sync[no_sync['hostid'].astype(int)
                                      == problematico[0]]

            if filas_asociadas.empty:
                print(f"Sin evento en host padre 1:{problematico[0]}")
            for fila in filas_asociadas.index:
                a_incorporar = [filas_asociadas['hostid'][fila],
                                None, 'Sincronizado', None]
                problematicos.loc[len(problematicos)] = a_incorporar[0]
                row_is_present = (a_sincronizar[['eventid', 'depends_eventid']] == [
                    a_incorporar[0], a_incorporar[1]]).all(axis=1).any()
                if not row_is_present:
                    a_sincronizar.loc[len(
                        a_sincronizar)] = a_incorporar
                """ print(type(no_sync['eventid']))
                print(type(a_incorporar[0])) """

                no_sync.loc[no_sync['hostid'].astype(int) ==
                            a_incorporar[0], 'synced'] = 1
        else:
            if len(response_json) > 1:
                response_dependientes = dict(
                    response_json[1])

                if 'desconectados_dependientes' in response_dependientes:
                    dependientes = response_dependientes['desconectados_dependientes']
                    filas_asociadas = no_sync[no_sync['hostid'].astype(int)
                                              == problematico[0]]
                    """ print(problematico[0]) """
                    """ print(filas_asociadas)
                                            print(type(no_sync['hostid'][0]))
                                            print(type(problematico[0])) """
                    evento_problematico = 0
                    if filas_asociadas.empty:
                        print(f"Sin evento en host padre 2:{problematico[0]}")
                    for fila in filas_asociadas.index:
                        a_incorporar = [filas_asociadas['hostid'][fila],
                                        None, 'Sincronizado', None]
                        problematicos.loc[len(problematicos)] = a_incorporar[0]
                        row_is_present = (a_sincronizar[['eventid', 'depends_eventid']] == [
                            a_incorporar[0], a_incorporar[1]]).all(axis=1).any()
                        if not row_is_present:
                            a_sincronizar.loc[len(
                                a_sincronizar)] = a_incorporar
                        no_sync.loc[no_sync['hostid'] ==
                                    a_incorporar[0], 'synced'] = 1
                        evento_problematico = filas_asociadas['hostid'][fila]
                        print("si entra aca")
                    for dependiente in dependientes:
                        filas_asociadas_dependientes = no_sync[no_sync['hostid']
                                                               == dependiente[0]]
                        if filas_asociadas_dependientes.empty:
                            print(
                                f"Sin evento en host hijo 3:{dependiente[0]}")
                        for fila_dependiente in filas_asociadas_dependientes.index:
                            a_incorporar = [
                                filas_asociadas_dependientes['hostid'][fila_dependiente], evento_problematico, 'Sincronizado', None]
                            dependencias.loc[len(
                                dependencias)] = a_incorporar[0]
                            row_is_present = (a_sincronizar[['eventid', 'depends_eventid']] == [
                                a_incorporar[0], a_incorporar[1]]).all(axis=1).any()
                            if not row_is_present:
                                a_sincronizar.loc[len(
                                    a_sincronizar)] = a_incorporar
                            no_sync.loc[no_sync['hostid'] ==
                                        a_incorporar[0], 'synced'] = 1

                            """ print(
                                                type(no_sync['eventid']))
                                            print(
                                                type(a_incorporar[0])) """
    return {'a_sincronizar': a_sincronizar,
            'problematicos': problematicos,
            'dependencias': dependencias}
