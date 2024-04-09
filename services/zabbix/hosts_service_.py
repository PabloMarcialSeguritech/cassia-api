from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
from utils.traits import success_response, failure_response
import numpy as np
from paramiko import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import AuthenticationException, BadHostKeyException, SSHException
from cryptography.fernet import Fernet
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
from models.cassia_actions import CassiaAction as CassiaActionModel
from models.interface_model import Interface
from models.cassia_action_log import CassiaActionLog
import socket
from datetime import datetime
import pytz
from fastapi.exceptions import HTTPException
from fastapi import status
from services.cassia.configurations_service import get_configuration
import json
import re
import time
from pythonping import ping
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import aiomysql
import asyncssh
import asyncio
import subprocess
import infraestructure.zabbix.host_repository as host_repository

settings = Settings()


def decrypt(encripted_text, key):
    fernet = Fernet(key)
    return fernet.decrypt(encripted_text.encode())


async def run_action_(ip, id_action, current_user_session):
    response = None
    data = {}
    try:
        # 1.- Obtener la accion mediante su id_action
        action = await get_action(id_action)
        data['ip'] = ip
        if action:
            # 2.- Determinar el tipo de acción
            action_type = await get_action_type(id_action)
            if action_type == 0:
                has_proxy, proxy_credentials = await proxy_validation(ip)
                if has_proxy:
                    success_ping, ping_message = await async_ping(ip, proxy_credentials['ip_proxy'],
                                                                  proxy_credentials['user_proxy'],
                                                                  proxy_credentials['password_proxy'])
                    response = await create_response(success_ping, ping_message, data)
                    return response
                else:
                    success_ping, ping_message = await async_ping_by_local(ip)
                    response = await create_response(success_ping, ping_message, data)

                    return response
            elif action_type == -1:
                print("Otra..")
            else:
                print("Accion no determinada...")

            return action_type
        else:
            message = "ID acción necesaria"
            success_ping = False
            response = await create_response(success_ping, message)
            return response
    except ValueError as error:
        message = error.args[0] if error.args else "No se proporcionó un mensaje de error"
        success_ping = False
        response = await create_response(success_ping, message)
        return response


async def get_action_type(id_action):
    action_type = -1  # caso cuando no se determina el tipo de accion
    # 1.- Determinando si es ping la accion
    with DB_Zabbix().Session() as session:
        ping_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == 'ping_id').first()
        if ping_id:
            action_type = 0  # caso cuando se determina que el tipo de accion es tipo ping
    return action_type


async def get_action(id_action):
    with DB_Zabbix().Session() as session:
        action = session.query(CassiaActionModel).filter(CassiaActionModel.action_id == id_action).first()
    session.close()
    return action


async def create_response(success: bool = False, message: str = "unsuccessful", data=None):
    if data is None:
        data = {"action": "false"}
    else:
        data['action'] = 'false'

    response = {
        'message': message,
        'success': success,
        'data': data,
        'status_code': 200,
        'recommendation': ''
    }

    if success:
        response['success'] = True
        data['action'] = 'true'

    return JSONResponse(content=jsonable_encoder(response), status_code=response['status_code'])


async def proxy_validation(ip):
    has_proxy = 0
    credentials = None
    try:
        ip_proxy, user_proxy, password_proxy = await get_credentials_for_proxy_async(ip)
        if ip_proxy and user_proxy and password_proxy:
            has_proxy = 1
            credentials = {
                'ip_proxy': ip_proxy,
                'user_proxy': user_proxy,
                'password_proxy': password_proxy
            }
        return has_proxy, credentials
    except (ValueError, Exception) as e:
        print(f"Error en proxy_validation: {e}")
        has_proxy = False
        credentials = {
            'ip_proxy': None,
            'user_proxy': None,
            'password_proxy': None
        }
        return has_proxy, credentials


async def async_ping_by_proxy(ip, ssh_host, ssh_user, ssh_pass):
    # Si hace el ping retorna 1 sino retorna 0
    success_ping = False
    message = ""
    try:

        async with asyncssh.connect(ssh_host, username=ssh_user, password=ssh_pass, known_hosts=None,
                                    connect_timeout=6) as conn:
            result = await conn.run(f'ping -c 2 -W 5 {ip}', check=False)
            is_package_lost = await verify_output_ping(result.stdout)
            if is_package_lost:
                success_ping = False
                message = "no ejecutado con exito"
            else:
                success_ping = True
                message = "success"
            return success_ping, message

    except Exception as e:
        print(f"Excepción ocurrida en la función async_ping_by_proxy:{str(e)}")
        success_ping = False
        message = "no ejecutado con exito"
        return success_ping, message


async def get_db_connection():
    try:
        connection = await aiomysql.connect(
            host=settings.db_zabbix_host,
            port=int(settings.db_zabbix_port),  # Asegúrate de que el puerto sea un entero
            db=settings.db_zabbix_name,
            user=settings.db_zabbix_user,
            password=settings.db_zabbix_pass,
            autocommit=True
        )
        return connection
    except aiomysql.Error as e:
        print(f"Error while establishing database connection: {e}")
        raise  # Re-lanza la excepción para que el llamador pueda manejarla adecuadamente


async def run_stored_procedure_async(connection, sp_name, sp_params):
    try:
        async with connection.cursor() as cursor:
            await cursor.callproc(sp_name, sp_params)
            result_sets = []
            more_results = True
            while more_results:
                results = await cursor.fetchall()
                if cursor.description is not None:  # Verificar si hay resultados antes de obtener los nombres de las columnas
                    column_names = [column[0] for column in cursor.description]  # Obtener los nombres de las columnas
                    result_dicts = [dict(zip(column_names, row)) for row in
                                    results]  # Convertir los resultados a diccionarios
                    result_sets.extend(result_dicts)
                more_results = await cursor.nextset()
            return result_sets
    except aiomysql.Error as error:
        print(f"Error while calling stored procedure: {error}")
        return []


async def get_credentials_for_proxy_async(ip):
    connection = None
    try:
        connection = await get_db_connection()
        if connection is None:
            print("Error: Failed to establish database connection")
            return
        response_data_base = await run_stored_procedure_async(connection, 'sp_proxy_credential', (ip,))
        if response_data_base:
            credentials = pd.DataFrame(response_data_base).replace(np.nan, "").to_dict(orient="records")
            ip_proxy = credentials[0]['ip']
            user_proxy = decrypt(credentials[0]['usr'], settings.ssh_key_gen).decode()
            password_proxy = decrypt(credentials[0]['psswrd'], settings.ssh_key_gen).decode()
        else:
            raise ValueError("El proxy no esta configurado o las credenciales configuradas son incorrectas")
    finally:
        connection.close()

    return ip_proxy, user_proxy, password_proxy


async def verify_output_ping(output):
    is_package_lost = 1
    text_coincidence = re.search(
        r'(\d+ packets transmitted, \d+ received, 100% packet loss, time \d+ms)',
        output)
    if not text_coincidence:
        is_package_lost = 0
    return is_package_lost


async def async_ping_by_local(ip):
    # -n para windows -c para ubuntu
    cmd = ['ping', '-c', '4', '-w', '5', ip]  # Envía 4 paquetes
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await process.communicate()
        output = stdout.decode('utf-8', errors='ignore')
        if "100% packet loss" in output or "100% pérdida de paquetes" in output or "100% perdidos" in output:
            success_ping = False
            message = "no ejecutado con exito"
            return success_ping, message
        else:
            success_ping = True
            message = "success"
            return success_ping, message

    except Exception as e:
        print(f"Excepción ocurrida en la función async_ping_by_local: {e}")
        success_ping = False
        message = "no ejecutado con exito"
        return success_ping, message


async def async_ping(ip, ssh_host, ssh_user, ssh_pass):
    try:
        tasks = {
            asyncio.create_task(async_ping_by_local(ip)),
            asyncio.create_task(async_ping_by_proxy(ip, ssh_host, ssh_user, ssh_pass))
        }
        while tasks:
            # Espera hasta que al menos una tarea se complete
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # Verificar las tareas completadas por un resultado exitoso
            for task in done:
                success, message = task.result()
                if success == 1:
                    # Cancelar cualquier tarea restante
                    for t in pending:
                        t.cancel()
                    return success, message

            # Si no se encontró una tarea exitosa, continuar con las tareas pendientes
            tasks = pending

        # Si llegamos a este punto, ninguna tarea fue exitosa
        success = False
        message = "no ejecutado con exito"
        return success, message

    except Exception as e:
        print(f"Excepción ocurrida en la función async_ping: {e}")
        success = False
        message = f"No exitoso por excepción: {str(e)}"
        return success, message


async def get_host_metrics_(host_id):
    host_health_detail = pd.DataFrame(await host_repository.get_host_health_detail(host_id)).replace(np.nan, "")
    return success_response(data=host_health_detail.to_dict(orient="records"))


async def get_host_filter_(municipalityId, dispId, subtype_id):

    if subtype_id == "0":
        subtype_id = ""
    if dispId == "0":
        dispId = ""
    if dispId == "-1":
        dispId = ''

    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    switch_config = pd.DataFrame(await host_repository.get_switch_config())
    print(1)
    switch_id = "12"
    switch_troughtput = False
    if not switch_config.empty:
        switch_id = switch_config['value'].iloc[0]
    metric_switch_val = "Interface Bridge-Aggregation_: Bits"
    metric_switch = pd.DataFrame(await host_repository.get_switch_throughput_config())
    if not metric_switch.empty:
        metric_switch_val = metric_switch['value'].iloc[0]
    if subtype_id == metric_switch_val:
        subtype_id = ""

        if dispId == switch_id:
            switch_troughtput = True

    """ if subtype_id == "376276" or subtype_id == "375090":
        subtype_host_filter = '376276,375090'
    else:
        subtype_host_filter = subtype_id """
    """ arcos_band = False """
    rfid_config = pd.DataFrame(await host_repository.get_rfid_config())
    rfid_id = "9"
    if not rfid_config.empty:
        rfid_id = rfid_config['value'].iloc[0]
    if subtype_id == "3":
        subtype_id = ""
    if dispId == "11":
        dispId_filter = "11,2"
    else:
        dispId_filter = dispId
    statement1 = text(
        f"call sp_hostView('{municipalityId}','{dispId}','{subtype_id}')")
    if dispId == "11":
        statement1 = text(
            f"call sp_hostView('{municipalityId}','{dispId},2','')")
    hosts = session.execute(statement1)
    print("hosts:::", hosts)

    if dispId == "11":
        hosts_ = await host_repository.get_host_view(municipalityId, f'{dispId},2', None)

    else:
        hosts_ = await host_repository.get_host_view(municipalityId, f'{dispId}', subtype_id)

    print("hosts_::::", hosts_)
    # print(problems)
    data = pd.DataFrame(hosts)
    data = data.replace(np.nan, "")

    if len(data) > 1:
        hostids = tuple(data['hostid'].values.tolist())
    else:
        if len(data) == 1:
            hostids = f"({data['hostid'][0]})"
        else:
            hostids = "(0)"

    statement2 = text(
        f"""
        SELECT hc.correlarionid,
        hc.hostidP,
        hc.hostidC,
        (SELECT location_lat from host_inventory where hostid=hc.hostidP) as init_lat,
        (SELECT location_lon from host_inventory where hostid=hc.hostidP) as init_lon,
        (SELECT location_lat from host_inventory where hostid=hc.hostidC) as end_lat,
        (SELECT location_lon from host_inventory where hostid=hc.hostidC) as end_lon
        from host_correlation hc
        where (SELECT location_lat from host_inventory where hostid=hc.hostidP) IS NOT NULL 
        and
        (
        hc.hostidP in {hostids}
        and hc.hostidC in {hostids})
        """
    )
    corelations = session.execute(statement2)
    statement3 = text(
        f"CALL sp_problembySev('{municipalityId}','{dispId_filter}','{subtype_id}')")
    problems_by_sev = session.execute(statement3)
    data3 = pd.DataFrame(problems_by_sev).replace(np.nan, "")
    if dispId == rfid_id:
        if municipalityId == '0':
            alertas_rfid = session.query(CassiaArchTrafficEvent).filter(
                CassiaArchTrafficEvent.closed_at == None,
            ).all()

        else:
            statement = text("call sp_catCity()")
            municipios = session.execute(statement)
            municipios = pd.DataFrame(municipios).replace(np.nan, "")

            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio = municipio['name'].item()
            else:
                municipio = ''

            alertas_rfid = session.query(CassiaArchTrafficEvent).filter(
                CassiaArchTrafficEvent.closed_at == None,
                CassiaArchTrafficEvent.municipality == municipio
            ).all()
        alertas_rfid = pd.DataFrame([(
            r.severity,
        ) for r in alertas_rfid], columns=['severity'])
        """ print(alertas_rfid.to_string()) """
        alertas_rfid = alertas_rfid.groupby(
            ['severity'])['severity'].count().rename_axis('Severities').reset_index()
        alertas_rfid.rename(
            columns={'severity': 'Severities', 'Severities': 'severity'}, inplace=True)
        problems_count = pd.concat([data3, alertas_rfid], ignore_index=True)
        data3 = problems_count.groupby(
            ['severity']).sum().reset_index()

    statement4 = text(
        f"call sp_hostAvailPingLoss('{municipalityId}','{dispId}','')")
    hostAvailables = session.execute(statement4)
    print(statement4)
    data4 = pd.DataFrame(hostAvailables).replace(np.nan, "")
    """ dependientes = text(
        f"call sp_diagnostic_problemsD('{municipalityId}','{dispId}')")
    dependientes = pd.DataFrame(
        session.execute(dependientes)).replace(np.nan, '') """
    downs_filtro = await downs_count(municipalityId, dispId, subtype_id, session)
    if not data4.empty:
        print(data4)
        if data4['Down'][0] is None:
            downs_totales = 0
        else:
            downs_totales = int(data4['Down'][0])
        origenes = len(downs_filtro)
        data4['Downs_origen'] = origenes

    data2 = pd.DataFrame(corelations)
    data2 = data2.replace(np.nan, "")
    # aditional data
    subgroup_data = []

    statement6 = ""
    if subtype_id != "":
        statement6 = text(
            f"CALL sp_MetricViewH('{municipalityId}','{dispId}','{subtype_id}')")
    if switch_troughtput:
        statement6 = text(
            f"call sp_switchThroughtput('{municipalityId}','{switch_id}','{metric_switch_val}')")
        """ print(statement6) """
    if statement6 != "":
        subgroup_data = session.execute(statement6)
    data6 = pd.DataFrame(subgroup_data).replace(np.nan, "")

    global_host_available = text(
        f"call sp_hostAvailPingLoss('0','{dispId}','')")
    global_host_available = pd.DataFrame(
        session.execute(global_host_available))
    dependientes_global = text(f"call sp_diagnostic_problemsD('0','{dispId}')")
    dependientes_global = pd.DataFrame(
        session.execute(dependientes_global)).replace(np.nan, '')
    downs_global = await downs_count(0, dispId, '', session)
    if not global_host_available.empty:
        downs_totales = int(global_host_available['Down'][0])
        origenes = len(downs_global)
        global_host_available['Downs_origen'] = origenes
        """ global_host_available['Downs_dependientes'] = dependientes_conteo """
    """ print(global_host_available.to_string()) """

    session.close()
    response = {"hosts": data.to_dict(
        orient="records"), "relations": data2.to_dict(orient="records"),
        "problems_by_severity": data3.to_dict(orient="records"),
        "host_availables": data4.to_dict(orient="records", ),
        "subgroup_info": data6.to_dict(orient="records"),
        "global_host_availables": global_host_available.to_dict(orient="records")
    }
    # print(response)
    return success_response(data=response)


async def downs_count(municipalityId, dispId, subtype, session):
    severities = "6"
    if subtype == "0":
        subtype = ""

    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
    lpr_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "lpr_id").first()
    lpr_id = "1"
    if lpr_config:
        lpr_id = lpr_config.value
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    """ if tech_host_type == "11":
        tech_host_type = "11,2" """
    """ if subtype != "" and tech_host_type == "":
        tech_host_type = "0" """
    switch_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_id").first()
    switch_id = "12"

    if switch_config:
        switch_id = switch_config.value

    metric_switch_val = "Interface Bridge-Aggregation_: Bits"
    metric_switch = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_throughtput").first()
    if metric_switch:
        metric_switch_val = metric_switch.value
    if subtype == metric_switch_val:
        subtype = ""
    statement = text(
        f"call sp_viewProblem('{municipalityId}','{dispId}','{subtype}','')")

    problems = session.execute(statement)
    data = pd.DataFrame(problems).replace(np.nan, "")
    ping_loss_message = session.query(CassiaConfig).filter(
        CassiaConfig.name == "ping_loss_message").first()
    ping_loss_message = ping_loss_message.value if ping_loss_message else "Unavailable by ICMP ping"
    if not data.empty:
        data['tipo'] = [0 for i in range(len(data))]
        data.loc[data['Problem'] ==
                 ping_loss_message, 'tipo'] = 1
        data['local'] = [0 for i in range(len(data))]
        data['dependents'] = [0 for i in range(len(data))]
        data['alert_type'] = ["" for i in range(len(data))]

    downs_origen = text(
        f"""call sp_diagnostic_problems1('{municipalityId}','{dispId}')""")
    downs_origen = pd.DataFrame(session.execute(downs_origen))
    if not downs_origen.empty:
        """ data['tipo'] = [0 for i in range(len(data))]
        data.loc[data['hostid'].astype(int).isin(
            downs_origen['hostid'].tolist()), 'tipo'] = 1
        data['local'] = [0 for i in range(len(data))]
        data.loc[data['hostid'].astype(int).isin(
            downs_origen['hostid'].tolist()), 'local'] = 0
        data['dependents'] = [0 for i in range(len(data))] """
        data_problems = text(
            """select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_arch_traffic_events_2 cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_2 cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id 
where cate.closed_at is NULL and cate.hostid in :hostids""")
        """select cate.*,cdp.dependents  from cassia_arch_traffic_events cate
left join cassia_diagnostic_problems cdp on cdp.eventid=cate.cassia_arch_traffic_events_id 
where cate.closed_at is NULL and cate.hostid in :hostids """
        """ print(data_problems) """
        data_problems = pd.DataFrame(session.execute(
            data_problems, {'hostids': downs_origen['hostid'].tolist()})).replace(np.nan, 0)

        if not data_problems.empty:
            """ data_problems['TimeRecovery'] = [
                '' for i in range(len(data_problems))] """
            data_problems['r_eventid'] = [
                '' for i in range(len(data_problems))]
            data_problems['Ack'] = [0 for i in range(len(data_problems))]
            """ data_problems['Ack_message'] = [
                '' for i in range(len(data_problems))] """
            data_problems['manual_close'] = [
                0 for i in range(len(data_problems))]
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
    dependientes_filtro = text(
        f"call sp_diagnostic_problemsD('{municipalityId}','{dispId}')")
    dependientes_filtro = pd.DataFrame(
        session.execute(dependientes_filtro)).replace(np.nan, '')
    """ host = dependientes_filtro[dependientes_filtro['hostid'] == 16143]
        print(host.to_string())
        print(dependientes_filtro) """
    if not dependientes_filtro.empty:
        indexes = data[data['Problem'] == ping_loss_message]
        indexes = indexes[indexes['hostid'].isin(
            dependientes_filtro['hostid'].to_list())]
        data.loc[data.index.isin(indexes.index.to_list()), 'tipo'] = 0

    sincronizados_totales = text("""select * from cassia_diagnostic_problems_2 cdp 
where cdp.closed_at is NULL""")

    sincronizados_totales = pd.DataFrame(
        session.execute(sincronizados_totales)).replace(np.nan, 0)
    if not sincronizados_totales.empty:
        if not data.empty:
            for ind in data.index:
                if data['Problem'][ind] == ping_loss_message:
                    dependientes = sincronizados_totales[sincronizados_totales['hostid_origen']
                                                         == data['hostid'][ind]]
                    """ print(dependientes) """
                    dependientes['depends_hostid'] = dependientes['depends_hostid'].astype(
                        'int')
                    dependientes = dependientes[dependientes['depends_hostid'] != 0]
                    dependientes = dependientes.drop_duplicates(
                        subset=['depends_hostid'])
                    data.loc[data.index == ind,
                             'dependents'] = len(dependientes)

    if not data.empty:
        now = datetime.now(pytz.timezone('America/Mexico_City'))
        data['fecha'] = pd.to_datetime(data['Time'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
            pytz.timezone('America/Mexico_City'))
        data['diferencia'] = now-data['fecha']
        data['dias'] = data['diferencia'].dt.days
        data['horas'] = data['diferencia'].dt.components.hours
        data['minutos'] = data['diferencia'].dt.components.minutes
        """ print(data['diferencia']) """
        data.loc[data['alert_type'].isin(
            ['rfid', 'lpr']), 'Problem'] = data.loc[data['alert_type'].isin(['rfid', 'lpr']), ['dias', 'horas', 'minutos']].apply(lambda x:
                                                                                                                                  f"Este host no ha tenido lecturas por más de {x['dias']} dias {x['horas']} hrs {x['minutos']} min" if x['dias'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['horas']} hrs {x['minutos']} min" if x['horas'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['minutos']} min", axis=1)
        data = data.drop(columns=['diferencia'])
        data['diferencia'] = data.apply(
            lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
        data.drop_duplicates(
            subset=['hostid', 'Problem'], inplace=True)
        """ print(data.to_string()) """

        """ data['Problem'] = data.apply(lambda x: x['diferencia'] if x['alert_type'] in [
                                     'rfid', 'lpr'] else x['Problem']) """
    if not data.empty:
        data = data[data['Estatus'] == 'PROBLEM']
        if not data.empty:
            data = data[data['tipo'] == 1]
    return data

