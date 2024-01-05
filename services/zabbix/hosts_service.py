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
import socket

from fastapi.exceptions import HTTPException
from fastapi import status
from services.cassia.configurations_service import get_configuration
import json
import re
import time
from pythonping import ping

settings = Settings()


def get_host_filter(municipalityId, dispId, subtype_id):
    if subtype_id == "0":
        subtype_id = ""
    if dispId == "0":
        dispId = ""

    print(subtype_id)

    print(type(subtype_id))
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    switch_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_id").first()
    switch_id = "12"
    switch_troughtput = False
    if switch_config:
        switch_id = switch_config.value

    metric_switch_val = "Interface Bridge-Aggregation_: Bits"
    metric_switch = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_throughtput").first()
    if metric_switch:
        metric_switch_val = metric_switch.value
    if subtype_id == metric_switch_val:
        subtype_id = ""

        if dispId == switch_id:
            switch_troughtput = True

    """ if subtype_id == "376276" or subtype_id == "375090":
        subtype_host_filter = '376276,375090'
    else:
        subtype_host_filter = subtype_id """
    """ arcos_band = False """
    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
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
        print(alertas_rfid.to_string())
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
    data4 = pd.DataFrame(hostAvailables).replace(np.nan, "")
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
        print(statement6)
    if statement6 != "":
        subgroup_data = session.execute(statement6)
    data6 = pd.DataFrame(subgroup_data).replace(np.nan, "")

    global_host_available = text(
        f"call sp_hostAvailPingLoss('0','{dispId}','')")
    global_host_available = pd.DataFrame(
        session.execute(global_host_available))
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


def get_host_correlation_filter(host_group_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
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
        {host_group_id} in (SELECT groupid from hosts_groups hg where hg.hostid=hc.hostidP)
        or {host_group_id} in (SELECT groupid from hosts_groups hg where hg.hostid=hc.hostidC)
        )
        """
    )
    corelations = session.execute(statement)
    session.close()
    data = pd.DataFrame(corelations)
    data = data.replace(np.nan, "")
    return success_response(data=data.to_dict(orient="records"))


async def get_host_metrics(host_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select DISTINCT i.templateid,i.itemid  from items i where hostid={host_id} AND i.templateid IS NOT NULL")
    template_ids = session.execute(statement)
    template_ids = pd.DataFrame(template_ids).replace(np.nan, "")

    if len(template_ids) > 1:

        template_ids = ",".join(
            map(str, template_ids['templateid'].values.tolist()))

    else:
        if len(template_ids) == 1:

            template_ids = f"{template_ids['templateid'][0]}"
        else:
            template_ids = "0"

    statement = text(f"""
call sp_hostHealt({host_id},'{template_ids}');
""")
    print(statement)

    metrics = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    session.close()
    return success_response(data=metrics.to_dict(orient="records"))


async def get_host_alerts(host_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"""
SELECT from_unixtime(p.clock,'%d/%m/%Y %H:%i:%s' ) as Time,
	p.severity,h.hostid,h.name Host,hi.location_lat as latitude,hi.location_lon as longitude,
	it.ip,p.name Problem, IF(ISNULL(p.r_eventid),'PROBLEM','RESOLVED') Estatus, p.eventid,p.r_eventid,
	IF(p.r_clock=0,'',From_unixtime(p.r_clock,'%d/%m/%Y %H:%i:%s' ) )'TimeRecovery',
	p.acknowledged Ack,IFNULL(a.Message,'''') AS Ack_message FROM hosts h	
	INNER JOIN host_inventory hi ON (h.hostid=hi.hostid)
	INNER JOIN interface it ON (h.hostid=it.hostid)
	INNER JOIN items i ON (h.hostid=i.hostid)
	INNER JOIN functions f ON (i.itemid=f.itemid)
	INNER JOIN triggers t ON (f.triggerid=t.triggerid)
	INNER JOIN problem p ON (t.triggerid = p.objectid)
	LEFT JOIN acknowledges a ON (p.eventid=a.eventid)
	WHERE  h.hostid={host_id}
	ORDER BY p.clock  desc 
    limit 20
""")
    alerts = session.execute(statement)
    alerts = pd.DataFrame(alerts).replace(np.nan, "")
    alertas_rfid = session.query(CassiaArchTrafficEvent).filter(
        CassiaArchTrafficEvent.closed_at == None,
        CassiaArchTrafficEvent.hostid == host_id
    ).all()
    alertas_rfid = pd.DataFrame([(
        r.created_at,
        r.severity,
        r.hostid,
        r.hostname,
        r.latitude,
        r.longitude,
        r.ip,
        r.message,
        r.status,
        r.cassia_arch_traffic_events_id,
        '',
        '',
        0,
        ''
    )
        for r in alertas_rfid], columns=['Time', 'severity', 'hostid',
                                         'Host', 'latitude', 'longitude',
                                         'ip',
                                         'Problem', 'Estatus',
                                         'eventid',
                                         'r_eventid',
                                         'TimeRecovery',
                                         'Ack',
                                         'Ack_message'])
    data = pd.concat([alertas_rfid, alerts],
                     ignore_index=True).replace(np.nan, "")

    session.close()
    return success_response(data=data.to_dict(orient="records"))


async def get_host_arcos(host_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"""
SELECT  DISTINCT h.hostid,h.name Host,hi.location_lat as latitude,hi.location_lon as longitude,
	it.ip FROM hosts h	
	INNER JOIN host_inventory hi ON (h.hostid=hi.hostid)
	INNER JOIN interface it ON (h.hostid=it.hostid)
	INNER JOIN items i ON (h.hostid=i.hostid)
	WHERE h.hostid ={host_id}
""")
    host = session.execute(statement)
    host = pd.DataFrame(host).replace(np.nan, "")
    session.close()
    if len(host) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Host id not exist"
        )
    statement = text(f"""
SELECT m.Nombre as Municipio, a.Nombre as Arco, r.Descripcion,
r.Estado, a2.UltimaLectura,
ISNULL(cl.lecturas,0)  as Lecturas,
a.Longitud,a.Latitud,
a2.Carril,
r.Ip 
FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
where r.Ip = '{host["ip"].values[0]}'
order by a.Longitud,a.Latitud
""")
    db_c5 = DB_C5()
    session_c5 = db_c5.Session()
    arcos = session_c5.execute(statement)
    arcos = pd.DataFrame(arcos).replace(np.nan, "")
    data = pd.DataFrame()
    if len(arcos) > 0:
        data = pd.merge(host, arcos, left_on="ip", right_on="Ip")

    session_c5.close()
    return success_response(data=data.to_dict(orient="records"))


def encrypt(plaintext, key):
    fernet = Fernet(key)
    return fernet.encrypt(plaintext.encode())


def decrypt(encriptedText, key):
    fernet = Fernet(key)
    return fernet.decrypt(encriptedText.encode())


def get_info_actions(ip):
    response = None
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_GetInfoAccion('{ip}')")
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    action_ping_by_default = {
        "hostid": None,
        "host": "",
        "ip": ip,
        "interfaceid": None,
        "name": "Ping",
        "protocol": "ssh",
        "action_id": -1
    }
    response = {"actions": [action_ping_by_default]}
    # Eliminar la columna 'comand'
    if 'comand' in data.columns:
        data = data.drop(columns=['comand'])
    session.close()

    if not data.empty:
        response['actions'].extend(data.to_dict(orient='records'))

    return success_response(data=response)


def get_credentials(ip):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_getCredentials('{ip}')")
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    session.close()
    return data.to_dict(orient="records")


def prepare_action(ip, id_action):
    if id_action == -1:
        response = get_configuration()
        try:
            # Utiliza el método json() de tu objeto JSONResponse
            data = json.loads(response.body)
            # Verifica que la respuesta sea valida y contiene el campo 'data'
            if 'data' in data and isinstance(data['data'], list):
                # Itera sobre la lista de diccionarios en 'data'
                for config in data['data']:
                    # Busca el diccionario con el nombre 'ping_by_proxy'
                    if config.get('name') == 'ping_by_proxy':
                        # Obtiene el valor asociado con 'ping_by_proxy'
                        value = config.get('value')
                        if value:
                            dict_credentials_list = get_credentials_for_proxy(
                                ip)
                        else:
                            dict_credentials_list = get_credentials(ip)
                        if dict_credentials_list is None or not dict_credentials_list:
                            return failure_response(message="Credenciales no encontradas")
                        else:
                            dict_credentials = dict_credentials_list[0]
                            return run_action(dict_credentials['ip'], 'ping -c 4 ' + ip,
                                              dict_credentials_list)
        except json.JSONDecodeError:
            print("Error decoding JSON response")

    else:
        db_zabbix = DB_Zabbix()
        session = db_zabbix.Session()
        action = session.query(CassiaActionModel).filter(
            CassiaActionModel.action_id == id_action).first()
        session.close()
        if action is None:
            return failure_response(message="ID acción necesaria")
        dict_credentials_list = get_credentials(ip)
        if dict_credentials_list is None or not dict_credentials_list:
            return failure_response(message="Credenciales no encontradas")
        return run_action(ip, action.comand, get_credentials(ip))


def run_action(ip, command, dict_credentials_list):
    dict_credentials = dict_credentials_list[0]
    ssh_host = ip
    ssh_user = decrypt(dict_credentials['usr'], settings.ssh_key_gen)
    ssh_pass = decrypt(dict_credentials['psswrd'], settings.ssh_key_gen)
    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy())

    data = {
        "action": "false",
        "ip": ip
    }

    try:
        # Check if the command contains the word "ping"
        if "ping" in command:
            # Extract the IP address from the ping command
            ping_ip = extract_ip_from_ping_command(command)
            if ping_ip:
                data['ip'] = ping_ip
            else:
                print("Unable to extract IP address from the ping command.")
                return failure_response(message="Comando de ping invalido")

        ssh_client.connect(
            hostname=ssh_host, username=ssh_user.decode(), password=ssh_pass.decode())

        _stdin, _stdout, _stderr = ssh_client.exec_command(command)
        error_lines = _stderr.readlines()

        if not error_lines:
            data['action'] = 'true'
            if "reboot" in command:
                # Esperar al servidor que este offline
                timeout_offline = 120000  # Ajustar el timeout
                start_time_offline = time.time()
                while True:
                    try:
                        response = ping(data['ip'], count=1)
                        if not response.success():
                            offline_time = time.time() - start_time_offline
                            print(f"Accion reboot - el servidor esta offline. Tiempo fuera de linea: {offline_time} segundos.")
                            break
                    except Exception as e:
                        print(f"Error durante ping: {str(e)}")

                    if time.time() - start_time_offline > timeout_offline:
                        print("Tiempo fuera de linea agotado.")
                        return failure_response(message="Verificación de reboot tiempo de espera agotado")

                    time.sleep(10)  # Ajustar el intervalo entre intentos de ping

                # Esperar a que el servidor se encuentre online
                timeout_online = 120000  # Ajustar el timeout
                start_time_online = time.time()
                while True:
                    try:
                        response = ping(data['ip'], count=1)
                        if response.success():
                            online_time = time.time() - start_time_online
                            print(f"Servidor esta en linea de nuevo. Tiempo online: {online_time} segundos.")
                            data['total_time'] = offline_time + online_time
                            break
                    except Exception as e:
                        print(f"Error durante el ping: {str(e)}")

                    if time.time() - start_time_online > timeout_online:
                        print("Timeout alcanzado. El servidor no volvio a estar online.")
                        return failure_response(message="Verificación de reboot tiempo de espera agotado")

                    time.sleep(10)  # Ajustar el intervalo entre intentos de ping

            return success_response(data=data)
        else:
            error_msg = " ".join(error_lines)
            print(f"Problema de conexión al servidor. Detalles: {error_msg}")
            return failure_response(message=f"Problema de conexión al servidor",
                                    recommendation="revisar que tenga conexión estable ala dirección del servidor")

    except BadHostKeyException as e:

        error_msg = f"Error de clave de host"
        print(f"Error de clave de host: {str(e)}")
        return failure_response(message=error_msg)

    except AuthenticationException as e:

        error_msg = "Error de autenticación"
        print(f"Error de autenticación: {str(e)}")
        return failure_response(message=error_msg,
                                recommendation="revisar que estén correctas las credenciales ssh")

    except SSHException as e:

        error_msg = "Problema de conexión por SSH"
        print(f"Problema de conexión por SSH: {str(e)}")
        return failure_response(message=error_msg)

    except socket.error as e:

        error_msg = "Error de conexión de socket"
        print(f"Error de conexión de socket: {str(e)}")
        return failure_response(message=error_msg)

    finally:

        ssh_client.close()


def get_credentials_for_proxy(ip):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_proxy_credential('{ip}')")
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    session.close()
    return data.to_dict(orient="records")


def extract_ip_from_ping_command(command):
    # Use regular expression to find the IP address in the ping command
    match = re.search(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', command)
    if match:
        return match.group()
    else:
        return None
