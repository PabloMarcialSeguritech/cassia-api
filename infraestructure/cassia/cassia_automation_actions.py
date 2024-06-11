from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
from sqlalchemy import text
from utils.traits import success_response, failure_response
import numpy as np
from paramiko import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import AuthenticationException, BadHostKeyException, SSHException
from cryptography.fernet import Fernet
from models.cassia_actions import CassiaAction as CassiaActionModel
from models.cassia_action_log import CassiaActionLog
import socket
from datetime import datetime
import pytz
from fastapi.exceptions import HTTPException
from fastapi import status
from infraestructure.db_queries_model import DBQueries
import re
import time
from pythonping import ping
from services.zabbix import hosts_service_
from utils.settings import Settings
from utils.db import DB_Zabbix
from models.cassia_config import CassiaConfig
from infraestructure.cassia import CassiaConfigRepository
settings = Settings()


async def get_hosts_actions_to_process():
    db_model = DB()
    try:
        query_get_hosts_actions_to_process = DBQueries(
        ).query_get_hosts_actions_to_process
        await db_model.start_connection()
        host_actions_to_process_data = await db_model.run_query(query_get_hosts_actions_to_process)
        host_actions_to_process_df = pd.DataFrame(host_actions_to_process_data)
        return host_actions_to_process_df

    except Exception as e:
        print(f"Excepcion en get_hosts_actions_to_process: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_hosts_actions_to_process: {e}")
    finally:
        await db_model.close_connection()


async def get_auto_action_conditions(condition_id):
    db_model = DB()
    try:
        query_get_auto_action_conditions = DBQueries(
        ).builder_query_get_auto_action_conditions(condition_id)
        await db_model.start_connection()
        host_auto_action_conditions_data = await db_model.run_query(query_get_auto_action_conditions)
        host_auto_action_conditions_df = pd.DataFrame(
            host_auto_action_conditions_data)
        return host_auto_action_conditions_df

    except Exception as e:
        print(f"Excepcion en get_auto_action_conditions: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_auto_action_conditions: {e}")
    finally:
        await db_model.close_connection()


async def get_auto_action_operational_values(interface_id,  action_auto_id):
    db_model = DB()
    try:
        query_get_auto_action_operational_values = DBQueries(
        ).builder_query_get_auto_action_operational_values(interface_id, action_auto_id)
        await db_model.start_connection()
        action_operational_values_data = await db_model.run_query(query_get_auto_action_operational_values)
        action_operational_values_df = pd.DataFrame(
            action_operational_values_data)
        return action_operational_values_df

    except Exception as e:
        print(f"Excepcion en get_auto_action_operational_values: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_auto_action_operational_values: {e}")
    finally:
        await db_model.close_connection()


async def insert_auto_action_operational_values(values):
    db_model = DB()
    try:
        query_insert_auto_action_operational_values = DBQueries(
        ).builder_query_insert_action_auto_operational_values(values)
        print(query_insert_auto_action_operational_values)
        await db_model.start_connection()
        insert_values = await db_model.run_query(query_insert_auto_action_operational_values)
        return True

    except Exception as e:
        print(f"Excepcion en insert_auto_action_operational_values: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en insert_auto_action_operational_values: {e}")
    finally:
        await db_model.close_connection()


async def update_auto_action_operational_values(auto_operation_id, last_value, updated_at):
    db_model = DB()
    try:
        query_update_auto_action_operational_values = DBQueries(
        ).builder_query_update_action_auto_operational_values(auto_operation_id, last_value, updated_at)
        print(query_update_auto_action_operational_values)
        await db_model.start_connection()
        update_values = await db_model.run_query(query_update_auto_action_operational_values)
        return True
    except Exception as e:
        print(f"Excepcion en update_auto_action_operational_values: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_auto_action_operational_values: {e}")
    finally:
        await db_model.close_connection()


async def close_auto_action_operational_values(auto_operation_ids, closed_at):
    db_model = DB()
    try:
        query_close_auto_action_operational_values = DBQueries(
        ).builder_query_close_action_auto_operational_values(auto_operation_ids, closed_at)
        print(query_close_auto_action_operational_values)
        await db_model.start_connection()
        closed_values = await db_model.run_query(query_close_auto_action_operational_values)
        return True
    except Exception as e:
        print(f"Excepcion en close_auto_action_operational_values: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en close_auto_action_operational_values: {e}")
    finally:
        await db_model.close_connection()


async def get_auto_action_operational_values_to_process():
    db_model = DB()
    try:
        query_get_auto_action_operational_values_to_process = DBQueries(
        ).query_get_cassia_auto_operational_values_to_process
        await db_model.start_connection()
        action_operational_values_to_process_data = await db_model.run_query(query_get_auto_action_operational_values_to_process)
        action_operational_values_to_process_df = pd.DataFrame(
            action_operational_values_to_process_data)
        return action_operational_values_to_process_df

    except Exception as e:
        print(
            f"Excepcion en get_auto_action_operational_values_to_process: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_auto_action_operational_values_to_process: {e}")
    finally:
        await db_model.close_connection()


async def get_credentials_df(ip):
    db_model = DB()
    try:
        stored_name_get_credentials = DBQueries(
        ).stored_name_get_credentials
        await db_model.start_connection()
        print(ip)
        get_credentials_data = await db_model.run_stored_procedure(stored_name_get_credentials, (f"{ip}",))
        get_credentials_df = pd.DataFrame(
            get_credentials_data).replace(np.nan, "")
        return get_credentials_df

    except Exception as e:
        print(
            f"Excepcion en get_auto_action_operational_values_to_process: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_auto_action_operational_values_to_process: {e}")
    finally:
        await db_model.close_connection()


# PROCESOS PARA CORRER LAS ACCIONES

async def get_credentials(ip):
    data = await get_credentials_df(ip)
    return data.to_dict(orient="records")


async def prepare_action(ip, id_action):
    print("aqui se esta ejecutando")
    ping_id = await CassiaConfigRepository.get_config_value_by_name("ping_id")
    if ping_id.empty:
        ping_id = 0
    else:
        ping_id = int(ping_id['value'][0])
    """ with DB_Zabbix().Session() as session:
        ping_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == 'ping_id').first()
        if ping_id:
            ping_id = int(ping_id.value)
        else:
            ping_id = 0 """

    if id_action == ping_id:
        return await hosts_service_.run_action_(ip, id_action, '')
    else:
        db_zabbix = DB_Zabbix()
        session = db_zabbix.Session()
        action = session.query(CassiaActionModel).filter(
            CassiaActionModel.action_id == id_action).first()
        session.close()
        if action is None:
            return failure_response(message="ID acción necesaria")
        dict_credentials_list = await get_credentials(ip)
        print(dict_credentials_list)
        if dict_credentials_list is None or not dict_credentials_list:
            with DB_Zabbix().Session() as session:
                log = {'action_id': id_action,
                       'clock': datetime.now(pytz.timezone('America/Mexico_City')),
                       'session_id': None,
                       'interface_id': None,
                       'result': 0,
                       'comments': f"Credenciales no encontradas para la ip {ip}"}
                log_register = CassiaActionLog(
                    **log
                )
                session.add(log_register)
                session.commit()
            return failure_response(message="Credenciales no encontradas")

        return run_action(ip, action.comand, dict_credentials_list, action.verification_id, action.action_id)


def run_action(ip, command, dict_credentials_list, verification_id, action_id):
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

    with DB_Zabbix().Session() as session:
        try:
            log = {'action_id': action_id,
                   'clock': datetime.now(pytz.timezone('America/Mexico_City')),
                   'session_id': None,
                   'interface_id': dict_credentials['interfaceid'] if (
                       'interfaceid' in dict_credentials) else 1,
                   'result': 0,
                   'comments': None}

            # Check if the command contains the word "ping"
            if "ping" in command:
                # Extract the IP address from the ping command
                ping_ip = extract_ip_from_ping_command(command)
                if ping_ip:
                    data['ip'] = ping_ip
                else:
                    log['result'] = 0
                    log['comments'] = f'Unable to extract IP address from the ping command.'
                    action_log = CassiaActionLog(**log)
                    session.add(action_log)
                    session.commit()
                    print("Unable to extract IP address from the ping command.")
                    return failure_response(message="Comando de ping invalido")

            ssh_client.connect(
                hostname=ssh_host, username=ssh_user.decode(), password=ssh_pass.decode())

            _stdin, _stdout, _stderr = ssh_client.exec_command(command)

            error_lines = _stderr.readlines()
            out = _stdout

            if not error_lines:
                data['action'] = 'true'
                log['result'] = 1
                if "reboot" in command or "shutdown /r /f /t 0" in command:

                    # Esperar al servidor que este offline
                    timeout_offline = 120000  # Ajustar el timeout
                    start_time_offline = time.time()
                    while True:
                        try:
                            response = ping(data['ip'], count=1)
                            if not response.success():
                                offline_time = time.time() - start_time_offline
                                print(
                                    f"Accion reboot - el servidor esta offline. Tiempo fuera de linea: {offline_time} segundos.")
                                break
                        except Exception as e:
                            print(f"Error durante ping: {str(e)}")

                        if time.time() - start_time_offline > timeout_offline:
                            print("Tiempo fuera de linea agotado.")
                            log['result'] = 0
                            log['comments'] = f'Tiempo fuera de linea agotado.'
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message="Verificación de reboot tiempo de espera agotado")

                        # Ajustar el intervalo entre intentos de ping
                        time.sleep(10)

                    # Esperar a que el servidor se encuentre online
                    timeout_online = 120000  # Ajustar el timeout
                    start_time_online = time.time()
                    while True:
                        try:
                            response = ping(data['ip'], count=1)
                            if response.success():
                                online_time = time.time() - start_time_online
                                print(
                                    f"Servidor esta en linea de nuevo. Tiempo online: {online_time} segundos.")
                                data['total_time'] = offline_time + online_time
                                break
                        except Exception as e:
                            print(f"Error durante el ping: {str(e)}")

                        if time.time() - start_time_online > timeout_online:
                            print(
                                "Timeout alcanzado. El servidor no volvio a estar online.")
                            log['result'] = 0
                            log['comments'] = f'Timeout alcanzado. El servidor no volvio a estar online.'
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message="Verificación de reboot tiempo de espera agotado")

                        # Ajustar el intervalo entre intentos de ping
                        time.sleep(10)

                match verification_id:
                    case 0:
                        output_ping = _stdout.read().decode(encoding="utf-8")
                        statistics_match = re.search(
                            r'(\d+ packets transmitted, \d+ received, 100% packet loss, time \d+ms)',
                            output_ping)
                        if statistics_match:
                            data['action'] = 'false'
                            log['result'] = 0
                            log['comments'] = "El ping no tuvo respuesta (100% de pérdida de paquetes)"
                    case 3:
                        data['result'] = get_status(_stdout.read().decode())
                    case 4:
                        data['result'] = check_status(
                            command, ssh_client, 'Started', 'iniciado')
                    case 5:
                        data['result'] = check_status(
                            command, ssh_client, 'Stopped', 'parado')
                    case 6:
                        data['result'] = check_status(
                            command, ssh_client, 'Started', 'reiniciado')
                    case 7:
                        result_response = check_status_sql_server(
                            _stdout, ssh_client)
                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = f'Servicio no instalado o servidor no disponible'
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=f"Problema de conexión al servidor",
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 8:
                        service_name = _stdout.read().decode(encoding="utf-8")
                        result_response = start_stop_sql_server_windows(
                            service_name, ssh_client, 'start', 'RUNNING',
                            "Error al iniciar el servicio. Tiempo limite de espera excedido.")

                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 9:
                        service_name = _stdout.read().decode(encoding="utf-8")
                        result_response = start_stop_sql_server_windows(
                            service_name, ssh_client, 'stop', 'STOPPED',
                            "Error al detener el servicio. Tiempo limite de espera excedido.")

                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 10:
                        service_name = _stdout.read().decode(encoding="utf-8")
                        result_response_stop = start_stop_sql_server_windows(
                            service_name, ssh_client, 'stop', 'STOPPED',
                            "Error al detener el servicio. Tiempo limite de espera excedido.")
                        if not result_response_stop['status']:
                            log['result'] = 0
                            log['comments'] = result_response_stop['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_stop['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        result_response_start = start_stop_sql_server_windows(
                            service_name, ssh_client, 'start', 'RUNNING',
                            "Error al iniciar el servicio. Tiempo limite de espera excedido.")
                        if not result_response_start['status']:
                            log['result'] = 0
                            log['comments'] = result_response_start['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_start['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        data['result'] = "Servicio reiniciado correctamente"
                    case 11:
                        data['result'] = get_status_windows(
                            _stdout.read().decode('utf-8'))
                    case 12:
                        result_response = check_start_stop_windows_service(
                            'GenetecServer', ssh_client, 'start', 'RUNNING',
                            'Error al iniciar el servicio. Tiempo limite de espera excedido.')
                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 13:
                        result_response = check_start_stop_windows_service(
                            'GenetecServer', ssh_client, 'stop', 'STOPPED',
                            'Error al detener el servicio. Tiempo limite de espera excedido.')
                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 14:
                        result_response_stop = start_stop_windows_service(
                            "GenetecServer", ssh_client, 'stop', 'STOPPED',
                            "Error al detener el servicio. Tiempo limite de espera excedido.")
                        if not result_response_stop['status']:
                            log['result'] = 0
                            log['comments'] = result_response_stop['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_stop['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        result_response_start = start_stop_windows_service(
                            "GenetecServer", ssh_client, 'start', 'RUNNING',
                            "Error al iniciar el servicio. Tiempo limite de espera excedido.")
                        if not result_response_start['status']:
                            log['result'] = 0
                            log['comments'] = result_response_start['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_start['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        data['result'] = "Servicio reiniciado correctamente"

                action_log = CassiaActionLog(**log)
                session.add(action_log)
                session.commit()
                return success_response(data=data)
            else:
                error_msg = " ".join(error_lines)
                if "service could not be found" in error_msg:
                    return failure_response(message=f"Servicio no encontrado",
                                            recommendation="revisar que el servidor tenga disponible el servicio")
                print(
                    f"Problema de conexión al servidor. Detalles: {error_msg}")
                log['comments'] = error_msg
                action_log = CassiaActionLog(**log)
                session.add(action_log)
                session.commit()
                return failure_response(message=f"Problema de conexión al servidor",
                                        recommendation="revisar que tenga conexión estable ala dirección del servidor")

        except BadHostKeyException as e:

            error_msg = f"Error de clave de host"
            print(f"Error de clave de host: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg)

        except AuthenticationException as e:

            error_msg = "Error de autenticación"
            print(f"Error de autenticación: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg,
                                    recommendation="revisar que estén correctas las credenciales ssh")

        except SSHException as e:

            error_msg = "Problema de conexión por SSH"
            print(f"Problema de conexión por SSH: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg)

        except socket.error as e:

            error_msg = "Error de conexión de socket"
            print(f"Error de conexión de socket: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg)
        except Exception as e:
            print(e)
            log['comments'] = f'Error desconocido: {e}'
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=e)
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


def check_status(command, ssh_client, command_verification, accion):
    service = command.split()
    service = service[-1]
    _stdin, _stdout, _stderr = ssh_client.exec_command(
        f"systemctl status {service}")
    result_status = _stdout.read().decode()
    result_status = result_status.splitlines()
    result_status = result_status[-1]
    if command_verification in result_status:
        return f"Servicio {service} {accion} correctamente"
    else:
        return "Accion no realizada correctamente"


def get_status(cadena):
    lineas = cadena.splitlines()
    result = ""
    for linea in lineas:
        if "Active" in linea:
            if "running" in linea:
                result = "Servicio activo (El servicio está en ejecución y funcionando normalmente)"
            if "inactive" in linea:
                result = "Servicio inactivo (El servicio no está en ejecución y no se puede iniciar)"
            if "activating" in linea:
                result = "Activando servicio (El servicio está en proceso de iniciar)"
            if "deactivating" in linea:
                result = "Desactivando servicio (El servicio está en proceso de detenerse)"
            if "failed" in linea:
                result = "Fallo en el servicio (El servicio ha fallado al iniciar o ha sido detenido debido a un error)"
            if "exited" in linea:
                result = "Servicio finalizado (El servicio se ha detenido de manera normal después de haber estado en ejecución)"
            if "listening" in linea:
                result = "Escuchando servicio (El servicio está escuchando y esperando conexiones)"
            if "plugged" in linea:
                result = "Servicio enchufado (Indica que el servicio está enchufado en el socket de activación)"
    return result


def get_status_windows(cadena):
    lineas = cadena.splitlines()
    result = ""
    for linea in lineas:
        if "STATE" in linea:
            if "RUNNING" in linea:
                result = "Servicio activo (El servicio está en ejecución)"
            if "STOPPED" in linea:
                result = "Servicio inactivo (El servicio no está en ejecución)"
            if "PAUSED" in linea:
                result = "El servicio está pausado."
            if "START_PENDING" in linea:
                result = "El servicio está en proceso de inicio"
            if "STOP_PENDING" in linea:
                result = "El servicio está en proceso de detención"
            if "CONTINUE_PENDING" in linea:
                result = "El servicio está en proceso de reanudación después de estar pausado."
            if "PAUSE_PENDING" in linea:
                result = "El servicio está en proceso de pausa."
    return result


def check_status_sql_server(out, ssh_client):
    service_name = out.read().decode(encoding="utf-8")
    result_response = {'status': 0, 'result': '', 'message_error': ''}
    if len(service_name) <= 0:
        result_response["message_error"] = 'El servicio no existe en este host'
        return result_response
    service_name = service_name[14:]

    comando = f"sc query {service_name}"
    _stdin, _stdout, _stderr = ssh_client.exec_command(comando)
    error_lines = _stderr.readlines()
    """ sc query type= service state= all | findstr /C:"MSSQL" """
    if not error_lines:
        result_response['status'] = 1
        result = _stdout.read().decode(encoding="utf-8")
        result = result.encode().decode(
            'unicode_escape')
        result = get_status_windows(result)
        result_response['result'] = result
        return result_response
    else:
        error_msg = " ".join(error_lines)
        result_response['message_error'] = error_msg
        print(
            f"Problema de conexión al servidor. Detalles: {error_msg}")
        return result_response


def start_stop_sql_server_windows(service_name, ssh_client, comand_st, verification_status, error_message):
    result_response = {'status': 0, 'result': '', 'message_error': ''}
    if len(service_name) <= 0:
        result_response["message_error"] = 'El servicio no existe en este host'
        return result_response
    service_name = service_name[14:]

    comando = f"sc {comand_st} {service_name}"
    _stdin, _stdout, _stderr = ssh_client.exec_command(comando)

    error_lines = _stderr.readlines()

    if not error_lines:

        cont = 0
        while True:

            comand = f"sc query {service_name}"

            _stdin, _stdout, _stderr = ssh_client.exec_command(comand)
            out = _stdout.read().decode(encoding="utf-8")

            status = check_status_windows(
                out, verification_status)

            if status:
                break
            time.sleep(2)
            cont += 1
            if cont > 120:
                result_response['status'] = 0
                result_response['message_error'] = error_message
                return result_response

        result_response['status'] = 1

        match comand_st:
            case 'stop':

                result_response['result'] = "El servicio se ha detenido correctamente"
            case 'start':
                result_response['result'] = "El servicio se ha iniciado correctamente"

        return result_response
    else:
        error_msg = " ".join(error_lines)
        result_response['message_error'] = error_msg
        print(
            f"Problema de conexión al servidor. Detalles: {error_msg}")
        return result_response


def start_stop_windows_service(service_name, ssh_client, comand_st, verification_status, error_message):
    result_response = {'status': 0, 'result': '', 'message_error': ''}

    comando = f"sc {comand_st} {service_name}"
    _stdin, _stdout, _stderr = ssh_client.exec_command(comando)

    error_lines = _stderr.readlines()

    if not error_lines:

        cont = 0
        while True:

            comand = f"sc query {service_name}"

            _stdin, _stdout, _stderr = ssh_client.exec_command(comand)
            out = _stdout.read().decode(encoding="utf-8")

            status = check_status_windows(
                out, verification_status)

            if status:
                break
            time.sleep(2)
            cont += 1
            if cont > 120:
                result_response['status'] = 0
                result_response['message_error'] = error_message
                return result_response

        result_response['status'] = 1

        match comand_st:
            case 'stop':

                result_response['result'] = "El servicio se ha detenido correctamente"
            case 'start':
                result_response['result'] = "El servicio se ha iniciado correctamente"

        return result_response
    else:
        error_msg = " ".join(error_lines)
        result_response['message_error'] = error_msg
        print(
            f"Problema de conexión al servidor. Detalles: {error_msg}")
        return result_response


def check_status_windows(cadena, status):
    lineas = cadena.splitlines()
    result = False
    for linea in lineas:
        if "STATE" in linea:
            if status in linea:
                return True
    return result


def check_start_stop_windows_service(service_name, ssh_client, comand_st, verification_status, error_message):
    result_response = {'status': 0, 'result': '', 'message_error': ''}
    cont = 0
    while True:
        comand = f"sc query {service_name}"
        _stdin, _stdout, _stderr = ssh_client.exec_command(comand)
        status = check_status_windows(
            _stdout.read().decode(encoding="utf-8"), verification_status)
        if status:
            break
        time.sleep(2)
        cont += 1
        error_lines = _stderr.readlines()
        if cont > 120:
            result_response['status'] = 0
            result_response['message_error'] = error_message
            return result_response
        if error_lines:
            result_response['status'] = 0
            result_response['message_error'] = error_lines
            return result_response

    result_response['status'] = 1

    match comand_st:
        case 'stop':
            result_response['result'] = "El servicio se ha detenido correctamente"
        case 'start':
            result_response['result'] = "El servicio se ha iniciado correctamente"

    return result_response


def encrypt(plaintext, key):
    fernet = Fernet(key)
    return fernet.encrypt(plaintext.encode())


def decrypt(encriptedText, key):
    fernet = Fernet(key)
    return fernet.decrypt(encriptedText.encode())
