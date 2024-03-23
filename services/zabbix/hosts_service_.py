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
        raise # Re-lanza la excepción para que el llamador pueda manejarla adecuadamente


async def async_ping_by_proxy(ip, ssh_host, ssh_user, ssh_pass):
    # Si hace el ping retorna 1 sino retorna 0
    success_ping = False
    message = ""
    try:

        async with asyncssh.connect(ssh_host, username=ssh_user, password=ssh_pass, known_hosts=None) as conn:
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
    cmd = ['ping', '-c', '2', '-w', '5', ip]  # Envía 4 paquetes
    try:
        process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await process.communicate()
        output = stdout.decode('utf-8', errors='ignore')
        if "0% packet loss" in output or "0% pérdida de paquetes" in output or "0% perdidos" in output:
            print("exitoso")
            success_ping = True
            message = "success"
            return success_ping, message
        else:
            success_ping = False
            message = "no ejecutado con exito"
            return success_ping, message
    except Exception as e:
        print("Excepción ocurrida en la función async_ping_by_local ")
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
