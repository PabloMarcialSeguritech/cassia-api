from fastapi import APIRouter, WebSocket
import services.zabbix.alerts_service as alerts_service
import schemas.exception_agency_schema as exception_agency_schemas
import schemas.exceptions_schema as exception_schema
import schemas.problem_record_schema as problem_record_schema
import schemas.problem_record_history_schema as problem_records_history_schema
import schemas.cassia_ticket_schema as cassia_ticket_schema
from fastapi import Depends, status, Path
from services import auth_service
from services import auth_service2
from fastapi import Body
from models.user_model import User
from models.cassia_user_session import CassiaUserSession
from fastapi import File, UploadFile, Form, Query
from fastapi.responses import FileResponse
from typing import Optional
from infraestructure.database import DB
from dependencies import get_db

import services.zabbix.hosts_service as hosts_service
import asyncio
import paramiko
import re
from utils.settings import Settings
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import time

from fastapi.responses import HTMLResponse




ws_router = APIRouter()
settings = Settings()
sessions = {}
sistema_operativo = {}
# Un diccionario para mantener las referencias de las tareas por sesión
tasks = {}


@ws_router.get(
    '/ws_get'
)
async def ws_get():
    return "hola"


'''@ws_router.websocket("/ws_terminal_")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = await websocket.receive_text()
        await websocket.send_text(f"Mensaje recibido: {data}")'''

async def send_command(shell, command):
    print("en send_command")
    # Asegúrate de enviar el retorno de carro y nueva línea
    shell.send(command + "\r\n")
    await asyncio.sleep(0.8)  # Espera para la ejecución


async def get_response(shell):
    print("en get_response")
    await asyncio.sleep(0.5)  # Espera para la salida
    output = ""
    while shell.recv_ready():
        output += shell.recv(4096).decode('utf-8')
        await asyncio.sleep(0.1)
    print("output::", output)
    return output

@ws_router.websocket('/ws_terminal')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = f"{websocket.client.host}_{websocket.client.port}"
    ssh_host = ""
    ssh_user = ""
    ssh_pass = ""
    global sistema_operativo
    while True:
        command = await websocket.receive_text()
        if command == "\x03":
            # Cancelar la tarea en ejecución cuando se recibe Ctrl+C
            if session_id in tasks:
                tasks[session_id].cancel()
                del tasks[session_id]
            await send_command(sessions[session_id]['shell'], command)
            response = await get_response(sessions[session_id]['shell'])
            # Envía directamente sin procesar
            await websocket.send_text(response)
        elif command.startswith('ping'):
            # Lanzar la tarea para comandos con salida continua
            task = asyncio.create_task(send_continuous_data(
                websocket, command, shell, session_id))
            tasks[session_id] = task
        elif (command.startswith('htop') or command.startswith('top')) and session_id in sistema_operativo and \
                sistema_operativo[session_id] == "Linux":
            # Lanzar la tarea para comandos con salida continua
            task = asyncio.create_task(send_continuous_data(
                websocket, command, shell, session_id))
            tasks[session_id] = task
        elif command.startswith('hosttarget:'):
            partes = command.split(":")
            # La dirección IP estará en la segunda parte (índice 1)
            direccion_ip = partes[1]
            dict_credentials_list = hosts_service.get_credentials(direccion_ip)
            if dict_credentials_list is None or not dict_credentials_list:
                error_message = f"Credenciales no encontradas para la ip {direccion_ip}"
                # Enviar el mensaje de error al websocket
                await send_message(websocket, error_message)
                # Esperar 2 segundos antes de cerrar la conexión
                await asyncio.sleep(4)
                await websocket.close()  # Cerrar la conexión websocket
            else:
                dict_credentials = dict_credentials_list[0]
                ssh_user = hosts_service.decrypt(
                    dict_credentials['usr'], settings.ssh_key_gen)
                ssh_pass = hosts_service.decrypt(
                    dict_credentials['psswrd'], settings.ssh_key_gen)
                if session_id not in sessions:
                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    try:
                        ssh = await connect_ssh(direccion_ip, ssh_user, ssh_pass)
                    except Exception as e:
                        error_message = "No fue posible hacer la conexión al servidor"
                        print(
                            f"Error al conectar a la ip {direccion_ip} por SSH: {str(e)}")
                        # Enviar el mensaje de error al websocket
                        await send_message(websocket, error_message)
                        # Esperar 2 segundos antes de cerrar la conexión
                        await asyncio.sleep(4)
                        await websocket.close()

                    shell = ssh.invoke_shell()
                    sessions[session_id] = {"ssh": ssh, "shell": shell}

                shell = sessions[session_id]['shell']
                # Limpiar mensaje de bienvenida o cualquier otro output inicial
                await get_response(shell)
                sistema_operativo[session_id] = await detectar_sistema_operativo(shell)
                if sistema_operativo[session_id] == 'Windows':
                    comando_ram_total_usada_windows = """ powershell "Get-CimInstance -ClassName Win32_PhysicalMemory | Measure-Object -Property Capacity -Sum | ForEach-Object { '{0:N2} GB' -f ($_.Sum / 1GB) }" """
                    comando_ram_utilizacion_windows = """ powershell "$totalMem = (Get-CimInstance -ClassName Win32_ComputerSystem).TotalPhysicalMemory; $freeMem = (Get-CimInstance -ClassName Win32_OperatingSystem).FreePhysicalMemory * 1024; $usedMem = $totalMem - $freeMem; $percentUsed = ($usedMem / $totalMem) * 100; '{0:N2} %' -f $percentUsed" """
                    comando_espacioDisco_windows = """powershell "Get-PSDrive -PSProvider 'FileSystem' | Select-Object Name, @{Name='Used (GB)';Expression={ '{0:N2}' -f (($_.Used / 1GB)) }}, @{Name='Free (GB)';Expression={ '{0:N2}' -f (($_.Free / 1GB)) }}, @{Name='Total (GB)';Expression={ '{0:N2}' -f (($_.Used / 1GB) + ($_.Free / 1GB)) }}" """
                    comando_info_windows = """ powershell "(Get-WmiObject -Class Win32_OperatingSystem).Caption; (Get-WmiObject Win32_NetworkAdapterConfiguration | Where-Object { $_.IPAddress -ne $null }).IPAddress[0]" """

                    # Obtencion de RAM total GB
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_ram_total_usada_windows)
                    resultado_ramGB_windows = stdout.read().decode() + stderr.read().decode()
                    # Obtencion de Ram utilization %
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_ram_utilizacion_windows)
                    resultado_ramPorcentaje_windows = stdout.read().decode() + stderr.read().decode()
                    # Obtencion de Espacio en disco
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_espacioDisco_windows)
                    resultado_espacioDisco_windows = stdout.read().decode() + stderr.read().decode()
                    # obtencion de info windows
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_info_windows)
                    resultado_info_windows = stdout.read().decode() + stderr.read().decode()

                    # await websocket.send_text(f"RAM GB: {resultado_ramGB} & RAM %: {resultado_ramPorcentaje}")
                    # Título en negrita y subrayado
                    tituloInfo = "\033[4mInfo - SO\033[0m"
                    tituloRAM = "\033[4mRAM - Status\033[0m"
                    tituloDISK = "\033[4mEspacio en Disco - Status\033[0m"

                    resultado_Info_formateado_windows = f"\033[34m{resultado_info_windows}\033[0m"
                    # Formatear RAM GB en azul y en negrita
                    resultado_ramGB_formateado_windows = f"\033[34m{resultado_ramGB_windows}\033[0m"
                    # Formatear el porcentaje de RAM utilizado en azul
                    resultado_ramPorcentaje_formateado_windows = f"\033[34m{resultado_ramPorcentaje_windows.strip()}\033[0m"
                    # Formatear el porcentaje de Espacio Disco utilizado en azul
                    resultado_espacioDisco_formateado_windows = f"{resultado_espacioDisco_windows}"

                    # Combinar todo en un mensaje con saltos de línea e indentación adecuados
                    mensaje_formateado = (
                        f"\033[G\n*********** {tituloInfo} ************\n"
                        f"\033[G\n {resultado_Info_formateado_windows}"
                        f"\033[G\n*********** {tituloRAM} ***********\n"
                        f"\033[G\n- RAM Total: {resultado_ramGB_formateado_windows}"
                        f"\033[G- RAM % Used: {resultado_ramPorcentaje_formateado_windows}"
                        f"\033[G\n\n********* {tituloDISK} **********\n"
                        f"\033[G\n {resultado_espacioDisco_formateado_windows}"
                        f"\033[G\nEnter para continuar...\n"

                    )

                    # Enviar el mensaje formateado
                    await websocket.send_text(mensaje_formateado)
                elif sistema_operativo[session_id] == "Linux":
                    # Usa exec_command para los comandos específicos
                    comando_ram_total_usada_linux = """ free -g | awk '/Mem:/ {print $2 " GB"}' """  # linux command
                    comando_ram_utilizacion_linux = """ free | awk '/Mem:/ {printf("%.2f", $3/$2 * 100.0); print "%"}' """  # linux command
                    comando_espacioDisco_linux = """ df -h | awk '$NF=="/"{print "Total: " $2 ", Usado: " $3 ", Libre: " $4}' """
                    comando_info_linux = """ cat /etc/os-release | grep PRETTY_NAME | cut -d '"' -f2 """
                    commando_infoIP_linux = """ hostname -I | awk '{print $1}' """
                    # Obtencion de RAM total GB
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_ram_total_usada_linux)
                    resultado_ramGB_linux = stdout.read().decode() + stderr.read().decode()
                    # Obtencion de Ram utilization %
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_ram_utilizacion_linux)
                    resultado_ramPorcentaje_linux = stdout.read().decode() + stderr.read().decode()
                    # Obtencion de Espacio en disco
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_espacioDisco_linux)
                    resultado_espacioDisco_linux = stdout.read().decode() + stderr.read().decode()
                    # obtencion de info windows
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        comando_info_linux)
                    resultado_infolinux = stdout.read().decode() + stderr.read().decode()
                    # obtencion de info IP windows
                    stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(
                        commando_infoIP_linux)
                    resultado_infoIPlinux = stdout.read().decode() + stderr.read().decode()

                    # await websocket.send_text(f"RAM GB: {resultado_ramGB} & RAM %: {resultado_ramPorcentaje}")
                    # Título en negrita y subrayado
                    tituloInfo = "\033[4mInfo - SO\033[0m"
                    tituloRAM = "\033[4mRAM - Status\033[0m"
                    tituloDISK = "\033[4mEspacio en Disco - Status\033[0m"

                    resultado_Info_formateado_linux = f"\033[34m{resultado_infolinux}\033[0m"
                    # Formatear RAM GB en azul y en negrita
                    resultado_InfoIP_formateado_linux = f"\033[34m{resultado_infoIPlinux}\033[0m"
                    # Formatear RAM GB en azul y en negrita
                    resultado_ramGB_formateado_linux = f"\033[34m{resultado_ramGB_linux}\033[0m"
                    # Formatear el porcentaje de RAM utilizado en azul
                    resultado_ramPorcentaje_formateado_linux = f"\033[34m{resultado_ramPorcentaje_linux.strip()}\033[0m"
                    # Formatear el porcentaje de Espacio Disco utilizado en azul
                    resultado_espacioDisco_formateado_linux = f"{resultado_espacioDisco_linux}"

                    # Combinar todo en un mensaje con saltos de línea e indentación adecuados
                    mensaje_formateado = (
                        f"\033[G\n*********** {tituloInfo} ************\n"
                        f"\033[G\n {resultado_Info_formateado_linux}"
                        f"\033[G\n {resultado_InfoIP_formateado_linux}"
                        f"\033[G\n*********** {tituloRAM} ***********\n"
                        f"\033[G\n- RAM Total: {resultado_ramGB_formateado_linux}"
                        f"\033[G- RAM % Used: {resultado_ramPorcentaje_formateado_linux}"
                        f"\033[G\n\n********* {tituloDISK} **********\n"
                        f"\033[G\n {resultado_espacioDisco_formateado_linux}"
                        f"\033[G\nEnter para continuar...\n"

                    )

                    # Enviar el mensaje formateado
                    await websocket.send_text(mensaje_formateado)
        else:
            await send_command(shell, command)
            response = await get_response(shell)
            await websocket.send_text(response)


async def detectar_sistema_operativo(shell):
    comando_linux = "uname -a"
    comando_windows = "ver"

    print("shell:", shell)
    await send_command(shell, comando_linux)
    resultado_linux = await get_response(shell)
    if "Linux" in resultado_linux or "Darwin" in resultado_linux:
        return "Linux"  # o "Unix-like" si prefieres un término más genérico
    else:
        await send_command(shell, comando_windows)
        resultado_windows = await get_response(shell)
        if "Microsoft Windows" in resultado_windows:
            return "Windows"
    return "Desconocido"


async def send_message(websocket, message):
    await websocket.send_text(message)


async def send_continuous_data(websocket: WebSocket, command: str, shell, session_id):
    shell.send(command + "\n")
    while True:
        await asyncio.sleep(0.5)
        if shell.recv_ready():
            data = shell.recv(1024).decode('utf-8')
            await websocket.send_text(data)
        if session_id not in tasks:  # Verificar si la tarea fue cancelada
            break


async def connect_ssh(direccion_ip, ssh_user, ssh_pass):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        ssh = await loop.run_in_executor(pool, connect_ssh_blocking, direccion_ip, ssh_user, ssh_pass)
    return ssh


def connect_ssh_blocking(direccion_ip, ssh_user, ssh_pass):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(direccion_ip, username=ssh_user,
                    password=ssh_pass, timeout=30)
        return ssh
    except paramiko.ssh_exception.SSHException as e:
        # Manejar el error de conexión SSH aquí
        print(f"Error al conectar a la dirección IP {direccion_ip}: {str(e)}")
        return None  # Otra opción podría ser lanzar una excepción personalizada para manejar este caso