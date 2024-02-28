from fastapi import APIRouter, WebSocket
import services.zabbix.hosts_service as hosts_service
from fastapi import Depends, status, Path
from services import auth_service
from services import auth_service2
from models.cassia_user_session import CassiaUserSession
from fastapi.responses import HTMLResponse
import asyncio
import paramiko
import re
from utils.settings import Settings
from datetime import datetime

settings = Settings()
hosts_router = APIRouter(prefix="/hosts")
sessions = {}
sistema_operativo = {}


@hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str = "", dispId: str = "", subtype_id: str = ""):
    return hosts_service.get_host_filter(municipalityId, dispId, subtype_id)


@hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_hosts_filter(municipalityId: str, dispId: str = "", subtype_id: str = ""):
    return hosts_service.get_host_filter(municipalityId, dispId, subtype_id)


@hosts_router.get(
    "/relations/{municipalityId}",
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host corelations filtered by municipality ID",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_host_relations(municipalityId: str):
    return hosts_service.get_host_correlation_filter(municipalityId)


@hosts_router.get('/detail/health/{hostId}',
                  tags=["Zabbix - Hosts - Detail"],
                  status_code=status.HTTP_200_OK,
                  summary="Get host metrics",
                  dependencies=[Depends(auth_service2.get_current_user_session)])
async def get_hosts_filter(hostId: int = Path(description="ID of Host", example="10596")):
    return await hosts_service.get_host_metrics(hostId)


@hosts_router.get('/detail/alerts/{hostId}',
                  tags=["Zabbix - Hosts - Detail"],
                  status_code=status.HTTP_200_OK,
                  summary="Get host alerts",
                  dependencies=[Depends(auth_service2.get_current_user_session)])
async def get_hosts_filter(hostId: int = Path(description="ID of Host", example="10596")):
    return await hosts_service.get_host_alerts(hostId)


@hosts_router.get('/detail/arcos/{hostId}',
                  tags=["Zabbix - Hosts - Detail"],
                  status_code=status.HTTP_200_OK,
                  summary="Get host arcos metric",
                  dependencies=[Depends(auth_service2.get_current_user_session)])
async def get_hosts_filter(hostId: int = Path(description="ID of Host", example="20157")):
    return await hosts_service.get_host_arcos(hostId)


html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://localhost:8000/api/v1/zabbix/hosts/ws");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""


@hosts_router.get("/soc")
async def get():
    return HTMLResponse(html)


@hosts_router.websocket("/ws/{token}")
async def websocket_endpoint(websocket: WebSocket, token):
    await websocket.accept()
    while True:
        receive_task = asyncio.create_task(recv(websocket))
        send_task = asyncio.create_task(send(websocket, 1))
        await receive_task
        await send_task


async def recv(websocket):
    data = await websocket.receive_text()
    await websocket.send_text(f"Message text was :{data}")
    await recv(websocket)


async def send(websocket, index):
    await websocket.send_text(f"sleeping")
    await asyncio.sleep(5)
    await send(websocket, index + 1)


@hosts_router.get('/actions/{ip}',
                  tags=["Zabbix - Hosts"],
                  status_code=status.HTTP_200_OK,
                  summary="Get actions info",
                  dependencies=[Depends(auth_service2.get_current_user_session)])
def get_info_actions(ip: str = Path(description="IP address", example="192.168.100.1")):
    return hosts_service.get_info_actions(ip)


@hosts_router.post('/action/{ip}/{id_action}',
                   tags=["Zabbix - Hosts"],
                   status_code=status.HTTP_200_OK,
                   summary="Run action on a server",
                   dependencies=[Depends(auth_service2.get_current_user_session)])
async def run_action(ip: str = Path(description="IP address", example="192.168.100.1"),
                     id_action: int = Path(description="ID action", example="119"),
                     current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    return await hosts_service.prepare_action(ip, id_action, current_user_session)


async def send_command(shell, command):
    print("en send_command")
    shell.send(command + "\r\n")  # Asegúrate de enviar el retorno de carro y nueva línea
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


@hosts_router.websocket('/ws_terminal')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = f"{websocket.client.host}_{websocket.client.port}"
    ssh_host = ""
    ssh_user = ""
    ssh_pass = ""
    global sistema_operativo
    comando_ram_total_usada_linux = "free -h | awk '/Mem:/{print \"Memoria Total (GB): \" $2 \", Memoria Usada (GB): \" $3 \", Memoria Libre (GB): \" $4}'"
    comando_espacio_disco_linux = "df -h --total | awk '/total/{print \"Espacio total (GB): \" $2 \", Espacio utilizado (GB): \" $3 \", Espacio libre (GB): \" $4}'"
    fecha_hora_actual = None
    fecha_hora_actual_str =""
    resultado_ram  = ""
    resultado_disco = ""
    while True:
        command = await websocket.receive_text()
        if command.startswith('hosttarget:'):
            print("en if command con hosttarget")
            partes = command.split(":")
            # La dirección IP estará en la segunda parte (índice 1)
            direccion_ip = partes[1]
            dict_credentials_list = hosts_service.get_credentials(direccion_ip)
            dict_credentials = dict_credentials_list[0]
            ssh_user = hosts_service.decrypt(dict_credentials['usr'], settings.ssh_key_gen)
            ssh_pass = hosts_service.decrypt(dict_credentials['psswrd'], settings.ssh_key_gen)
            if session_id not in sessions:
                print("en if de session_id")
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(direccion_ip, username=ssh_user, password=ssh_pass)
                shell = ssh.invoke_shell()
                sessions[session_id] = {"ssh": ssh, "shell": shell}

            shell = sessions[session_id]['shell']

            # Limpiar mensaje de bienvenida o cualquier otro output inicial
            await get_response(shell)
            sistema_operativo[session_id] = await detectar_sistema_operativo(shell)
            if sistema_operativo[session_id] == 'Windows':
                comando_ram_total_usada_windows = """ powershell "Get-CimInstance -ClassName Win32_PhysicalMemory | Measure-Object -Property Capacity -Sum | ForEach-Object { '{0:N2} GB' -f ($_.Sum / 1GB) }" """
                comando_ram_utilizacion_windows = """ powershell "$totalMem = (Get-CimInstance -ClassName Win32_ComputerSystem).TotalPhysicalMemory; $freeMem = (Get-CimInstance -ClassName Win32_OperatingSystem).FreePhysicalMemory * 1024; $usedMem = $totalMem - $freeMem; $percentUsed = ($usedMem / $totalMem) * 100; '{0:N2} %' -f $percentUsed" """
                comando_espacioDisco_windows = """ powershell "Get-PSDrive -PSProvider 'FileSystem' | Select-Object Name, @{Name='Used (GB)';Expression={ '{0:N2}' -f (($.Used / 1GB)) }}, @{Name='Free (GB)';Expression={ '{0:N2}' -f (($.Free / 1GB)) }}, @{Name='Total (GB)';Expression={ '{0:N2}' -f (($.Used / 1GB) + ($.Free / 1GB)) }}" """
                comando_info_windows = """ powershell "(Get-WmiObject -Class Win32_OperatingSystem).Caption; (Get-WmiObject Win32_NetworkAdapterConfiguration | Where-Object { $_.IPAddress -ne $null }).IPAddress[0]" """

                # Obtencion de RAM total GB
                stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(comando_ram_total_usada_windows)
                resultado_ramGB_windows = stdout.read().decode() + stderr.read().decode()
                # Obtencion de Ram utilization %
                stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(comando_ram_utilizacion_windows)
                resultado_ramPorcentaje_windows = stdout.read().decode() + stderr.read().decode()
                # Obtencion de Espacio en disco
                stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(comando_espacioDisco_windows)
                resultado_espacioDisco_windows = stdout.read().decode() + stderr.read().decode()
                # obtencion de info windows
                stdin, stdout, stderr = sessions[session_id]['ssh'].exec_command(comando_info_windows)
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

        else:
            await send_command(shell, command)
            response = await get_response(shell)
            await websocket.send_text(response)


async def detectar_sistema_operativo(shell):

    comando_linux = "uname -a"
    comando_windows = "ver"

    print("shell:", shell)
    await send_command(shell, comando_linux)
    resultado_linux =  await get_response(shell)
    print("resultado_linux::", resultado_linux)
    if "Linux" in resultado_linux or "Darwin" in resultado_linux:
        return "Linux"  # o "Unix-like" si prefieres un término más genérico
    else:
        await send_command(shell, comando_windows)
        resultado_windows =  await get_response(shell)
        if "Microsoft Windows" in resultado_windows:
            return "Windows"
    return "Desconocido"

async def send_message(websocket, message):
    print("message:::", message)
    await websocket.send_text(message)
