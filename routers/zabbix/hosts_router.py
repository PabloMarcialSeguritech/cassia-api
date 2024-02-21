from fastapi import APIRouter, WebSocket
import services.zabbix.hosts_service as hosts_service
from fastapi import Depends, status, Path
from services import auth_service
from services import auth_service2
from models.cassia_user_session import CassiaUserSession
from fastapi.responses import HTMLResponse
import asyncio
import paramiko
import os
from utils.settings import Settings

current_path = {}
settings = Settings()
hosts_router = APIRouter(prefix="/hosts")


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
    print(token)
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
    print(index)
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


@hosts_router.websocket('/ws_terminal')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = f"{websocket.client.host}_{websocket.client.port}"  # Identificador de sesión más seguro
    current_path[session_id] = "C:\\"  # Establecer la ruta inicial
    partes = None
    direccion_ip = ""
    dict_credentials_list = None
    while True:
        data = await websocket.receive_text()
        if data.startswith('cd'):
            # Cambiar directorio
            path = data.split(' ', 1)[1] if ' ' in data else ''
            new_path = os.path.join(current_path[session_id], path)
            new_path = os.path.normpath(new_path)  # Normalizar el path
            if os.path.isdir(new_path):  # Verificar si el nuevo path es un directorio válido
                current_path[session_id] = new_path
                respuesta_ssh = f"Cambiado a {new_path}"
            else:
                respuesta_ssh = "Directorio no encontrado."
        elif data == 'info_sistema':
            respuesta_ssh = await ejecutar_comando_ssh(session_id, direccion_ip, 'systeminfo', dict_credentials_list)
        elif data.startswith('hosttarget:'):
            partes = data.split(":")
            # La dirección IP estará en la segunda parte (índice 1)
            direccion_ip = partes[1]
            dict_credentials_list = hosts_service.get_credentials(direccion_ip)
            respuesta_ssh = await ejecutar_comando_ssh(session_id, direccion_ip, 'systeminfo', dict_credentials_list)
        else:
            respuesta_ssh = await ejecutar_comando_ssh(session_id, direccion_ip, data, dict_credentials_list)

        prompt = await get_system_prompt(session_id, direccion_ip, dict_credentials_list)
        # formatted_response = f"user@host:{prompt}> message:{respuesta_ssh}"
        formatted_response = f"<part>user@host:{prompt}<part>message:{respuesta_ssh}"
        await websocket.send_text(formatted_response)


async def get_system_prompt(session_id, direccion_ip, dict_credentials_list):
    # Ejecutar comandos SSH para obtener el nombre de usuario y hostname
    # Estos comandos solo necesitan ser ejecutados una vez al inicio de la sesión
    username = ''
    # hostname = 'nombre_de_host'
    #     username = await ejecutar_comando_ssh('echo %USERNAME%')
    hostname = await ejecutar_comando_ssh(session_id, direccion_ip, 'hostname', dict_credentials_list)
    # return f"{username}@{hostname}${current_path[session_id]}"
    return f"{username.strip()}@{hostname.strip()}  {current_path[session_id]}"


async def ejecutar_comando_ssh(session_id, ip, comando, dict_credentials_list):
    dict_credentials = dict_credentials_list[0]
    ssh_host = ip
    ssh_user = hosts_service.decrypt(dict_credentials['usr'], settings.ssh_key_gen)
    ssh_pass = hosts_service.decrypt(dict_credentials['psswrd'], settings.ssh_key_gen)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ssh_host, username=ssh_user, password=ssh_pass)

    # Ejecutar el comando en el directorio actual
    comando_completo = f"cd /d {current_path[session_id]} && {comando}" if comando != 'cd' else "cd"
    stdin, stdout, stderr = ssh.exec_command(comando_completo)
    resultado = stdout.read().decode().strip() + stderr.read().decode().strip()
    ssh.close()
    return resultado
