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


# Regex para eliminar secuencias de escape ANSI y secuencias específicas de Windows
ansi_escape_sequences = re.compile(
    r'''
    \x1B  # Escape character (ASCII 27)
    (?:   # Non-capturing group for either
      \]0;.*?[\x07] |  # OSC (Operating System Command) sequences (used to set the window title)
      [@-Z\\-_] |  # Control characters
      \[[0-?]*[ -/]*[@-~]  # CSI sequences (Control Sequence Introducer)
    )
    ''',
    re.VERBOSE
)


async def send_command(shell, command):
    shell.send(command + "\r")
    await asyncio.sleep(0.8)  # Dar tiempo para la ejecución y respuesta


async def get_response(shell):
    # Dar tiempo para que la respuesta esté lista
    await asyncio.sleep(1)
    response = ""
    while shell.recv_ready():
        response += shell.recv(4096).decode('utf-8')
        await asyncio.sleep(0.8)  # Recoger todos los datos disponibles
    # Limpiar la respuesta de secuencias de escape ANSI y otros caracteres no deseados
    return ansi_escape_sequences.sub('', response).strip()


@hosts_router.websocket('/ws_terminal')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = f"{websocket.client.host}_{websocket.client.port}"
    ssh_host = ""
    ssh_user = ""
    ssh_pass = ""
    global sistema_operativo
    comando_linux = "uname -a"
    comando_windows = "ver"

    while True:
        command = await websocket.receive_text()
        if command.startswith('hosttarget:'):

            partes = command.split(":")
            # La dirección IP estará en la segunda parte (índice 1)
            direccion_ip = partes[1]
            dict_credentials_list = hosts_service.get_credentials(direccion_ip)
            dict_credentials = dict_credentials_list[0]
            ssh_user = hosts_service.decrypt(dict_credentials['usr'], settings.ssh_key_gen)
            ssh_pass = hosts_service.decrypt(dict_credentials['psswrd'], settings.ssh_key_gen)
            if session_id not in sessions:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(direccion_ip, username=ssh_user, password=ssh_pass)
                shell = ssh.invoke_shell()
                sessions[session_id] = {"ssh": ssh, "shell": shell}

            shell = sessions[session_id]['shell']

            # Limpiar mensaje de bienvenida o cualquier otro output inicial
            await get_response(shell)

        await send_command(shell, command)
        response = await get_response(shell)

        # Aquí asumimos que 'response' ya tiene el formato limpio y sin el comando enviado
        # Enviar respuesta al cliente
        await websocket.send_text(f"<part>user@host:<part>message:{response}")

    # Cuando se cierra el WebSocket, también cerrar la sesión SSH
    ssh = sessions[session_id]['ssh']
    if ssh:
        ssh.close()
        del sessions[session_id]
