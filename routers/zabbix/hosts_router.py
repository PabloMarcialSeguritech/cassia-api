from fastapi import APIRouter, WebSocket
import services.zabbix.hosts_service as hosts_service
from fastapi import Depends, status, Path
from services import auth_service
import services.zabbix.interface_service as interface_service
from fastapi.responses import HTMLResponse
import asyncio

hosts_router = APIRouter(prefix="/hosts")


@hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_hosts_filter(municipalityId: str, dispId: str = "", subtype_id: str = ""):
    return hosts_service.get_host_filter(municipalityId, dispId, subtype_id)


@hosts_router.get(
    '/{municipalityId}',
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host by municipality ID, technology or device type, and subtype",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_hosts_filter(municipalityId: str, dispId: str = "", subtype_id: str = ""):
    return hosts_service.get_host_filter(municipalityId, dispId, subtype_id)


@hosts_router.get(
    "/relations/{municipalityId}",
    tags=["Zabbix - Hosts"],
    status_code=status.HTTP_200_OK,
    summary="Get host corelations filtered by municipality ID",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_host_relations(municipalityId: str):
    return hosts_service.get_host_correlation_filter(municipalityId)


@hosts_router.post('/ping/{hostId}',
                   tags=["Zabbix - Hosts"],
                   status_code=status.HTTP_200_OK,
                   summary="Create a ping on a device",
                   dependencies=[Depends(auth_service.get_current_user)])
def create_ping(hostId: int = Path(description="ID of Host", example="10596")):
    return interface_service.create_ping(hostId)


@hosts_router.get('/detail/health/{hostId}',
                  tags=["Zabbix - Hosts - Detail"],
                  status_code=status.HTTP_200_OK,
                  summary="Get host metrics",
                  dependencies=[Depends(auth_service.get_current_user)])
async def get_hosts_filter(hostId: int = Path(description="ID of Host", example="10596")):
    return await hosts_service.get_host_metrics(hostId)


@hosts_router.get('/detail/alerts/{hostId}',
                  tags=["Zabbix - Hosts - Detail"],
                  status_code=status.HTTP_200_OK,
                  summary="Get host alerts",
                  dependencies=[Depends(auth_service.get_current_user)])
async def get_hosts_filter(hostId: int = Path(description="ID of Host", example="10596")):
    return await hosts_service.get_host_alerts(hostId)


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


@hosts_router.post('/reboot/{hostId}',
                   tags=["Zabbix - Hosts"],
                   status_code=status.HTTP_200_OK,
                   summary="Reboot on a server",
                   dependencies=[Depends(auth_service.get_current_user)])
def reboot(hostId: int = Path(description="ID of Host", example="10596")):
    return hosts_service.reboot(hostId)
