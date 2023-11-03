from fastapi import FastAPI, WebSocket
from routers.user_router import auth_router
from routers.zabbix import zabbix_router
from routers.cassia import cassia_router
from middleware.error_handler import ErrorHandler
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import asyncio
app = FastAPI(
)
app.version = '1.3'
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ErrorHandler)
app.include_router(auth_router)
app.include_router(zabbix_router)
app.include_router(cassia_router)

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
            var ws = new WebSocket("ws://localhost:8000/api/v1/zabbix/hosts/ws/eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJqdWFuLm1hcmNpYWwiLCJleHAiOjE2OTA4OTk4NjB9.XH_dMo4x7QAcpldTqXxJYrtZmHLOE1noSwye_mxH8s0");
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                console.log(event)
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


""" @app.get("/soc") """


async def get():
    return HTMLResponse(html)


""" @app.websocket("/ws") """


async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        receive_task = asyncio.create_task(recv(websocket))
        send_task = asyncio.create_task(send(websocket))
        await receive_task
        await send_task
        """ asyncio.sleep()
        data = await websocket.receive_text()
        await websocket.send_text(f"Message text was :{data}") """


async def recv(websocket):
    data = await websocket.receive_text()
    await websocket.send_text(f"Message text was :{data}")


async def send(websocket):
    await websocket.send_text(f"sleeping")
    await asyncio.sleep(5)
