from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
import socket
import numpy as np
from utils.traits import success_response
from models.cassia_state import CassiaState
from fastapi import HTTPException, status
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta
settings = Settings()


async def get_configuration():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"SELECT * FROM cassia_config")
    configuration = session.execute(statement)
    configuration = pd.DataFrame(configuration).replace(np.nan, "")
    session.close()

    return success_response(data=configuration.to_dict(orient="records"))


async def get_estados():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select state_id as id, state_name as name, url,url_front from cassia_state where deleted_at is null")
    estados = session.execute(statement)
    estados = pd.DataFrame(estados).replace(np.nan, "")
    session.close()
    return success_response(data=estados.to_dict(orient="records"))


async def ping_estado(id_estado):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    estado = session.query(CassiaState).filter(CassiaState.state_id == id_estado,
                                               CassiaState.deleted_at == None).first()
    if not estado:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El estado con el id proporcionado no existe.")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    available = True
    try:
        sock.connect((estado.ip, int(estado.port)))
        print(f"El puerto esta abierto")
        result = f"El estado {estado.state_name} esta disponible."
    except socket.error:
        print("El puerto esta cerrado")
        result = f"El estado {estado.state_name} no esta disponible."
        available = False
    finally:
        sock.close()
    session.close()
    return success_response(message=result, data={'available': available})


async def slack_message(message: str):
    # Reemplaza 'TU_TOKEN' con el token de tu aplicación
    token = 'xoxb-6438308523153-6438346187841-D4zGsBsAPIbx7ss8aADDak3X'

    # Reemplaza 'CANAL_DESTINO' con el nombre del canal o el identificador
    channel = 'C06CL3U23NY'

    # Mensaje que deseas enviar
    mensaje = message

    # Crea un cliente de Slack
    client = WebClient(token=token)

    try:
        # Envía el mensaje al canal
        response = client.chat_postMessage(channel=channel, text=mensaje)
        print(response)
        print(type(response))
        """ 
        print("Mensaje enviado:", response['ts']) """
    except SlackApiError as e:
        """ print("Error al enviar el mensaje:", e.response["error"]) """
    return success_response(response)


async def slack_message_get():
    # Reemplaza 'TU_TOKEN' con el token de tu aplicación
    token = 'xoxb-6438308523153-6438346187841-D4zGsBsAPIbx7ss8aADDak3X'

    # Reemplaza 'CANAL_DESTINO' con el nombre del canal o el identificador
    channel = 'C06CL3U23NY'

    # Mensaje que deseas enviar

    # Crea un cliente de Slack
    client = WebClient(token=token)
    hora_anterior = datetime.now() - timedelta(hours=1)
    timestamp_hora_anterior = int(hora_anterior.timestamp())
    try:
        # Envía el mensaje al canal
        response = client.conversations_history(
            channel=channel, oldest=timestamp_hora_anterior,)
        mensajes = response['messages']

        print(mensajes)
        print(type(mensajes))
        # Procesa los mensajes según sea necesario
        for mensaje in mensajes:
            print(mensaje['text'])
    except SlackApiError as e:
        print("Error al obtener mensajes:", e.response["error"])
    return success_response(mensajes)
