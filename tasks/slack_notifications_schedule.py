import unicodedata
from rocketry import Grouper
from rocketry.conds import every
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
import utils.settings as settings
import uuid
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from models.cassia_slack_notification import CassiaSlackNotification
import time
# Creating the Rocketry app
slack_scheduler = Grouper()
# Creating some tasks
SETTINGS = settings.Settings()
slack_notify = SETTINGS.slack_notify
abreviatura_estado = SETTINGS.abreviatura_estado
estado = SETTINGS.estado
slack_channel = SETTINGS.slack_channel
slack_token = SETTINGS.slack_token
slack_bot = SETTINGS.slack_bot
slack_problem_severities = SETTINGS.slack_problem_severities


@slack_scheduler.cond('slack_notify')
def is_traffic():
    return slack_notify


@slack_scheduler.task(("every 60 seconds & slack_notify"), execution="thread")
async def send_messages():

    guardados = 0
    db_zabbix = DB_Zabbix()
    with db_zabbix.Session() as session:
        statement = text(
            f"call sp_viewProblem('0','','','{slack_problem_severities}')")

        problems = pd.DataFrame(session.execute(statement))
        if not problems.empty:
            problems = problems[problems['Problem']
                                == 'Unavailable by ICMP ping']

        mensajes = text(
            "select eventid from cassia_slack_notifications where local=1")
        mensajes = pd.DataFrame(session.execute(mensajes))
        mensajes_a_enviar = pd.DataFrame()

        client = WebClient(token=slack_token)

        if not mensajes.empty:
            if not problems.empty:
                mensajes['eventid'] = mensajes['eventid'].astype(int)
                mensajes_a_enviar = problems[~problems['eventid'].isin(
                    mensajes['eventid'])]
        else:
            mensajes_a_enviar = problems

        for ind in mensajes_a_enviar.index:
            message_uuid = str(uuid.uuid4())
            mensaje = f"""Cassia - {estado}
Fecha y hora: {mensajes_a_enviar['Time'][ind]}
Dispositivo: {mensajes_a_enviar['Host'][ind]}
Estado: {mensajes_a_enviar['Estatus'][ind]}
Problema: {mensajes_a_enviar['Problem'][ind]}
Severidad: {mensajes_a_enviar['severity'][ind]}
Ip: {mensajes_a_enviar['ip'][ind]}
Hostid: {mensajes_a_enviar['hostid'][ind]}
Latitud: {mensajes_a_enviar['latitude'][ind]}
Longitud: {mensajes_a_enviar['longitude'][ind]}
Eventid: {mensajes_a_enviar['eventid'][ind]}
{estado}
{message_uuid}"""

            try:
                fecha = datetime.strptime(
                    mensajes_a_enviar['Time'][ind], '%d/%m/%Y %H:%M:%S')
                response = client.chat_postMessage(
                    channel=slack_channel, text=mensaje)
                print("Mensaje enviado:", response['ts'])

                notification = CassiaSlackNotification(
                    uuid=message_uuid,
                    message=mensaje,
                    state=estado,
                    problem_date=fecha,
                    host=mensajes_a_enviar['Host'][ind],
                    incident=mensajes_a_enviar['Problem'][ind],
                    severity=mensajes_a_enviar['severity'][ind],
                    eventid=mensajes_a_enviar['eventid'][ind],
                    status=mensajes_a_enviar['Estatus'][ind],
                    message_date=datetime.now(),
                    hostid=mensajes_a_enviar['hostid'][ind],
                    ip=mensajes_a_enviar['ip'][ind],
                    latitude=mensajes_a_enviar['latitude'][ind],
                    longitude=mensajes_a_enviar['longitude'][ind],
                    local=1
                )
                session.add(notification)
                session.commit()
                time.sleep(1)
                guardados += 1
            except SlackApiError as e:
                print("Error al enviar el mensaje:", e.response["error"])

    print(f"terminado {guardados} guardados")


""" @slack_scheduler.task(("every 60 seconds & slack_notify"), execution="thread") """


async def get_messages():
    db_zabbix = DB_Zabbix()

    with db_zabbix.Session() as session:
        client = WebClient(token=slack_token)
        hora_anterior = datetime.now() - timedelta(minutes=700)
        timestamp_hora_anterior = int(hora_anterior.timestamp())

        try:
            response = client.conversations_history(
                channel=slack_channel, oldest=timestamp_hora_anterior)
            print(response)
            """ mensajes = [mensaje['text'] for mensaje in response['messages'] if mensaje.get(
                'bot_id') == slack_bot] """
            mensajes = [mensaje['text'] for mensaje in response['messages'] if mensaje.get(
                'bot_id') == slack_bot]
            print(mensajes)
            uuids = set(get_uuids(mensajes))

            data = process_messages(mensajes, uuids)
            mensajes_a_guardar = pd.DataFrame(data, columns=[
                'uuid', 'message', 'state', 'problem_date', 'host', 'incident', 'severity', 'status', 'ip', 'hostid', 'latitude', 'longitude', 'eventid'])

            mensajes_creados = text(
                f"select uuid from cassia_slack_notifications csn where uuid in ({','.join(uuids) if len(uuids) else 0})")
            mensajes_creados = pd.DataFrame(session.execute(mensajes_creados))

            if not mensajes_creados.empty:
                mensajes_a_guardar = mensajes_a_guardar[~mensajes_a_guardar['uuid'].isin(
                    mensajes_creados['uuid'])]

            for ind in mensajes_a_guardar.index:
                notification = CassiaSlackNotification(
                    **mensajes_a_guardar.iloc[ind].to_dict())
                session.add(notification)

            session.commit()

        except SlackApiError as e:
            print("Error al obtener mensajes:", e.response["error"])


""" @slack_scheduler.task(("every 60 seconds & slack_notify"), execution="thread") """


async def drop_duplicates():
    db_zabbix = DB_Zabbix()

    with db_zabbix.Session() as session:
        uuids_duplicados = text("""
select  uuid,count(*) as conteo from cassia_slack_notifications csn 
group by uuid 
HAVING count(*)>1""")
        uuids_duplicados = pd.DataFrame(session.execute(uuids_duplicados))
        if not uuids_duplicados.empty:
            uuids_duplicados['uuid'] = uuids_duplicados['uuid'].apply(
                lambda uuid: "'"+str(uuid)+"'")

            uuids_duplicados = ",".join(uuids_duplicados['uuid'].to_list())

            delete = text(f"""Delete from cassia_slack_notifications 
                          where uuid in({uuids_duplicados})
                          and eventid is null""")
            session.execute(delete)
            session.commit()
            print("limpiados")


def get_uuids(messages: list[str]):
    uuids = set()
    for mensaje in messages:
        lineas = mensaje.splitlines()
        uuid = "'" + lineas[-1] + "'"
        uuids.add(uuid)
    return uuids


def process_messages(messages: list[str], uuids: set):
    data = []
    for mensaje in messages:
        lineas = mensaje.splitlines()
        fecha = lineas[1][14:]
        formato = '%d/%m/%Y %H:%M:%S'
        fecha = datetime.strptime(fecha, formato)
        dispositivo = lineas[2][13:]
        status = lineas[3][8:]
        problema = lineas[4][10:]
        severidad = lineas[5][11:]
        ip = lineas[6][4:]
        hostid = lineas[7][8:]
        latitud = lineas[8][9:]
        longitud = lineas[9][10:]
        eventid = lineas[10][9:]
        estado = lineas[-2]
        uuid = lineas[-1]

        if uuid not in uuids:
            objeto = {
                'uuid': uuid,
                'message': mensaje,
                'state': estado,
                'problem_date': fecha,
                'host': dispositivo,
                'incident': problema,
                'severity': severidad,
                'status': status,
                'ip': ip,
                'hostid': hostid,
                'latitude': latitud,
                'longitude': longitud,
                'eventid': eventid
            }
            data.append(objeto)

    return data
