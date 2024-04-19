from fastapi import HTTPException, status
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np
from datetime import datetime
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from models.cassia_acknowledge import CassiaAcknowledge
from models.cassia_event_acknowledges import CassiaEventAcknowledge
import pytz


async def get_acknowledges(eventid, is_zabbix_event) -> pd.DataFrame:
    db_model = DB()
    try:

        sp_get_acknowledges = DBQueries().stored_name_get_acknowledges
        await db_model.start_connection()
        acknowledges_data = await db_model.run_stored_procedure(sp_get_acknowledges, (eventid, is_zabbix_event))
        acknowledges = pd.DataFrame(acknowledges_data).replace(np.nan, None)
        acknowledges['tickets'] = ['' for ack in range(len(acknowledges))]
        return acknowledges
    except Exception as e:
        print(f"Excepcion en get_acknowledges: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_acknowledges: {e}")
    finally:
        await db_model.close_connection()


async def get_event_tickets(eventid) -> pd.DataFrame:
    db_model = DB()
    try:
        get_tickets_query = DBQueries().builder_query_statement_get_cassia_event_tickets(eventid)
        await db_model.start_connection()
        tickets_data = await db_model.run_query(get_tickets_query)
        tickets = pd.DataFrame(tickets_data).replace(np.nan, None)

        return tickets
    except Exception as e:
        print(f"Excepcion en get_event_tickets: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_event_tickets: {e}")
    finally:
        await db_model.close_connection()


async def process_acknowledges_tickets(event: pd.DataFrame, acknowledges: pd.DataFrame, tickets: pd.DataFrame, is_cassia_event: int) -> dict:
    # Obtiene fecha actual
    now = datetime.now(pytz.timezone(
        'America/Mexico_City')).replace(tzinfo=None)
    # Obtiene la fecha del problema para sacar el acumulado
    if not int(is_cassia_event):
        clock_problem = event.iloc[0]['clock']
        clock_problem = datetime.fromtimestamp(
            clock_problem, pytz.timezone('America/Mexico_City')).replace(tzinfo=None)
    else:
        clock_problem = event.iloc[0]['created_at']
    # Obtiene el acumulado del evento hasta la fecha
    diff = now-clock_problem
    acumulated_cassia = round(diff.days*24 + diff.seconds/3600, 4)
    # Construye una respuesta inicial
    resume = {
        'acumulated_cassia': acumulated_cassia,
        'acumulated_ticket': 0,
        'date': now.strftime("%d/%m/%Y %H:%M:%S"),
    }
    # Verifica que el evento tenga acknowledges
    if not acknowledges.empty:
        resume["acumulated_ticket"] = []
        # Itera sobre los tickets para sacar el acumulado de cada ticket
        for ind in tickets.index:
            clock = tickets.iloc[ind]['clock']
            diff = now-clock
            hours = round(diff.days*24 + diff.seconds/3600, 4)
            """ print(hours) """
            resume["acumulated_ticket"].append({'tracker_id': str(tickets['tracker_id'][ind]),
                                                'ticket_id': str(tickets['ticket_id'][ind]),
                                                'accumulated': hours})

            """ print(clock <= pd.to_datetime(acknowledges["Time"]
                  [0], format="%d/%m/%Y %H:%M:%S")) """
            acknowledges.loc[clock <= pd.to_datetime(acknowledges["Time"], format="%d/%m/%Y %H:%M:%S"),
                             'tickets'] = acknowledges['tickets']+', '+str(tickets['tracker_id'][ind])
    return resume


async def create_zabbix_acknowledge(eventid, message, current_session, close):
    try:
        zabbix_api = ZabbixApi()
        params = {
            "eventids": [eventid],
            "action": 5 if close else 4,
            "message": message
        }
        created_acknowledge = await zabbix_api.do_request('event.acknowledge', params)
        eventid_response = created_acknowledge['eventids']

        if int(eventid) in eventid_response:
            print("Creado correctamente")
            last_acknowledge_id = await get_last_zabbix_event_acknowledge(eventid)
            print(last_acknowledge_id)
            created_cassia_acknowledge = await create_cassia_acknowledge(
                last_acknowledge_id, current_session)
            print(create_cassia_acknowledge)
            if created_cassia_acknowledge:
                return True
        return False

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en create_zabbix_acknowledge {e}")


async def get_last_zabbix_event_acknowledge(eventid):
    db_model = DB()
    try:
        query_get_last_ack = DBQueries(
        ).builder_query_statement_get_last_zabbix_event_acknowledge(eventid)
        """ print(query_get_last_ack) """
        await db_model.start_connection()
        last_acknowledge = await db_model.run_query(query_get_last_ack)
        acknowledge = pd.DataFrame(last_acknowledge)
        """ print(acknowledge) """

        acknowledge_id = acknowledge['acknowledgeid'][0]
        return acknowledge_id
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al obtener ultimo acknowledge de el evento {eventid}: {e}")
    finally:
        await db_model.close_connection()


async def create_cassia_acknowledge(
        last_acknowledge_id, current_session):

    db_model = DB()
    try:
        session = await db_model.get_session()

        cassia_acknowledge = CassiaAcknowledge(
            acknowledge_id=last_acknowledge_id,
            user_id=current_session.user_id
        )

        session.add(cassia_acknowledge)

        await session.commit()

        if cassia_acknowledge is not None:
            return True
        return False
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al guardar el usuario que creo el acknowledge : {e}")
    finally:
        print("Va a cerrar")
        await session.close()
        print("CERRROOOOO LA CONEXIIOOOOOOON")


async def create_cassia_event_acknowledge(eventid, message, current_session, close):
    db_model = DB()
    try:
        session = await db_model.get_session()
        cassia_event_acknowledge = CassiaEventAcknowledge(
            userid=current_session.user_id,
            eventid=eventid,
            message=message,
            action=5 if close else 4
        )
        session.add(cassia_event_acknowledge)
        await session.commit()

        if cassia_event_acknowledge is not None:
            return True
        return False
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al guardar el usuario que creo el acknowledge : {e}")
    finally:
        print("Va a cerrar")
        await session.close()
