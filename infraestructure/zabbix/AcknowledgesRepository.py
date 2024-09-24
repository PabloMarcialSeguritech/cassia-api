from fastapi import HTTPException, status
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np
from datetime import datetime
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from models.cassia_acknowledge import CassiaAcknowledge
from models.cassia_event_acknowledges import CassiaEventAcknowledge
from models.cassia_event_acknowledges_test import CassiaEventAcknowledgeTest  # PINK
import pytz
from utils.traits import get_datetime_now_with_tz


async def get_exceptions_pool(hostid, date, db) -> pd.DataFrame:
    try:
        # PINK
        query_statement_get_event_exceptions = DBQueries(
        ).builder_query_statement_get_event_exceptions(hostid, date)
        print(query_statement_get_event_exceptions)
        exceptions_data = await db.run_query(query_statement_get_event_exceptions)
        exceptions_df = pd.DataFrame(exceptions_data).replace(np.nan, None)
        return exceptions_df
    except Exception as e:
        print(f"Excepcion en get_exceptions: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_exceptions: {e}")


async def get_exceptions(hostid, date) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        query_statement_get_event_exceptions = DBQueries(
        ).builder_query_statement_get_event_exceptions(hostid, date)
        print(query_statement_get_event_exceptions)
        await db_model.start_connection()
        exceptions_data = await db_model.run_query(query_statement_get_event_exceptions)
        exceptions_df = pd.DataFrame(exceptions_data).replace(np.nan, None)
        return exceptions_df
    except Exception as e:
        print(f"Excepcion en get_exceptions: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_exceptions: {e}")
    finally:
        await db_model.close_connection()


async def get_acknowledges_pool(eventid, is_zabbix_event, db) -> pd.DataFrame:
    try:
        # PINK
        sp_get_acknowledges = DBQueries().stored_name_get_acknowledges_test
        acknowledges_data = await db.run_stored_procedure(sp_get_acknowledges, (eventid, is_zabbix_event))
        acknowledges = pd.DataFrame(acknowledges_data).replace(np.nan, None)
        acknowledges['tickets'] = ['' for ack in range(len(acknowledges))]
        return acknowledges
    except Exception as e:
        print(f"Excepcion en get_acknowledges: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_acknowledges: {e}")


async def get_acknowledges(eventid, is_zabbix_event) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        sp_get_acknowledges = DBQueries().stored_name_get_acknowledges_test
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


async def get_event_tickets(eventid, is_cassia_event) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        get_tickets_query = DBQueries().builder_query_statement_get_cassia_event_tickets_test(
            eventid, is_cassia_event)
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
            user_id=current_session.user_id,
            clock=get_datetime_now_with_tz()
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
        await session.close()


async def create_cassia_event_acknowledge(eventid, message, current_session, close):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # PINK
        cassia_event_acknowledge = CassiaEventAcknowledgeTest(
            userid=current_session.user_id,
            eventid=eventid,
            message=message,
            action=5 if close else 4,
            clock=get_datetime_now_with_tz()
        )
        session.add(cassia_event_acknowledge)
        await session.commit()

        if cassia_event_acknowledge is not None:
            if close:
                close_event = await close_cassia_event_acknowledge(eventid)
                if close_event:
                    return True
            return True
        return False
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al guardar el usuario que creo el acknowledge : {e}")
    finally:
        print("Va a cerrar")
        await session.close()


async def close_cassia_event_acknowledge(eventid):
    db_model = DB()
    try:
        current_time = datetime.now(pytz.timezone('America/Mexico_City'))
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        # PINK
        query_close_cassia_event = DBQueries(
        ).builder_query_statement_close_event_by_id_test(eventid, formatted_time)
        """ print(query_get_last_ack) """
        await db_model.start_connection()
        close_event = await db_model.run_query(query_close_cassia_event)

        return True
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al cerrar el evento en close_cassia_event_acknowledge {eventid}: {e}")
    finally:
        await db_model.close_connection()
