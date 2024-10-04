from infraestructure.cassia import CassiaEventRepository
from infraestructure.zabbix import ZabbixEventRepository
from infraestructure.zabbix import AcknowledgesRepository
from infraestructure.cassia import cassia_gs_tickets_repository
from infraestructure.cassia import CassiaConfigRepository
from utils.traits import success_response, get_datetime_now_str_with_tz, get_datetime_now_with_tz
from fastapi import HTTPException, status
from datetime import datetime
import pytz
import pandas as pd
import time
import asyncio

# Función para convertir la cadena de fecha a un objeto datetime, manejando 'NaT'


def parse_date(date_str):
    if isinstance(date_str, datetime):  # Si ya es un objeto datetime
        return date_str
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
            except ValueError:
                return None

# Función para convertir la fecha al formato deseado


# Función para convertir la fecha al formato deseado
def format_date(date):
    if isinstance(date, str):
        try:
            # Intentar parsear la fecha en diferentes formatos
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(date, fmt)
                    return dt.strftime("%d/%m/%Y %H:%M:%S")
                except ValueError:
                    continue
            return date  # En caso de no poder parsear, devolver la fecha original
        except Exception as e:
            return date  # En caso de cualquier excepción, devolver la fecha original
    elif isinstance(date, (datetime, pd.Timestamp)):
        return date.strftime("%d/%m/%Y %H:%M:%S")
    return date  # En caso de cualquier otro tipo, devolver la fecha original


async def get_acks(eventid, is_cassia_event, db):
    if int(is_cassia_event):
        # Obtiene el evento de cassia
        event = await CassiaEventRepository.get_cassia_event_pool(eventid, db)
    else:
        event = await ZabbixEventRepository.get_zabbix_event_pool(eventid, db)
    # Si no existe regresa un excepcion de http
    if event.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The eventid not exists",
        )
    messages = []
    now_str = get_datetime_now_str_with_tz()
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
    messages.append({'type': 'Creación de evento',
                     'message': 'Creación del evento.',
                     'date': parse_date(clock_problem),
                     'user': None})
    tasks = {
        'event_acknowledges_df': asyncio.create_task(AcknowledgesRepository.get_acknowledges_pool(eventid, is_cassia_event, db)),
        'event_exceptions_df': asyncio.create_task(AcknowledgesRepository.get_exceptions_pool(event['hostid'][0], clock_problem, db)),
        'event_tickets_df': asyncio.create_task(cassia_gs_tickets_repository.get_event_tickets_by_affiliation_and_date_pool(event['alias'][0], clock_problem, db)),
        'event_resets_df': asyncio.create_task(cassia_gs_tickets_repository.get_event_resets_by_affiliation_and_date_pool(event['alias'][0], clock_problem, db)),

    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    # Obtiene los acknowledges del evento
    event_acknowledges = dfs['event_acknowledges_df']
    for ind in event_acknowledges.index:
        messages.append({'type': 'Acknowledge',
                         'message': event_acknowledges['message'][ind],
                         'date': parse_date(event_acknowledges['Time'][ind]),
                         'user': event_acknowledges['user'][ind]})

    # Obtiene las excepciones dentro del timepo de vida del evento

    event_exceptions = dfs['event_exceptions_df']

    if not event_exceptions.empty:
        for ind in event_exceptions.index:
            messages.append({'type': 'Excepcion',
                            'message': f'ID: {event_exceptions["exception_id"][ind]}. Agencia: {event_exceptions["agency_name"][ind]}. Descripcion: {event_exceptions["description"][ind]}.',
                             'date': parse_date(event_exceptions['created_at'][ind]),
                             'user': ''})
            if event_exceptions['closed_at'][ind] is not None:
                messages.append({'type': 'Cierre de Excepcion',
                                 'message': f'ID: {event_exceptions["exception_id"][ind]}. Agencia: {event_exceptions["agency_name"][ind]}.',
                                 'date': parse_date(event_exceptions['closed_at'][ind]),
                                 'user': ''})
    # Obtiene los tickets dentro del tiempo de vida del evento
    event_tickets = dfs['event_tickets_df']
    if not event_tickets.empty:
        for ticket_index in event_tickets.index:
            ticket = event_tickets.loc[ticket_index].to_dict()
            if ticket['requested_at'] is not None:
                messages.append({'type': 'Solicitud de ticket',
                                'message': "Ticket solicitado",
                                 'date': parse_date(ticket['requested_at']),
                                 'user': ticket['user_mail']})
            if ticket['created_at'] is not None:
                messages.append({'type': 'Creación de ticket',
                                'message': f"Ticket creado con folio {ticket['ticket_id']}",
                                 'date': parse_date(ticket['created_at']),
                                 'user': ticket['user_mail']})
            ticket_detail = await cassia_gs_tickets_repository.get_ticket_detail_by_ticket_id_pool(ticket['ticket_id'], db)
            for ind in ticket_detail.index:
                if ticket_detail['status'][ind] == 'creado':
                    tipo = "Ticket - Nota interna" if ticket_detail['type'][
                        ind] == "ticketcommentinternalnote" else "Ticket - Avance y solución" if ticket_detail['type'][ind] == "ticketcommentprogresssolution" else "Ticket - Cambio de estatus" if ticket_detail['type'][ind] == 'ticketstatuschange' else 'Comentario'
                    messages.append({'type': tipo,
                                    'message': ticket_detail['comment'][ind],
                                     'date': parse_date(ticket_detail['created_at'][ind]),
                                     'user': ticket_detail['user_email'][ind]})
    event_resets = dfs['event_resets_df']

    if not event_resets.empty:
        for ind in event_resets.index:
            messages.append({'type': 'Reset',
                            'message': f'Estatus inicial: {event_resets["initial_status"][ind]}. Resultado: {event_resets["result"][ind]}',
                             'date': parse_date(event_resets['date'][ind]),
                             'user': event_resets['mail'][ind]})
    messages = sorted(messages, key=lambda x: parse_date(
        x["date"]) or datetime.min)
    for message in messages:
        if message["date"]:
            message["date"] = format_date(message["date"])

    acumulated_cassia = round(diff.days*24 + diff.seconds/3600, 4)
    messages.append({'type': 'Progreso del evento a la fecha.',
                     'message': f'Acumulado del evento: {acumulated_cassia} hrs.',
                     'date': now_str,
                     'user': None})
    return success_response(data=messages)


async def register_ack(eventid, message, current_session, close, is_zabbix_event):

    if int(is_zabbix_event):

        event = await ZabbixEventRepository.get_zabbix_event(eventid)
        if event.empty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The eventid not exists",
            )
        created_acknowledge = await AcknowledgesRepository.create_zabbix_acknowledge(eventid, message, current_session, close)
        message = "Acknowledge creado correctamente" if created_acknowledge else "Error al crear Acknowledge"
        if created_acknowledge:
            is_gs_tickets_active = await CassiaConfigRepository.get_config_value_by_name(
                'gs_tickets')
            if is_gs_tickets_active.empty:
                is_gs_tickets_active = 0
            else:
                is_gs_tickets_active = is_gs_tickets_active['value'][0]
            if is_gs_tickets_active:
                active_tickets = await cassia_gs_tickets_repository.get_active_tickets_by_hostid(event['hostid'][0])
                if not active_tickets.empty:
                    ticket_data = {
                        "ticketId": int(active_tickets['ticket_id'][0]),
                        "comment": message,
                        "engineer": current_session.mail,
                    }
                    print(ticket_data)
                    created_ticket_comment = await cassia_gs_tickets_repository.create_ticket_comment_avance_solucion(ticket_data)
                    if created_ticket_comment is not False:
                        print(created_ticket_comment)
                        save_ticket_data = await cassia_gs_tickets_repository.save_ticket_comment_avance_solucion(ticket_data, created_ticket_comment, current_session.mail, active_tickets['cassia_gs_tickets_id'][0])
                        print(save_ticket_data)
        return success_response(message=message)
    else:

        return await register_ack_cassia(eventid, message, current_session, close)


async def register_ack_cassia(eventid, message, current_session, close):

    event = await CassiaEventRepository.get_cassia_event(eventid)

    if event.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="El evento de CASSIA no existe",
        )

    created_acknowledge = await AcknowledgesRepository.create_cassia_event_acknowledge(eventid, message, current_session, close)
    """ TICKETS_PROGRESS_SOLUTION """
    if created_acknowledge:
        is_gs_tickets_active = await CassiaConfigRepository.get_config_value_by_name(
            'gs_tickets')
        if is_gs_tickets_active.empty:
            is_gs_tickets_active = 0
        else:
            is_gs_tickets_active = is_gs_tickets_active['value'][0]
        if is_gs_tickets_active:
            active_tickets = await cassia_gs_tickets_repository.get_active_tickets_by_hostid(event['hostid'][0])
            if not active_tickets.empty:
                ticket_data = {
                    "ticketId": int(active_tickets['ticket_id'][0]),
                    "comment": message,
                    "engineer": current_session.mail,
                }
                print(ticket_data)
                created_ticket_comment = await cassia_gs_tickets_repository.create_ticket_comment_avance_solucion(ticket_data)
                if created_ticket_comment is not False:
                    print(created_ticket_comment)
                    save_ticket_data = await cassia_gs_tickets_repository.save_ticket_comment_avance_solucion(ticket_data, created_ticket_comment, current_session.mail, active_tickets['cassia_gs_tickets_id'][0])
                    print(save_ticket_data)

    message = "Acknowledge de CASSIA creado correctamente" if created_acknowledge else "Error al crear Acknowledge de CASSIA"
    return success_response(message=message)
