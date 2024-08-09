from infraestructure.cassia import CassiaEventRepository
from infraestructure.zabbix import ZabbixEventRepository
from infraestructure.zabbix import AcknowledgesRepository
from infraestructure.cassia import cassia_gs_tickets_repository
from utils.traits import success_response, get_datetime_now_str_with_tz, get_datetime_now_with_tz
from fastapi import HTTPException, status
from datetime import datetime
import pytz
import pandas as pd


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
            return None


async def get_acks(eventid, is_cassia_event):
    if int(is_cassia_event):
        # Obtiene el evento de cassia
        event = await CassiaEventRepository.get_cassia_event(eventid)
    else:
        event = await ZabbixEventRepository.get_zabbix_event(eventid)
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
    acumulated_cassia = round(diff.days*24 + diff.seconds/3600, 4)
    messages.append({'type': 'Creación de evento',
                     'message': 'Creación del evento.',
                     'date': clock_problem,
                     'user': None})
    messages.append({'type': 'Progreso del evento a la fecha.',
                     'message': f'Acumulado del evento: {acumulated_cassia} hrs.',
                     'date': now_str,
                     'user': None})
    # Obtiene los acknowledges del evento

    event_acknowledges = await AcknowledgesRepository.get_acknowledges(eventid, is_cassia_event)
    for ind in event_acknowledges.index:
        messages.append({'type': 'Acknowledge',
                         'message': event_acknowledges['message'][ind],
                         'date': event_acknowledges['Time'][ind],
                         'user': event_acknowledges['user'][ind]})
    event_tickets = await cassia_gs_tickets_repository.get_active_tickets_by_afiliation(event['alias'][0])
    if not event_tickets.empty:
        ticket = event_tickets.loc[0].to_dict()
        if ticket['requested_at'] is not None:
            messages.append({'type': 'Solicitud de ticket',
                             'message': "Ticket solicitado",
                             'date': ticket['requested_at'],
                             'user': ticket['user_mail']})
        if ticket['created_at'] is not None:
            messages.append({'type': 'Creación de ticket',
                             'message': f"Ticket creado con folio {ticket['ticket_id']} con correo {ticket['created_with_mail']}",
                             'date': ticket['created_at'],
                             'user': ticket['user_mail']})
        ticket_detail = await cassia_gs_tickets_repository.get_ticket_detail_by_ticket_id(ticket['ticket_id'])
        for ind in ticket_detail.index:
            if ticket_detail['status'][ind] == 'creado':
                tipo = "Nota interna" if ticket_detail['type'][
                    ind] == "ticketcommentinternalnote" else "Avance y solución" if ticket_detail['type'][ind] == "ticketcommentprogresssolution" else "Comentario"
                messages.append({'type': 'Comentario de ticket',
                                 'message': f"Tipo: {tipo} - Comentario: {ticket_detail['comment'][ind]}",
                                 'date': ticket_detail['created_at'][ind],
                                 'user': ticket_detail['user_email'][ind]})

    messages = sorted(messages, key=lambda x: parse_date(
        x["date"]) or datetime.min)
    return success_response(data=messages)

    # Obtiene los tickets del evento
    print("auiiiiiii")
    print(event)

    tickets = await AcknowledgesRepository.get_event_tickets(eventid, is_cassia_event)
    # Procesa los acknowledges y tickets para obtener el acmulado de cada uno hasta hoy y retorna un diccionario
    resume = await AcknowledgesRepository.process_acknowledges_tickets(event, event_acknowledges, tickets, is_cassia_event)
    response = dict()
    response.update(resume)
    response.update({'history': event_acknowledges.to_dict(orient="records")})
    response.update({'tickets': tickets.to_dict(orient='records')})

    return success_response(message="success", data=response)


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
    message = "Acknowledge de CASSIA creado correctamente" if created_acknowledge else "Error al crear Acknowledge de CASSIA"
    return success_response(message=message)
