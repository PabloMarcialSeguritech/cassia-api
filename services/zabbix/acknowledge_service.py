from infraestructure.cassia import CassiaEventRepository
from infraestructure.zabbix import ZabbixEventRepository
from infraestructure.zabbix import AcknowledgesRepository
from utils.traits import success_response
from fastapi import HTTPException, status


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
    # Obtiene los acknowledges del evento
    event_acknowledges = await AcknowledgesRepository.get_acknowledges(eventid, is_cassia_event)
    # Obtiene los tickets del evento
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
