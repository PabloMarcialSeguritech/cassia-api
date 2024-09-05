from fastapi import HTTPException, status
from infraestructure.cassia import cassia_gs_tickets_repository
from utils.traits import success_response
from schemas import cassia_gs_ticket_schema
from infraestructure.cassia import CassiaConfigRepository


async def get_ticket_detail(ticket_id):
    created_ticket = await cassia_gs_tickets_repository.get_ticket_by_ticket_id(ticket_id)
    ticket_detail = await cassia_gs_tickets_repository.get_ticket_detail_by_ticket_id(ticket_id)
    if created_ticket.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Ticket no encontrado")
    response = created_ticket.loc[0].to_dict()
    ticket_detail_array = []
    for ind in ticket_detail.index:
        ticket_detail_array.append(ticket_detail.loc[ind].to_dict())
    response['ticket_detail'] = ticket_detail_array
    return success_response(data=response)


async def get_created_tickets():
    active_tickets = await cassia_gs_tickets_repository.get_active_tickets()
    return success_response(data=active_tickets.to_dict(orient='records'))


async def create_ticket(ticket_data: cassia_gs_ticket_schema.CassiaGSTicketSchema, current_session):
    host_data = await cassia_gs_tickets_repository.get_host_data(ticket_data.hostid)

    if host_data.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Host no encontrado")
    suscriptores_id_df = await CassiaConfigRepository.get_config_value_by_name('suscriptores_id')
    suscriptores_id = 11
    if not suscriptores_id_df.empty:
        suscriptores_id = int(suscriptores_id_df['value'][0])
    print(host_data)
    if host_data['device_id'][0] != suscriptores_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Este host no es un suscriptor por lo que no se pueden crear tickets en el.")
    if host_data['alias'][0] == "" or host_data['alias'][0] is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="La afiliación de este host no esta configurada")
    if host_data['hardware_no_serie'][0] == "" or host_data['hardware_no_serie'][0] is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El numero de serie de este host no esta configurado")
    created_tickets_to_afiliation = await cassia_gs_tickets_repository.get_active_tickets_by_afiliation(
        host_data['alias'][0])
    if not created_tickets_to_afiliation.empty:
        print(created_tickets_to_afiliation)
        message = 'Hay una solicitud de ticket activa en esta afiliación' if created_tickets_to_afiliation[
            'status'][0] == 'solicitado' else 'Ya existe un ticket activo en esta afiliación'
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=message)
    host_data_dict = host_data.loc[0].to_dict()
    mac_address_df = await cassia_gs_tickets_repository.get_mac_address_by_hostid(ticket_data.hostid)
    mac_address = ''
    if not mac_address_df.empty:
        mac_address = mac_address_df['item_value'][0]
        mac_address = mac_address.replace(" ", "").upper()
        mac_address = mac_address.replace("-", "")
        mac_address = mac_address.replace(":", "")
    host_data_dict['mac_address'] = mac_address
    host_data_dict['service_id'] = 1866 if 'VVL' in host_data_dict['alias'] else 1836
    created_ticket = await cassia_gs_tickets_repository.create_ticket(host_data_dict, ticket_data.comment, current_session.mail)
    save_ticket_data = await cassia_gs_tickets_repository.save_ticket_data(ticket_data, host_data_dict, created_ticket, current_session)
    return success_response(message="Ticket solicitado correctamente")


async def create_ticket_comment(ticket_data: cassia_gs_ticket_schema.CassiaGSTicketCommentSchema, current_session):
    created_ticket = await cassia_gs_tickets_repository.get_ticket_by_ticket_id(ticket_data.ticket_id)
    if created_ticket.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="No existe el ticket con el id proporcionado")
    created_ticket_gs_id = created_ticket['cassia_gs_tickets_id'][0]
    created_ticket_comment = await cassia_gs_tickets_repository.create_ticket_comment(ticket_data, current_session.mail)
    save_ticket_data = await cassia_gs_tickets_repository.save_ticket_comment_data(ticket_data, created_ticket_comment, current_session.mail, created_ticket_gs_id)
    return success_response(message="Comentario de ticket solicitado correctamente")
