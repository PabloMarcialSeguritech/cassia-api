from fastapi import HTTPException, status
from infraestructure.cassia import cassia_gs_tickets_repository
from utils.traits import success_response
from schemas import cassia_gs_ticket_schema


async def create_ticket(ticket_data: cassia_gs_ticket_schema.CassiaGSTicketSchema, mail):
    host_data = await cassia_gs_tickets_repository.get_host_data(ticket_data.hostid)
    if host_data.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Host no encontrado")

    created_tickets_to_afiliation = await cassia_gs_tickets_repository.get_active_tickets_by_afiliation(
        host_data['alias'][0])
    if not created_tickets_to_afiliation.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Ya existe un ticket activo en esta afiliación")

    created_ticket = await cassia_gs_tickets_repository.create_ticket(host_data.loc[0].to_dict(), ticket_data.comment, mail)
    return success_response(data=created_ticket)


async def create_ticket_comment(ticket_data: cassia_gs_ticket_schema.CassiaGSTicketCommentSchema, mail):
    created_ticket = await cassia_gs_tickets_repository.get_ticket_by_ticket_id(ticket_data.ticket_id)
    if created_ticket.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="No existe el ticket con el id proporcionado")

    created_ticket = await cassia_gs_tickets_repository.create_ticket_comment(ticket_data, mail)
    return success_response(data=created_ticket)
