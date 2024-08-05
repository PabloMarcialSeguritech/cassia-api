from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from services.cassia import cassia_gs_tickets_service
from schemas.cassia_gs_ticket_schema import CassiaGSTicketSchema, CassiaGSTicketCommentSchema
cassia_gs_tickets_router = APIRouter(prefix="/gs/tickets")


@cassia_gs_tickets_router.post(
    '/',
    tags=["Cassia - GS - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Create ticket in host",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_ticket(ticket_data: CassiaGSTicketSchema, current_session=Depends(auth_service2.get_current_user_session)):
    return await cassia_gs_tickets_service.create_ticket(ticket_data, current_session.mail)


@cassia_gs_tickets_router.post(
    '/comment',
    tags=["Cassia - GS - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Create ticket in host",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_ticket_comment(ticket_data: CassiaGSTicketCommentSchema, current_session=Depends(auth_service2.get_current_user_session)):
    return await cassia_gs_tickets_service.create_ticket_comment(ticket_data, current_session.mail)
