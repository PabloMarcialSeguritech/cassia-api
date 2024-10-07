from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from services.cassia import cassia_gs_tickets_service
from schemas.cassia_gs_ticket_schema import CassiaGSTicketSchema, CassiaGSTicketCommentSchema, CassiaGSTicketCancelSchema
cassia_gs_tickets_router = APIRouter(prefix="/gs/tickets")


@cassia_gs_tickets_router.get(
    '/detail/{ticket_id}',
    tags=["Cassia - GS - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Create ticket in host",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_ticket_detail(ticket_id):
    return await cassia_gs_tickets_service.get_ticket_detail(ticket_id)


@cassia_gs_tickets_router.get(
    '/',
    tags=["Cassia - GS - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Get active GS Tickets",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_created_tickets():
    return await cassia_gs_tickets_service.get_created_tickets()


@cassia_gs_tickets_router.post(
    '/',
    tags=["Cassia - GS - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Create ticket in host",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_ticket(ticket_data: CassiaGSTicketSchema, current_session=Depends(auth_service2.get_current_user_session)):
    return await cassia_gs_tickets_service.create_ticket(ticket_data, current_session)


@cassia_gs_tickets_router.post(
    '/comment',
    tags=["Cassia - GS - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Create comment in a ticket",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_ticket_comment(ticket_data: CassiaGSTicketCommentSchema, current_session=Depends(auth_service2.get_current_user_session)):
    return await cassia_gs_tickets_service.create_ticket_comment(ticket_data, current_session)


@cassia_gs_tickets_router.post(
    '/cancel',
    tags=["Cassia - GS - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Create ticket in host",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def cancel_ticket(ticket_data: CassiaGSTicketCancelSchema, current_session=Depends(auth_service2.get_current_user_session)):
    return await cassia_gs_tickets_service.cancel_ticket(ticket_data, current_session)
