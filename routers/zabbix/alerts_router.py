from fastapi import APIRouter
import services.zabbix.alerts_service as alerts_service
import schemas.exception_agency_schema as exception_agency_schemas
import schemas.exceptions_schema as exception_schema
import schemas.problem_record_schema as problem_record_schema
import schemas.problem_record_history_schema as problem_records_history_schema
import schemas.cassia_ticket_schema as cassia_ticket_schema
from fastapi import Depends, status
from services import auth_service
from services import auth_service2
from fastapi import Body
from models.user_model import User
from models.cassia_user_session import CassiaUserSession
from fastapi import File, UploadFile, Form
from fastapi.responses import FileResponse
from typing import Optional

alerts_router = APIRouter()


@alerts_router.get(
    '/problems/{municipalityId}',
    tags=["Zabbix - Problems(Alerts)"],
    status_code=status.HTTP_200_OK,
    summary="Get problems by municipality ID, device type and technology, and subtype",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_problems_filter(municipalityId: str, tech_host_type: str = "", subtype: str = "", severities: str = ""):
    return alerts_service.get_problems_filter(municipalityId, tech_host_type, subtype, severities)


@alerts_router.get(
    '/problems/download/{municipalityId}',
    tags=["Zabbix - Problems(Alerts)"],
    status_code=status.HTTP_200_OK,
    summary="Get problems by municipality ID, device type and technology, and subtype in Excel",
    response_class=FileResponse,
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_problems_filter(municipalityId: str, tech_host_type: str = "", subtype: str = "", severities: str = ""):
    return alerts_service.get_problems_filter_report(municipalityId, tech_host_type, subtype, severities)


@alerts_router.post(
    '/problems/acknowledge/{eventid}',
    tags=["Zabbix - Problems(Alerts) - Acknowledge"],
    status_code=status.HTTP_200_OK,
    summary="Register a acknowledge in event, Ex: 34975081",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_problems_filter(eventid: str = "34975081", message: str = Form(max_length=2048), close: bool = Form(...),
                              current_user_session: CassiaUserSession = Depends(
                                  auth_service2.get_current_user_session)):
    return await alerts_service.register_ack(eventid, message, current_user_session, close, is_zabbix_event=1)


@alerts_router.get(
    '/problems/acknowledge/{eventid}',
    tags=["Zabbix - Problems(Alerts) - Acknowledge"],
    status_code=status.HTTP_200_OK,
    summary="Get acknowledges of one event, Ex: 34975081",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_problems_filter(eventid: str = "34975081"):
    return await alerts_service.get_acks(eventid)


@alerts_router.get(
    '/problems/tickets/{eventid}',
    tags=["Zabbix - Problems(Alerts) - Acknowledge - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Get tickets of one event, Ex: 34975081",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_tickets_filter(eventid: str = "34975081"):
    return await alerts_service.get_tickets(eventid)


@alerts_router.post(
    '/problems/tickets/link',
    tags=["Zabbix - Problems(Alerts) - Acknowledge - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Link ticket to one event",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def link_ticket(ticket_data: cassia_ticket_schema.CassiaTicketBase,
                      current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    return await alerts_service.link_ticket(ticket_data, current_user_session)


@alerts_router.delete(
    '/problems/tickets/link/{ticket_id}',
    tags=["Zabbix - Problems(Alerts) - Acknowledge - Tickets"],
    status_code=status.HTTP_200_OK,
    summary="Delete ticket linked to one event",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def link_ticket(ticket_id):
    return await alerts_service.delete_ticket(ticket_id)


""" @alerts_router.get(
    '/problems/{municipalityId}',
    tags=["Zabbix - Problems(Alerts)"],
    status_code=status.HTTP_200_OK,
    summary="Get problems by municipality ID, technology, and dispId",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_problems_filter(municipalityId: str, tech: str = "", hostType: str = ""):
    return zabbix_service.get_problems_filter_api(municipalityId, tech, hostType) """

""" Exception Agencies """


@alerts_router.get(
    '/exception_agencies',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Get all exception agencies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def get_agencies():
    return alerts_service.get_exception_agencies()


@alerts_router.post(
    '/exception_agencies',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Create an Exception Agency",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def create_agency(exception_agency: exception_agency_schemas.CassiaExceptionAgencyBase = Body(...)):
    return alerts_service.create_exception_agency(exception_agency=exception_agency)


@alerts_router.put(
    '/exception_agencies/{exception_agency_id}',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Update an Exception Agency with the id given",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def create_agency(exception_agency_id,
                  exception_agency: exception_agency_schemas.CassiaExceptionAgencyBase = Body(...)):
    return alerts_service.update_exception_agency(exception_agency_id=exception_agency_id,
                                                  exception_agency=exception_agency)


@alerts_router.delete(
    '/exception_agencies/{exception_agency_id}',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Delete an Exception Agency with the id given",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def create_agency(exception_agency_id):
    return alerts_service.delete_exception_agency(exception_agency_id=exception_agency_id)


""" Exception Agencies """


@alerts_router.get(
    '/exceptions',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Get all Exceptions",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_agencies():
    return await alerts_service.get_exceptions()


@alerts_router.post(
    '/exceptions',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Create an Exception",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_exception(exception: exception_schema.CassiaExceptionsBase = Body(...),
                           current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    return await alerts_service.create_exception(exception=exception,
                                                 current_user_session=current_user_session.session_id.hex)


@alerts_router.post(
    '/exceptions/close/{exception_id}',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Close an Exception",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def close_exception(exception_id, exception_data: exception_schema.CassiaExceptionsClose = Body(...),
                          current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    return await alerts_service.close_exception(exception_id=exception_id, exception_data=exception_data,
                                                current_user_session=current_user_session.session_id.hex)


@alerts_router.post(
    '/problemrecords/change-status/{problemid}',
    tags=["Zabbix - Problems(Alerts) - ProblemRecords"],
    status_code=status.HTTP_200_OK,
    summary="Change status of one Problem Record",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
def create_agency(problemid: int, estatus: problem_record_schema.ProblemRecordBase = Body(...),
                  current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session)):
    return alerts_service.change_status(estatus=estatus.estatus, problemid=problemid,
                                        current_user_id=current_user_session.user_id)


""" @alerts_router.put(
    '/exception_agencies/{exception_agency_id}',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Update an Exception Agency with the id given",
    dependencies=[Depends(auth_service.get_current_user)]
)
def create_agency(exception_agency_id, exception_agency:  exception_agency_schemas.ExceptionAgencyBase = Body(...)):
    return alerts_service.update_exception_agency(exception_agency_id=exception_agency_id, exception_agency=exception_agency)


@alerts_router.delete(
    '/exception_agencies/{exception_agency_id}',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Delete an Exception Agency with the id given",
    dependencies=[Depends(auth_service.get_current_user)]
)
def create_agency(exception_agency_id):
    return alerts_service.delete_exception_agency(exception_agency_id=exception_agency_id)
 """


@alerts_router.post(
    '/problemrecords/history/{problemid}',
    tags=["Zabbix - Problems(Alerts) - ProblemRecords"],
    status_code=status.HTTP_200_OK,
    summary="Register a message or log in problem record",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_message(problemid: int, message: Optional[str] = Form(None),
                         current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session),
                         file: UploadFile | None = None):
    return await alerts_service.create_message(problemid=problemid, current_user_id=current_user_session.user_id,
                                               message=message, file=file)


@alerts_router.get(
    '/problemrecords/history/{problemid}',
    tags=["Zabbix - Problems(Alerts) - ProblemRecords"],
    status_code=status.HTTP_200_OK,
    summary="Get all messages or log of one problem record",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_messages(problemid: int):
    return await alerts_service.get_messages(problemid=problemid)


@alerts_router.get(
    '/problemrecords/history/files/download/{message_id}',
    tags=["Zabbix - Problems(Alerts) - ProblemRecords"],
    status_code=status.HTTP_200_OK,
    summary="Return file upload in history",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def download_file(message_id: str):
    return await alerts_service.download_file(message_id=message_id)


@alerts_router.post(
    '/problems/acknowledge_/{eventid}',
    tags=["Zabbix - Problems(Alerts) - Acknowledge"],
    status_code=status.HTTP_200_OK,
    summary="Register a acknowledge in event, Ex: 34975081",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_problems_filter_(eventid: str = "34975081", message: str = Form(max_length=2048), close: bool = Form(...),
                              current_user_session: CassiaUserSession = Depends(auth_service2.get_current_user_session),
                              is_zabbix_event: bool = Form(...)):
    return await alerts_service.register_ack(eventid, message, current_user_session, close, is_zabbix_event)