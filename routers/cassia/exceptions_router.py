from fastapi import APIRouter
import services.cassia.exceptions_service as exceptions_service
import schemas.exception_agency_schema as exception_agency_schemas
import schemas.exceptions_schema as exception_schema
from fastapi import Depends, status
from services import auth_service2
from fastapi import Body
from models.cassia_user_session import CassiaUserSession
from infraestructure.database import DB
from dependencies import get_db
exceptions_router = APIRouter()

""" Exception Agencies """


@exceptions_router.get(
    '/exception_agencies',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Get all exception agencies",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_agencies_async(db: DB = Depends(get_db)):
    return await exceptions_service.get_exception_agencies_async(db)


@exceptions_router.post(
    '/exception_agencies',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Create an Exception Agency",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_agency_async(exception_agency: exception_agency_schemas.CassiaExceptionAgencyBase = Body(...)):
    return await exceptions_service.create_exception_agency_async(exception_agency=exception_agency)


@exceptions_router.put(
    '/exception_agencies/{exception_agency_id}',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Update an Exception Agency with the id given",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_exception_agency_async(exception_agency_id,
                                        exception_agency: exception_agency_schemas.CassiaExceptionAgencyBase = Body(
                                            ...)):
    return await exceptions_service.update_exception_agency_async(exception_agency_id=exception_agency_id,
                                                                  exception_agency_data=exception_agency)


@exceptions_router.delete(
    '/exception_agencies/{exception_agency_id}',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Delete an Exception Agency with the id given",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_exception_agency(exception_agency_id):
    return await exceptions_service.delete_exception_agency_async(exception_agency_id=exception_agency_id)


""" Exceptions """


@exceptions_router.get(
    '/exceptions/count/{municipalityId}',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Get all Exceptions",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_exceptions_async(municipalityId: str = "0", dispId: str = "0", db: DB = Depends(get_db)):
    return await exceptions_service.get_exceptions_count(municipalityId, dispId, db)


@exceptions_router.get(
    '/exceptions/{municipalityId}',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Get all Exceptions",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_exceptions_async(municipalityId: str = "0", dispId: str = "0", db: DB = Depends(get_db)):
    return await exceptions_service.get_exceptions_async(municipalityId, dispId, db)


@exceptions_router.post(
    '/exceptions',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Create an Exception",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_exception_async(exception: exception_schema.CassiaExceptionsBase = Body(...),
                                 current_user_session: CassiaUserSession = Depends(
                                     auth_service2.get_current_user_session)):
    return await exceptions_service.create_exception_async(exception=exception,
                                                           current_user_session=current_user_session)


@exceptions_router.post(
    '/exceptions/close/{exception_id}',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Close an Exception",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def close_exception_async(exception_id, exception_data: exception_schema.CassiaExceptionsClose = Body(...),
                                current_user_session: CassiaUserSession = Depends(
                                    auth_service2.get_current_user_session)):
    return await exceptions_service.close_exception_async(exception_id=exception_id, exception_data=exception_data,
                                                          current_user_session=current_user_session.session_id.hex)


@exceptions_router.get(
    '/exceptions/list/detail',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Get all Exceptions",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_exceptions_detail_async():
    return await exceptions_service.get_exceptions_detail_async()


@exceptions_router.post(
    '/exceptions/update',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Update an Exception",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_exception_async(exception_data: exception_schema.CassiaExceptions = Body(...),
                                 current_user_session: CassiaUserSession = Depends(
                                     auth_service2.get_current_user_session)):

    response = await exceptions_service.update_exception_async(exception=exception_data,
                                                               current_user_session=current_user_session.session_id.hex)
    return response


@exceptions_router.delete(
    '/exceptions/{exception_id}',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Delete an Exception",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_exception_async(exception_id):
    response = await exceptions_service.delete_exception_async(exception_id)
    return response
