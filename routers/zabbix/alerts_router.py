from fastapi import APIRouter
import services.zabbix.alerts_service as alerts_service
import schemas.exception_agency_schema as exception_agency_schemas
import schemas.exceptions_schema as exception_schema
from fastapi import Depends, status
from services import auth_service
from fastapi import Body
from models.user_model import User
alerts_router = APIRouter()


@alerts_router.get(
    '/problems/{municipalityId}',
    tags=["Zabbix - Problems(Alerts)"],
    status_code=status.HTTP_200_OK,
    summary="Get problems by municipality ID, technology, and dispId",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_problems_filter(municipalityId: str, tech: str = "", hostType: str = ""):
    return alerts_service.get_problems_filter(municipalityId, tech, hostType)


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
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_agencies():
    return alerts_service.get_exception_agencies()


@alerts_router.post(
    '/exception_agencies',
    tags=["Zabbix - Problems(Alerts) - Exception Agencies"],
    status_code=status.HTTP_200_OK,
    summary="Create an Exception Agency",
    dependencies=[Depends(auth_service.get_current_user)]
)
def create_agency(exception_agency:  exception_agency_schemas.ExceptionAgencyBase = Body(...)):
    return alerts_service.create_exception_agency(exception_agency=exception_agency)


@alerts_router.put(
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


""" Exception Agencies """


@alerts_router.get(
    '/exceptions',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Get all Exceptions",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_agencies():
    return alerts_service.get_exceptions()


@alerts_router.post(
    '/exceptions',
    tags=["Zabbix - Problems(Alerts) - Exceptions"],
    status_code=status.HTTP_200_OK,
    summary="Create an Exception",
    dependencies=[Depends(auth_service.get_current_user)]
)
def create_agency(exception: exception_schema.ExceptionsBase = Body(...), current_user: User = Depends(auth_service.get_current_user)):
    return alerts_service.create_exception(exception=exception, current_user_id=current_user.user_id)


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
