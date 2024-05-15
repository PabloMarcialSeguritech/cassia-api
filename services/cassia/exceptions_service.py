from utils.settings import Settings
from fastapi.exceptions import HTTPException
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
from utils.traits import success_response
from infraestructure.cassia import cassia_exception_agencies_repository
from infraestructure.cassia import cassia_exceptions_repository
from fastapi import status
settings = Settings()


async def get_exception_agencies_async():
    exception_agencies = await cassia_exception_agencies_repository.get_cassia_exception_agencies()
    if not exception_agencies.empty:
        exception_agencies["id"] = exception_agencies["exception_agency_id"]
    return success_response(data=exception_agencies.to_dict(orient="records"))


async def create_exception_agency_async(exception_agency: exception_agency_schema.CassiaExceptionAgencyBase):
    exception_agency = await cassia_exception_agencies_repository.create_cassia_exception_agency(exception_agency)
    return success_response(message="Registro guardado correctamente",
                            success=True,
                            data=exception_agency,
                            status_code=status.HTTP_201_CREATED)


async def update_exception_agency_async(exception_agency_id: int, exception_agency_data: exception_agency_schema.CassiaExceptionAgencyBase):
    exception_agency = await cassia_exception_agencies_repository.get_cassia_exception_agency_by_id(exception_agency_id)
    if exception_agency.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el registro de Exception Agency"
        )
    result = await cassia_exception_agencies_repository.update_cassia_exception_agency(exception_agency_id, exception_agency_data)
    exception_agency = await cassia_exception_agencies_repository.get_cassia_exception_agency_by_id(exception_agency_id)
    return success_response(message="Exception Agency Actualizada",
                            data=exception_agency.to_dict(orient="records")[0])


async def delete_exception_agency_async(exception_agency_id: int):
    exception_agency = await cassia_exception_agencies_repository.get_cassia_exception_agency_by_id(exception_agency_id)
    if exception_agency.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el registro de Exception Agency"
        )
    result = await cassia_exception_agencies_repository.delete_cassia_exception_agency(exception_agency_id)
    message = "Exception Agency Eliminada" if result else "Error al eliminar Exception Agency"
    return success_response(message=message)


async def get_exceptions_count(municipalityId, dispId):
    exceptions_count = await cassia_exceptions_repository.get_cassia_exceptions_count(municipalityId, dispId)
    return success_response(data=exceptions_count.to_dict(orient='records'))


async def get_exceptions_async():
    exceptions = await cassia_exceptions_repository.get_cassia_exceptions()
    return success_response(data=exceptions.to_dict(orient='records'))


async def create_exception_async(exception: exception_schema.CassiaExceptionsBase, current_user_session):
    host = await cassia_exceptions_repository.get_host_by_id(exception.hostid)
    if host.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El host con el id proporcionado no existe"
        )
    exception_dict = exception.dict()
    exception_dict['session_id'] = current_user_session
    exception_dict['closed_at'] = None
    exception = await cassia_exceptions_repository.create_cassia_exception(exception_dict)

    return success_response(message="Excepcion creada correctamente",
                            data=exception,
                            status_code=status.HTTP_201_CREATED)


async def close_exception_async(exception_id, exception_data, current_user_session):
    exception = await cassia_exceptions_repository.get_cassia_exception_by_id(exception_id)
    if exception.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"La Excepcion con el id proporcionado no existe"
        )
    result = await cassia_exceptions_repository.close_cassia_exception_by_id(exception_id, exception_data.closed_at)
    message = "Excepcion cerrada correctamente" if result else "Error al cerrar la excepcion"
    exception = await cassia_exceptions_repository.get_cassia_exception_by_id(exception_id)
    print(message)
    print(exception)
    return success_response(message=message,
                            data=exception.to_dict(orient="records")[0],
                            status_code=status.HTTP_200_OK)
