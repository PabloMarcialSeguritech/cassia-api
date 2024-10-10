from models.cassia_exceptions_async_test import CassiaExceptionsAsyncTest
from utils.settings import Settings
from fastapi.exceptions import HTTPException
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
from utils.traits import success_response, get_datetime_now_with_tz
from infraestructure.cassia import cassia_exception_agencies_repository
from infraestructure.cassia import cassia_exceptions_repository
from infraestructure.cassia import cassia_gs_tickets_repository
from infraestructure.cassia import CassiaConfigRepository
from fastapi import status
import pandas as pd
import pytz
from datetime import datetime
import asyncio
from models.cassia_user_session import CassiaUserSession


settings = Settings()


async def get_exception_agencies_async_backup():
    exception_agencies = await cassia_exception_agencies_repository.get_cassia_exception_agencies()
    if not exception_agencies.empty:
        exception_agencies["id"] = exception_agencies["exception_agency_id"]
    return success_response(data=exception_agencies.to_dict(orient="records"))


async def get_exception_agencies_async(db):
    exception_agencies = await cassia_exception_agencies_repository.get_cassia_exception_agencies_pool(db)
    if not exception_agencies.empty:
        exception_agencies["id"] = exception_agencies["exception_agency_id"]
    return success_response(data=exception_agencies.to_dict(orient="records"))


async def create_exception_agency_async(exception_agency: exception_agency_schema.CassiaExceptionAgencyBase):
    exception_agency = await cassia_exception_agencies_repository.create_cassia_exception_agency(exception_agency)
    return success_response(message="Registro guardado correctamente",
                            success=True,
                            data=exception_agency,
                            status_code=status.HTTP_201_CREATED)


async def update_exception_agency_async(exception_agency_id: int,
                                        exception_agency_data: exception_agency_schema.CassiaExceptionAgencyBase):
    exception_agency = await cassia_exception_agencies_repository.get_cassia_exception_agency_by_id(exception_agency_id)
    if exception_agency.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el registro de Exception Agency"
        )
    result = await cassia_exception_agencies_repository.update_cassia_exception_agency(exception_agency_id,
                                                                                       exception_agency_data)
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


async def get_exceptions_count(municipalityId, dispId, db):
    if int(dispId) == 0:
        dispId = ''
    if dispId == '-1':
        dispId = ''
    exceptions_count = await cassia_exceptions_repository.get_cassia_exceptions_count_pool(municipalityId, dispId, db)
    return success_response(data=exceptions_count.to_dict(orient='records'))


async def get_exceptions_count_backup(municipalityId, dispId):
    if int(dispId) == 0:
        dispId = ''
    if dispId == '-1':
        dispId = ''
    exceptions_count = await cassia_exceptions_repository.get_cassia_exceptions_count(municipalityId, dispId)
    return success_response(data=exceptions_count.to_dict(orient='records'))


async def get_exceptions_async(municipalityId, dispId, db):
    if int(dispId) == 0:
        dispId = ''
    if dispId == '-1':
        dispId = ''
    exceptions = await cassia_exceptions_repository.get_cassia_exceptions_pool(municipalityId, dispId, db)
    return success_response(data=exceptions.to_dict(orient='records'))


async def get_exceptions_async_backup(municipalityId, dispId):
    if int(dispId) == 0:
        dispId = ''
    if dispId == '-1':
        dispId = ''
    exceptions = await cassia_exceptions_repository.get_cassia_exceptions(municipalityId, dispId)
    return success_response(data=exceptions.to_dict(orient='records'))


async def create_exception_async(exception: exception_schema.CassiaExceptionsBase, current_user_session):
    host = await cassia_exceptions_repository.get_host_by_id(exception.hostid)
    if host.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El host con el id proporcionado no existe"
        )
    """ current_time = get_datetime_now_with_tz() """
    exception_dict = exception.dict()
    exception_dict['session_id'] = current_user_session.session_id.hex
    exception_dict['closed_at'] = None
    exception_dict['created_at'] = exception.created_at
    exception = await cassia_exceptions_repository.create_cassia_exception(exception_dict)
    """ TICKETS_PROGRESS_SOLUTION """
    if exception is not None:
        is_gs_tickets_active = await CassiaConfigRepository.get_config_value_by_name(
            'gs_tickets')
        if is_gs_tickets_active.empty:
            is_gs_tickets_active = 0
        else:
            is_gs_tickets_active = is_gs_tickets_active['value'][0]
        if is_gs_tickets_active:
            active_tickets = await cassia_gs_tickets_repository.get_active_tickets_by_hostid(exception.hostid)
            if not active_tickets.empty:
                ticket_data = {
                    "ticketId": int(active_tickets['ticket_id'][0]),
                    "comment": exception.description,
                    "engineer": current_user_session.mail,
                }
                print(ticket_data)
                created_ticket_comment = await cassia_gs_tickets_repository.create_ticket_comment_avance_solucion(ticket_data)
                if created_ticket_comment is not False:
                    print(created_ticket_comment)
                    save_ticket_data = await cassia_gs_tickets_repository.save_ticket_comment_avance_solucion(ticket_data, created_ticket_comment, current_user_session.mail, active_tickets['cassia_gs_tickets_id'][0])
                    print(save_ticket_data)

    return success_response(message="Excepcion creada correctamente",
                            data=exception,
                            status_code=status.HTTP_201_CREATED)


async def save_exception(exception, hostid, current_user_session, db):
    exception_dict = exception.dict()
    exception_dict.pop('hostids')
    exception_dict['session_id'] = current_user_session.session_id.hex
    exception_dict['created_at'] = get_datetime_now_with_tz()
    exception_dict['updated_at'] = get_datetime_now_with_tz()
    exception_dict['init_date'] = exception.init_date
    exception_dict['end_date'] = exception.end_date
    exception_dict['hostid'] = hostid

    exception = await cassia_exceptions_repository.create_cassia_exception2(exception_dict, db)
    """ TICKETS_PROGRESS_SOLUTION """
    if exception is not None:
        is_gs_tickets_active = await CassiaConfigRepository.get_config_value_by_name(
            'gs_tickets')
        if is_gs_tickets_active.empty:
            is_gs_tickets_active = 0
        else:
            is_gs_tickets_active = is_gs_tickets_active['value'][0]
        if is_gs_tickets_active:
            active_tickets = await cassia_gs_tickets_repository.get_active_tickets_by_hostid(exception.hostid)
            if not active_tickets.empty:
                ticket_data = {
                    "ticketId": int(active_tickets['ticket_id'][0]),
                    "comment": exception.description,
                    "engineer": current_user_session.mail,
                }
                print(ticket_data)
                created_ticket_comment = await cassia_gs_tickets_repository.create_ticket_comment_avance_solucion(ticket_data)
                if created_ticket_comment is not False:
                    print(created_ticket_comment)
                    save_ticket_data = await cassia_gs_tickets_repository.save_ticket_comment_avance_solucion(ticket_data, created_ticket_comment, current_user_session.mail, active_tickets['cassia_gs_tickets_id'][0])
                    print(save_ticket_data)
    else:
        return {'hostid': hostid,
                'result': False}
    return {'hostid': hostid,
            'result': True}


async def create_exception2_async(exception: exception_schema.CassiaExceptions2Base, current_user_session, db):
    hostids_list_str = ",".join(list(map(str, exception.hostids)))
    hosts = await cassia_exceptions_repository.get_hosts_by_ids(hostids_list_str, db)
    if hosts.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"No existe ningun host con los ids proporcionados"
        )
    hosts_existentes = hosts['hostid'].to_list()
    host_no_existentes = list(set(exception.hostids)-set(hosts_existentes))
    if len(host_no_existentes) > 0:
        hosts_no_existentes_str = ", ".join(list(map(str, host_no_existentes)))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Los siguientes hosts no existen: {hosts_no_existentes_str}"
        )
    existen_excepciones = await cassia_exceptions_repository.get_active_exceptions_by_hostids(hostids_list_str,  db)
    if not existen_excepciones.empty:
        hosts_con_excepcion_activa_str = ", ".join(
            list(map(str, existen_excepciones['hostid'])))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Los siguientes hosts ya se encuentran en una excepcion: {hosts_con_excepcion_activa_str}"
        )

    existen_mantenimientos = await cassia_exceptions_repository.get_active_mantenimientos_by_hostids_and_dates(hostids_list_str, exception.init_date, exception.end_date, db)
    if not existen_mantenimientos.empty:
        hosts_con_mantenimiento_activo_str = ", ".join(
            list(map(str, existen_mantenimientos['hostid'])))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Los siguientes hosts ya se encuentran en un mantenimiento entre las fechas seleccionadas: {hosts_con_mantenimiento_activo_str}"
        )
    tasks = [save_exception(exception, hostid, current_user_session, db)
             for hostid in hosts_existentes]
    lote_len = 5
    results = []
    for i in range(0, len(tasks), lote_len):
        lote = tasks[i:i + lote_len]
        # Ejecutar las corutinas de forma concurrente
        resultados = await asyncio.gather(*lote)
        for resultado in resultados:
            results.append(resultado)
    print(results)
    if all(result['result'] for result in results):
        return success_response(message="Excepciones creadas correctamente",
                                data=exception,
                                status_code=status.HTTP_201_CREATED)
    errors = [result['hostid'] for result in results if not result['result']]
    errors_str = ", ".join(list(map(str, errors)))
    return success_response(message=f"Se crearon algunas excepciones, las siguientes excepciones no fueron creadas correctamente {errors_str}",
                            data=exception,
                            status_code=status.HTTP_201_CREATED)
    hosts = await cassia_exceptions_repository.get_hosts_by_ids(exception.hostids)

    host = await cassia_exceptions_repository.get_host_by_id(exception.hostids)
    if host.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El host con el id proporcionado no existe"
        )
    """ current_time = get_datetime_now_with_tz() """
    exception_dict = exception.dict()
    exception_dict['session_id'] = current_user_session.session_id.hex
    exception_dict['closed_at'] = None
    exception_dict['created_at'] = exception.created_at
    exception = await cassia_exceptions_repository.create_cassia_exception(exception_dict)
    """ TICKETS_PROGRESS_SOLUTION """
    if exception is not None:
        is_gs_tickets_active = await CassiaConfigRepository.get_config_value_by_name(
            'gs_tickets')
        if is_gs_tickets_active.empty:
            is_gs_tickets_active = 0
        else:
            is_gs_tickets_active = is_gs_tickets_active['value'][0]
        if is_gs_tickets_active:
            active_tickets = await cassia_gs_tickets_repository.get_active_tickets_by_hostid(exception.hostid)
            if not active_tickets.empty:
                ticket_data = {
                    "ticketId": int(active_tickets['ticket_id'][0]),
                    "comment": exception.description,
                    "engineer": current_user_session.mail,
                }
                print(ticket_data)
                created_ticket_comment = await cassia_gs_tickets_repository.create_ticket_comment_avance_solucion(ticket_data)
                if created_ticket_comment is not False:
                    print(created_ticket_comment)
                    save_ticket_data = await cassia_gs_tickets_repository.save_ticket_comment_avance_solucion(ticket_data, created_ticket_comment, current_user_session.mail, active_tickets['cassia_gs_tickets_id'][0])
                    print(save_ticket_data)

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


async def get_exceptions_detail_async():
    exceptions = await cassia_exceptions_repository.get_cassia_exceptions_detail()
    if not exceptions.empty:
        now = datetime.now(pytz.timezone(settings.time_zone))
        exceptions['created_at'] = pd.to_datetime(exceptions['created_at'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
            pytz.timezone(settings.time_zone))
        exceptions['diferencia'] = now - exceptions['created_at']
        exceptions['dias'] = exceptions['diferencia'].dt.days
        exceptions['horas'] = exceptions['diferencia'].dt.components.hours
        exceptions['minutos'] = exceptions['diferencia'].dt.components.minutes
        exceptions = exceptions.drop(columns=['diferencia', ])
        exceptions['active_time'] = exceptions.apply(
            lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
        exceptions = exceptions.drop(
            columns=['dias', 'horas', 'minutos'])
    return success_response(data=exceptions.to_dict(orient='records'))


async def update_exception_async(exception: exception_schema.CassiaExceptions, current_user_session):
    existing_exception = await cassia_exceptions_repository.get_cassia_instance_exception_by_id(exception.exception_id)

    if not existing_exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el registro de Exception"
        )

    current_time = datetime.now(pytz.timezone(settings.time_zone))

    # Ensure session_id is correctly set as a string without hyphens
    existing_exception.session_id = current_user_session
    existing_exception.description = exception.description
    existing_exception.hostid = exception.hostid
    existing_exception.exception_agency_id = exception.exception_agency_id,
    existing_exception.updated_at = current_time
    existing_exception.created_at = exception.created_at

    # Update the exception with new data
    result = await cassia_exceptions_repository.update_cassia_exception(existing_exception)

    return success_response(
        message="Excepción actualizada correctamente",
        data=result
    )


async def delete_exception_async(exception_id: int):
    existing_exception = await cassia_exceptions_repository.get_cassia_instance_exception_by_id(exception_id)
    if not existing_exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el registro de Exception"
        )
    current_time = datetime.now(pytz.timezone(settings.time_zone))
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    result = await cassia_exceptions_repository.delete_cassia_exception(exception_id, formatted_time)
    message = "Exception Eliminada" if result else "Error al eliminar Exception"
    return success_response(message=message)
