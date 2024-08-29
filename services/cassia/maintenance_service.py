import schemas.cassia_maintenance_schema as maintenance_schema
from infraestructure.cassia import cassia_exceptions_repository
from infraestructure.cassia import CassiaMaintenanceRepository
from fastapi.exceptions import HTTPException
from fastapi import status
from utils.traits import success_response, get_datetime_now_with_tz, get_datetime_now_str_with_tz
from datetime import datetime


async def create_maintenance_async_new(maintenance: maintenance_schema.CassiaMaintenanceNew, current_user_session):
    maintenance_dict = maintenance.dict()
    print(maintenance_dict)
    maintenance_host_ids = maintenance_dict['hostid']
    maintenance_date_start = maintenance_dict['date_start']
    maintenance_date_end = maintenance_dict['date_end']
    hostids = ",".join([str(hostid) for hostid in maintenance_host_ids])
    print(hostids)
    exist_maintenance = await CassiaMaintenanceRepository.exist_maintenance_new(
        hostids, maintenance_date_start,
        maintenance_date_end)
    if not exist_maintenance.empty:
        ids = ", ".join([str(hostid)
                        for hostid in exist_maintenance['hostid'].to_list()])
        message = f"Existe un mantenimiento programado en el mismo rango de fechas para los siguientes dispositivos: {ids}"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=message)

    
    if not exist_maintenance:
        host = await cassia_exceptions_repository.get_host_by_id(maintenance.hostid)
        if host.empty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"El host con el id proporcionado no existe"
            )

        current_time = get_datetime_now_with_tz()
        validate_dates_async(maintenance.date_start,
                             maintenance.date_end, current_time)

        maintenance_dict['session_id'] = current_user_session
        maintenance_dict['created_at'] = current_time.strftime(
            "%Y-%m-%d %H:%M:%S")

        maintenance = await CassiaMaintenanceRepository.create_cassia_maintenance(maintenance_dict)
        # exception = await cassia_exceptions_repository.create_cassia_exception(exception_dict)

        return success_response(message="Mantenimiento creado correctamente",
                                data=maintenance,
                                status_code=status.HTTP_201_CREATED)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Existe un mantenimiento programado en el mismo rango de fechas con el mismo dispositivo"
        )


async def create_maintenance_async(maintenance: maintenance_schema.CassiaMaintenance, current_user_session):
    maintenance_dict = maintenance.dict()
    maintenance_host_id = maintenance_dict['hostid']
    maintenance_date_start = maintenance_dict['date_start']
    maintenance_date_end = maintenance_dict['date_end']

    exist_maintenance = await CassiaMaintenanceRepository.exist_maintenance(
        maintenance_host_id, maintenance_date_start,
        maintenance_date_end)
    if not exist_maintenance:
        host = await cassia_exceptions_repository.get_host_by_id(maintenance.hostid)
        if host.empty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"El host con el id proporcionado no existe"
            )

        current_time = get_datetime_now_with_tz()
        validate_dates_async(maintenance.date_start,
                             maintenance.date_end, current_time)

        maintenance_dict['session_id'] = current_user_session
        maintenance_dict['created_at'] = current_time.strftime(
            "%Y-%m-%d %H:%M:%S")

        maintenance = await CassiaMaintenanceRepository.create_cassia_maintenance(maintenance_dict)
        # exception = await cassia_exceptions_repository.create_cassia_exception(exception_dict)

        return success_response(message="Mantenimiento creado correctamente",
                                data=maintenance,
                                status_code=status.HTTP_201_CREATED)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Existe un mantenimiento programado en el mismo rango de fechas con el mismo dispositivo"
        )


async def delete_maintenance_async(maintenance_id: int, current_user_session):
    maintenance = await CassiaMaintenanceRepository.get_maintenance_by_id(maintenance_id)
    if not maintenance:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el registro de Mantenimiento"
        )
    current_time = get_datetime_now_str_with_tz()
    result = await CassiaMaintenanceRepository.delete_maintenance(maintenance_id, current_time, current_user_session)
    message = "Mantenimiento Eliminado" if result else "Error al eliminar Mantenimiento"
    return success_response(message=message)


async def get_maintenances():
    maintenances = await CassiaMaintenanceRepository.get_maintenances()
    return success_response(data=maintenances.to_dict(orient='records'))


async def update_maintenance_async(maintenance: maintenance_schema.CassiaMaintenance, current_user_session):
    maintenance_dict = maintenance.dict()
    maintenance_host_id = maintenance_dict['hostid']
    maintenance_date_start = maintenance_dict['date_start']
    maintenance_date_end = maintenance_dict['date_end']

    exist_maintenance = await CassiaMaintenanceRepository.exist_maintenance(
        maintenance_host_id, maintenance_date_start,
        maintenance_date_end)

    if not exist_maintenance:

        existing_maintenance = await CassiaMaintenanceRepository.get_maintenance_by_id(maintenance.maintenance_id)

        if not existing_maintenance:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No existe el registro de Mantenimiento"
            )

        current_time = get_datetime_now_with_tz()

        validate_dates_async(maintenance.date_start,
                             maintenance.date_end, current_time)

        # Ensure session_id is correctly set as a string without hyphens
        existing_maintenance.session_id = current_user_session
        existing_maintenance.description = maintenance.description
        existing_maintenance.hostid = maintenance.hostid
        existing_maintenance.updated_at = current_time.strftime(
            "%Y-%m-%d %H:%M:%S")
        existing_maintenance.date_start = maintenance.date_start
        existing_maintenance.date_end = maintenance.date_end

        # Update the exception with new data
        result = await CassiaMaintenanceRepository.update_cassia_maintenance(existing_maintenance)

        return success_response(
            message="Mantenimiento actualizado correctamente",
            data=result
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Existe un mantenimiento programado en el mismo rango de fechas con el mismo dispositivo"
        )


def validate_dates_async(maintenance_date_start: datetime, maintenance_date_end: datetime, current_time: datetime):
    if (maintenance_date_start < current_time) or (maintenance_date_end < current_time):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"La fecha de inicio o fin es menor a la fecha actual"
        )
    if maintenance_date_end < maintenance_date_start:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"La fecha de fin es menor a la fecha final"
        )
