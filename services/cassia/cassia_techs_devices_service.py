from fastapi import status, HTTPException
from infraestructure.cassia import cassia_services_tech_repository
from infraestructure.cassia import cassia_criticalities_repository
from infraestructure.cassia import cassia_techs_repository
from infraestructure.cassia import cassia_tech_devices_repository
from utils.traits import success_response
from schemas import cassia_service_tech_schema
from schemas import cassia_tech_device_schema
from schemas import cassia_techs_schema


async def get_devices_by_tech(tech_id):
    tech_exist = await cassia_techs_repository.get_tech_by_id(tech_id)
    if tech_exist.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    devices = await cassia_tech_devices_repository.get_devices_by_tech_id(tech_id)
    if not devices.empty:
        devices['sla'] = 100
        devices['formatted_sla'] = "100.00%"
        devices['status'] = 1
        downs = await cassia_tech_devices_repository.get_downs()
        if not downs.empty:
            devices.loc[devices['hostid'].isin(
                downs['hostid'].to_list()), 'status'] = 0

    return success_response(data=devices.to_dict(orient="records"))


async def get_tech_device_by_id(device_id):
    device = await cassia_tech_devices_repository.get_tech_device_by_id(device_id)
    if device.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Dispositivo no encontrado")
    return success_response(data=device.to_dict(orient="records")[0])


async def create_tech_device(device_data: cassia_tech_device_schema.CassiaTechDeviceSchema):
    tech = await cassia_techs_repository.get_tech_by_id(device_data.cassia_tech_id)
    if tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    print(device_data.criticality_id)
    if device_data.criticality_id is not None and device_data.criticality_id != 0:
        criticality = await cassia_criticalities_repository.get_criticality_by_id(device_data.criticality_id)
        if criticality.empty:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Criticidad no encontrada")
    else:
        device_data.criticality_id = None
    hostids = ", ".join([str(hostid) for hostid in device_data.hostid])
    exists_devices = await cassia_tech_devices_repository.exist_devices(hostids)
    if not exists_devices.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Algunos dispositivos ya han sido creados")
    for hostid in device_data.hostid:
        created_device = await cassia_tech_devices_repository.create_device(hostid, device_data)

    return success_response(message="Información almacenada correctamente")


async def update_device(cassia_tech_device_id, device_data: cassia_tech_device_schema.UpdateCassiaTechDeviceSchema):
    tech = await cassia_techs_repository.get_tech_by_id(device_data.cassia_tech_id)
    if tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    if device_data.criticality_id is not None and device_data.criticality_id != 0:
        criticality = await cassia_criticalities_repository.get_criticality_by_id(device_data.criticality_id)
        if criticality.empty:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Criticidad no encontrada")
    else:
        device_data.criticality_id = None
    exist_device = await cassia_tech_devices_repository.exist_devices(device_data.hostid)
    if not exist_device.empty:

        if str(exist_device['cassia_tech_device_id'][0]) != str(cassia_tech_device_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="El hostid no puede ser asignado porque ya existe.")
    update_device = await cassia_tech_devices_repository.update_device(cassia_tech_device_id, device_data)
    return success_response(message="Registro actualizado correctamente")


async def delete_device(cassia_tech_device_id):
    cassia_tech_device = await cassia_tech_devices_repository.get_tech_device_by_id(cassia_tech_device_id)
    if cassia_tech_device.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Dispositivo no encontrado")
    deleted_device = await cassia_tech_devices_repository.delete_tech_device_by_id(cassia_tech_device_id)
    return success_response(message="Registro eliminado correctamente")
