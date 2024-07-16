from fastapi import status, HTTPException
from infraestructure.cassia import cassia_services_tech_repository
from infraestructure.cassia import cassia_criticalities_repository
from infraestructure.cassia import cassia_techs_repository
from infraestructure.cassia import cassia_tech_devices_repository
from utils.traits import success_response
from schemas import cassia_service_tech_schema
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


async def get_tech_by_id(tech_id):
    tech = await cassia_techs_repository.get_tech_by_id(tech_id)
    if tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    return success_response(data=tech.to_dict(orient="records")[0])


async def create_tech(tech_data: cassia_techs_schema.CassiaTechSchema):
    service_exist = await cassia_services_tech_repository.get_service_tech_by_id(tech_data.service_id)
    if service_exist.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Servicio no encontrado")
    if tech_data.cassia_criticality_id is not None:
        get_criticality = await cassia_criticalities_repository.get_criticality_by_id(
            tech_data.cassia_criticality_id)
        if get_criticality.empty:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Criticidad no encontrada")
    created_tech = await cassia_techs_repository.create_tech(
        tech_data)
    return success_response(message="Registro creado correctamente")


async def update_tech(cassia_tech_id, tech_data: cassia_techs_schema.CassiaTechSchema):
    cassia_tech = await cassia_techs_repository.get_tech_by_id(
        cassia_tech_id)
    if cassia_tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    service_exist = await cassia_services_tech_repository.get_service_tech_by_id(tech_data.service_id)
    if service_exist.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Servicio no encontrado")
    if tech_data.cassia_criticality_id is not None:
        get_criticality = await cassia_criticalities_repository.get_criticality_by_id(
            tech_data.cassia_criticality_id)
        if get_criticality.empty:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Criticidad no encontrada")
    update_tech = await cassia_techs_repository.update_tech(
        cassia_tech_id, tech_data)
    return success_response(message="Registro actualizado correctamente")


async def delete_tech(cassia_tech_id):
    cassia_tech = await cassia_techs_repository.get_tech_by_id(
        cassia_tech_id)
    if cassia_tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    deleted_tech = await cassia_techs_repository.delete_tech_by_id(cassia_tech_id)
    return success_response(message="Registro eliminado correctamente")
