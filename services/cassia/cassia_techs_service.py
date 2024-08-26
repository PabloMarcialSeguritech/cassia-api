from fastapi import status, HTTPException
from infraestructure.cassia import cassia_services_tech_repository
from infraestructure.cassia import cassia_criticalities_repository
from infraestructure.cassia import cassia_techs_repository
from infraestructure.cassia import cassia_tech_devices_repository
from utils.traits import success_response
from schemas import cassia_service_tech_schema
from schemas import cassia_techs_schema


async def get_techs_by_service(service_id):
    service_exist = await cassia_services_tech_repository.get_service_tech_by_id(service_id)
    if service_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Servicio no encontrado")
    response = dict()
    if not service_exist.empty:
        response['sla_service'] = 100
        response['sla_service_formatted'] = "100.00%"
        service_sla = await cassia_tech_devices_repository.get_sla_by_service(service_id)
        response['sla_service'] = service_sla
        response['sla_service_formatted'] = f"{service_sla}%" if service_sla != "NA" else "NA"

    techs = await cassia_techs_repository.get_techs_by_service_id(service_id)
    if not techs.empty:
        techs['sla'] = 100
        techs['formatted_sla'] = "100.00%"
        for ind in techs.index:
            tech_sla = await cassia_tech_devices_repository.get_sla_by_tech(techs['cassia_tech_id'][ind], techs['sla_hours'][ind])
            techs['sla'][ind] = tech_sla
            techs['formatted_sla'][ind] = f"{tech_sla}%" if tech_sla != "NA" else "NA"
    response['techs'] = techs.to_dict(orient="records")
    return success_response(data=response)


async def get_tech_by_id(tech_id):
    tech = await cassia_techs_repository.get_tech_by_id(tech_id)
    if tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    return success_response(data=tech.to_dict(orient="records")[0])


async def create_tech(tech_data: cassia_techs_schema.CassiaTechSchema):
    service_exist = await cassia_services_tech_repository.get_service_tech_by_id(tech_data.service_id)
    if service_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Servicio no encontrado")
    if tech_data.cassia_criticality_id is not None and tech_data.cassia_criticality_id != 0:
        get_criticality = await cassia_criticalities_repository.get_criticality_by_id(
            tech_data.cassia_criticality_id)
        if get_criticality.empty:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Criticidad no encontrada")
    else:
        tech_data.cassia_criticality_id = None
    name_exist = await cassia_services_tech_repository.get_service_tech_by_name(tech_data.service_id, tech_data.tech_name)
    if not name_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El nombre de la tecnologia en este servicio ya existe")
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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Servicio no encontrado")
    if tech_data.cassia_criticality_id is not None and tech_data.cassia_criticality_id != 0:
        get_criticality = await cassia_criticalities_repository.get_criticality_by_id(
            tech_data.cassia_criticality_id)
        if get_criticality.empty:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Criticidad no encontrada")
    else:
        tech_data.cassia_criticality_id = None
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
