from fastapi import status, HTTPException
from infraestructure.cassia import cassia_services_tech_repository
from infraestructure.cassia import cassia_criticalities_repository
from infraestructure.cassia import cassia_tech_devices_repository
from utils.traits import success_response
from schemas import cassia_service_tech_schema


async def get_services():
    services = await cassia_services_tech_repository.get_services()
    if not services.empty:
        services['sla'] = 100
        services['formatted_sla'] = "100.00%"
        for ind in services.index:
            service_sla = await cassia_tech_devices_repository.get_sla_by_service(services['cassia_tech_service_id'][ind])
            services['sla'][ind] = service_sla
            services['formatted_sla'][ind] = f"{service_sla}%" if service_sla != "NA" else "NA"
    return success_response(data=services.to_dict(orient="records"))


async def get_service(cassia_tech_service_id):
    service = await cassia_services_tech_repository.get_service_tech_by_id(cassia_tech_service_id)
    if service.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Servicio no encontrado")
    service['sla'] = 100
    service['formatted_sla'] = "100.00%"
    return success_response(data=service.to_dict(orient="records")[0])


async def create_service(service_data: cassia_service_tech_schema.CassiaTechServiceSchema):
    if service_data.cassia_criticality_id is not None and service_data.cassia_criticality_id != 0:
        get_criticality = await cassia_criticalities_repository.get_criticality_by_id(
            service_data.cassia_criticality_id)
        if get_criticality.empty:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Criticidad no encontrada")
    else:
        service_data.cassia_criticality_id = None
    created_service = await cassia_services_tech_repository.create_service(
        service_data)
    return success_response(message="Registro creado correctamente")


async def update_service(cassia_tech_service_id, service_data: cassia_service_tech_schema.CassiaTechServiceSchema):
    cassia_service = await cassia_services_tech_repository.get_service_tech_by_id(
        cassia_tech_service_id)
    if cassia_service.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Servicio no encontrado")
    if service_data.cassia_criticality_id is not None and service_data.cassia_criticality_id != 0:
        get_criticality = await cassia_criticalities_repository.get_criticality_by_id(
            service_data.cassia_criticality_id)
        if get_criticality.empty:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="Criticidad no encontrada")
    else:
        service_data.cassia_criticality_id = None
    update_service = await cassia_services_tech_repository.update_service(
        cassia_tech_service_id, service_data)

    return success_response(message="Registro creado correctamente")


async def delete_service(cassia_tech_service_id):
    service_exists = await cassia_services_tech_repository.get_service_tech_by_id(cassia_tech_service_id)
    if service_exists.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Servicio no encontrado")
    deleted_service = await cassia_services_tech_repository.delete_service_by_id(cassia_tech_service_id)
    return success_response(message="Registro eliminado correctamente")
