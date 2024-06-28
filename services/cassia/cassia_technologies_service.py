from fastapi import status, HTTPException
from infraestructure.cassia import cassia_technologies_repository
from utils.traits import success_response
from schemas import cassia_technologies_schema


""" async def get_technologies():
    techs = await cassia_technologies_repository.get_technologies()
    return success_response(data=techs.to_dict(orient="records"))
 """


async def get_technologies():
    techs = await cassia_technologies_repository.get_technologies()
    return success_response(data=techs.to_dict(orient="records"))


async def get_technology(cassia_technology_id):
    tech = await cassia_technologies_repository.get_technology_by_id(cassia_technology_id)
    if tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    return success_response(data=tech.to_dict(orient="records")[0])


async def create_technology(tech_data: cassia_technologies_schema):
    tech_created = await cassia_technologies_repository.create_technology(tech_data)
    return success_response(message="Registro creado correctamente")


async def update_technology(cassia_technology_id, tech_data: cassia_technologies_schema):
    tech_exist = await cassia_technologies_repository.get_technology_by_id(cassia_technology_id)
    if tech_exist.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    tech_updated = await cassia_technologies_repository.update_technology(cassia_technology_id, tech_data)
    return success_response(message="Registro actualizado correctamente")


async def delete_technology(cassia_technology_id):
    tech_exist = await cassia_technologies_repository.get_technology_by_id(cassia_technology_id)
    if tech_exist.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")
    deleted_tech = await cassia_technologies_repository.delete_technology_by_id(cassia_technology_id)
    return success_response(message="Registro eliminado correctamente")


async def get_technology_devices(tech_id):
    tech = await cassia_technologies_repository.get_technology_by_id(tech_id)
    if tech.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Tecnologia no encontrada")

    devices = await cassia_technologies_repository.get_technology_devices(tech_id)
    return success_response(data=devices.to_dict(orient="records"))
