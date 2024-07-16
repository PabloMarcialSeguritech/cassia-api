from fastapi import status, HTTPException
from infraestructure.cassia import cassia_events_config_repository
from utils.traits import success_response
from schemas import cassia_technologies_schema


async def get_events_config():
    techs = await cassia_events_config_repository.get_events_config()
    return success_response(data=techs.to_dict(orient="records"))
