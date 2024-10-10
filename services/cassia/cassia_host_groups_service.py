from fastapi import status, HTTPException
from infraestructure.cassia import cassia_host_groups_repository
from utils.traits import success_response
from schemas import cassia_technologies_schema
from infraestructure.database import DB


async def get_host_groups(db: DB):
    host_groups = await cassia_host_groups_repository.get_cassia_host_groups(db)
    return success_response(data=host_groups.to_dict(orient="records"))
