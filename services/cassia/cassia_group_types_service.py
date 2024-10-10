from fastapi import status, HTTPException
from infraestructure.cassia import cassia_group_types_repository
from utils.traits import success_response
from schemas import cassia_technologies_schema
from infraestructure.database import DB


async def get_group_types(db: DB):
    group_types = await cassia_group_types_repository.get_cassia_group_types(db)
    return success_response(data=group_types.to_dict(orient="records"))
