from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
from utils.traits import success_response
from fastapi import HTTPException, status
from infraestructure.cassia import cassia_automation_actions_front_repository
from schemas import cassia_auto_action_condition_schema
from schemas import cassia_auto_action_schema


async def get_conditions():
    conditions = await cassia_automation_actions_front_repository.get_conditions()
    return success_response(data=conditions.to_dict(orient="records"))


async def get_condition_detail(condition_id):
    condition = await cassia_automation_actions_front_repository.get_condition_by_id(condition_id)
    if condition.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Condicion no encontrada")
    condition_detail = await cassia_automation_actions_front_repository.get_condition_detail_by_id(condition_id)
    reponse = condition.iloc[0].to_dict()
    reponse['detail'] = condition_detail.to_dict(orient='records')
    return success_response(data=reponse)


async def create_auto_action_condition(condition_data: cassia_auto_action_condition_schema.AutoActionConditionSchema):
    condition_created = await cassia_automation_actions_front_repository.create_auto_action_condition(condition_data.name)
    for detail in condition_data.detail:
        await cassia_automation_actions_front_repository.create_auto_action_condition_detail(condition_created.condition_id, detail)
    return success_response(message="Registros guardados correctamente")


async def update_auto_action_condition(condition_data: cassia_auto_action_condition_schema.AutoActionConditionUpdateSchema):
    condition = await cassia_automation_actions_front_repository.get_condition_by_id(condition_data.condition_id)
    if condition.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Condicion no encontrada")
    update_condition = await cassia_automation_actions_front_repository.update_auto_action_condition_by_id(condition_data)
    for detail in condition_data.detail:
        if detail.cond_detail_id is None or detail.cond_detail_id == 0:
            await cassia_automation_actions_front_repository.create_auto_action_condition_detail(condition_data.condition_id, detail)
        else:
            await cassia_automation_actions_front_repository.update_auto_action_condition_detail_by_id(detail)
    return success_response(message="Registros actualizados correctamente")


async def delete_auto_action_condition(condition_id):
    condition = await cassia_automation_actions_front_repository.get_condition_by_id(condition_id)
    if condition.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Condicion no encontrada")
    delete_conditions = await cassia_automation_actions_front_repository.delete_conditions(condition_id)
    return success_response(message="Registro eliminado correctamente")


async def get_actions_auto():
    auto_actions = await cassia_automation_actions_front_repository.get_auto_actions()
    return success_response(data=auto_actions.to_dict(orient='records'))


async def get_action_auto(action_auto_id):
    auto_action = await cassia_automation_actions_front_repository.get_auto_action_by_id(action_auto_id)
    if auto_action.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Accion no encontrada")
    response_data = auto_action.iloc[0].to_dict()
    return success_response(data=response_data)


async def create_auto_action(action_data: cassia_auto_action_schema.AutoActionSchema):
    condition = await cassia_automation_actions_front_repository.get_condition_by_id(action_data.condition_id)
    if condition.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Condicion no encontrada")
    action = await cassia_automation_actions_front_repository.get_action_by_id(action_data.action_id)
    if action.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Accion no encontrada")
    created_auto_action = await cassia_automation_actions_front_repository.create_auto_action(action_data)
    return success_response(message="Accion creada correctamente")


async def update_auto_action(action_data: cassia_auto_action_schema.AutoActionUpdateSchema):
    condition = await cassia_automation_actions_front_repository.get_condition_by_id(action_data.condition_id)
    if condition.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Condicion no encontrada")
    action = await cassia_automation_actions_front_repository.get_action_by_id(action_data.action_id)
    if action.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Accion no encontrada")
    update = await cassia_automation_actions_front_repository.update_auto_action_by_id(action_data)
    return success_response(message="Accion actualizada correctamente")


async def delete_auto_action(action_auto_id):
    auto_action = await cassia_automation_actions_front_repository.get_auto_action_by_id(action_auto_id)
    if auto_action.empty:
        raise HTTPException(status.HTTP_404_NOT_FOUND,
                            detail=f"Accion no encontrada")
    delete_action = await cassia_automation_actions_front_repository.delete_action_auto_by_id(action_auto_id)
    return success_response(message="Registro eliminado correctamente")
