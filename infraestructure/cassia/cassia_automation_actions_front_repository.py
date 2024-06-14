from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from fastapi.exceptions import HTTPException
from fastapi import status
from infraestructure.db_queries_model import DBQueries
from utils.settings import Settings
import pandas as pd
from datetime import datetime
import pytz
import numpy as np
from models.cassia_auto_action_condition import CassiaAutoActionConditionModel
from models.cassia_auto_action_condition_detail import CassiaAutoActionConditionDetailModel
from models.cassia_action_auto import CassiaActionAutoModel
from schemas.cassia_auto_action_condition_schema import AutoActionConditionDetailSchema
from schemas import cassia_auto_action_condition_schema
from schemas import cassia_auto_action_schema
settings = Settings()


""" async def cancel_action_max_retry_times(ids, now_str) -> pd.DataFrame:
    db_model = DB()
    try:
        query_cancel_auto_action_max_retry_times = DBQueries(
        ).builder_query_cancel_auto_action_operation_values_max_retry_times(ids, now_str)
        await db_model.start_connection()
        cancel_auto_action_max_retry_times = await db_model.run_query(query_cancel_auto_action_max_retry_times)
        return True

    except Exception as e:
        print(f"Excepcion en cancel_action_max_retry_times: {e}")
        return False
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en cancel_action_max_retry_times: {e}")
    finally:
        await db_model.close_connection() """


async def delete_action_auto_by_id(action_auto_id) -> bool:
    db_model = DB()
    try:
        query_delete_action_auto_by_id = DBQueries(
        ).builder_query_delete_action_auto_by_id(action_auto_id)
        await db_model.start_connection()
        await db_model.run_query(query_delete_action_auto_by_id)
        return True
    except Exception as e:
        print(f"Excepcion en delete_action_auto_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_action_auto_by_id: {e}")
    finally:
        await db_model.close_connection()


async def delete_conditions(condition_id) -> bool:
    db_model = DB()
    try:
        query_delete_auto_condition_detail_by_id = DBQueries(
        ).builder_delete_auto_condition_detail_by_id(condition_id)
        query_delete_auto_condition_by_id = DBQueries(
        ).builder_delete_auto_condition_by_id(condition_id)
        await db_model.start_connection()
        await db_model.run_query(query_delete_auto_condition_detail_by_id)
        await db_model.run_query(query_delete_auto_condition_by_id)
        return True
    except Exception as e:
        print(f"Excepcion en delete_conditions: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_conditions: {e}")
    finally:
        await db_model.close_connection()


async def get_action_by_id(action_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_action_by_id = DBQueries(
        ).builder_query_get_action_by_id(action_id)
        await db_model.start_connection()
        action_data = await db_model.run_query(query_get_action_by_id)
        action_df = pd.DataFrame(
            action_data).replace(np.nan, None)
        return action_df

    except Exception as e:
        print(f"Excepcion en get_action_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_action_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_auto_action_by_id(action_auto_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_auto_actions = DBQueries(
        ).builder_query_get_auto_action_by_id(action_auto_id)
        await db_model.start_connection()
        auto_action_data = await db_model.run_query(query_get_auto_actions)
        auto_action_df = pd.DataFrame(
            auto_action_data).replace(np.nan, None)
        return auto_action_df

    except Exception as e:
        print(f"Excepcion en get_auto_action_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_auto_action_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_auto_actions() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_auto_actions = DBQueries(
        ).query_get_auto_actions
        await db_model.start_connection()
        auto_actions_data = await db_model.run_query(query_get_auto_actions)
        auto_actions_df = pd.DataFrame(
            auto_actions_data).replace(np.nan, None)
        return auto_actions_df

    except Exception as e:
        print(f"Excepcion en get_auto_actions: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_auto_actions: {e}")
    finally:
        await db_model.close_connection()


async def get_conditions() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_auto_actions_conditions = DBQueries(
        ).query_get_auto_actions_conditions
        await db_model.start_connection()
        auto_conditions_data = await db_model.run_query(query_get_auto_actions_conditions)
        auto_conditions_df = pd.DataFrame(
            auto_conditions_data).replace(np.nan, None)
        return auto_conditions_df

    except Exception as e:
        print(f"Excepcion en get_conditions: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_conditions: {e}")
    finally:
        await db_model.close_connection()


async def get_condition_by_id(condition_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_auto_action_condition_by_id = DBQueries(
        ).builder_query_get_auto_condition_by_id(condition_id)
        await db_model.start_connection()
        auto_condition_data = await db_model.run_query(query_get_auto_action_condition_by_id)
        auto_condition_df = pd.DataFrame(
            auto_condition_data).replace(np.nan, None)
        return auto_condition_df

    except Exception as e:
        print(f"Excepcion en get_condition_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_condition_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_condition_detail_by_id(condition_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_auto_condition_detail_by_id = DBQueries(
        ).builder_query_get_auto_condition_detail_by_id(condition_id)
        await db_model.start_connection()
        auto_condition_detail_data = await db_model.run_query(query_get_auto_condition_detail_by_id)
        auto_condition_detail_df = pd.DataFrame(
            auto_condition_detail_data).replace(np.nan, None)
        return auto_condition_detail_df

    except Exception as e:
        print(f"Excepcion en get_condition_detail_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_condition_detail_by_id: {e}")
    finally:
        await db_model.close_connection()


async def create_auto_action(action_data):
    db_model = DB()
    try:
        session = await db_model.get_session()
        auto_action = CassiaActionAutoModel(
            name=action_data.name,
            description=action_data.description,
            action_id=action_data.action_id,
            type_trigger=action_data.type_trigger,
            condition_id=action_data.condition_id,
        )
        session.add(auto_action)
        await session.commit()
        await session.refresh(auto_action)
        return auto_action

    except Exception as e:
        print(f"Excepcion en create_auto_action: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_auto_action: {e}")
    finally:
        await session.close()


async def create_auto_action_condition(name):
    db_model = DB()
    try:
        session = await db_model.get_session()
        auto_action_condition = CassiaAutoActionConditionModel(
            name=name
        )
        session.add(auto_action_condition)
        await session.commit()
        await session.refresh(auto_action_condition)
        return auto_action_condition

    except Exception as e:
        print(f"Excepcion en create_auto_action_condition: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_auto_action_condition: {e}")
    finally:
        await session.close()


async def create_auto_action_condition_detail(condition_id, detail: AutoActionConditionDetailSchema):
    db_model = DB()
    try:
        session = await db_model.get_session()
        auto_action_condition_detail = CassiaAutoActionConditionDetailModel(
            condition_id=condition_id,
            delay=detail.delay,
            template_name=detail.template_name,
            template_id=detail.template_id,
            range_min=detail.range_min,
            range_max=detail.range_max,
            units=detail.units)

        session.add(auto_action_condition_detail)
        await session.commit()
        await session.refresh(auto_action_condition_detail)
        return auto_action_condition_detail

    except Exception as e:
        print(f"Excepcion en create_auto_action_condition_detail: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_auto_action_condition_detail: {e}")
    finally:
        await session.close()


async def update_auto_action_condition_by_id(condition_data: cassia_auto_action_condition_schema.AutoActionConditionUpdateSchema) -> pd.DataFrame:
    db_model = DB()
    try:
        query_update_action_condition_by_id = DBQueries(
        ).builder_query_update_action_condition_by_id(condition_data.condition_id, condition_data.name)
        await db_model.start_connection()
        await db_model.run_query(query_update_action_condition_by_id)
        return True

    except Exception as e:
        print(f"Excepcion en update_auto_action_condition_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_auto_action_condition_by_id: {e}")
    finally:
        await db_model.close_connection()


async def update_auto_action_condition_detail_by_id(condition_data: cassia_auto_action_condition_schema.AutoActionConditionDetailUpdateSchema) -> pd.DataFrame:
    db_model = DB()
    try:
        query_update_action_condition_detail_by_id = DBQueries(
        ).builder_query_update_action_condition_detail_by_id(condition_data)
        await db_model.start_connection()
        await db_model.run_query(query_update_action_condition_detail_by_id)
        return True
    except Exception as e:
        print(f"Excepcion en update_auto_action_condition_detail_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_auto_action_condition_detail_by_id: {e}")
    finally:
        await db_model.close_connection()


async def update_auto_action_by_id(action_data: cassia_auto_action_schema.AutoActionUpdateSchema) -> pd.DataFrame:
    db_model = DB()
    try:
        query_update_action_by_id = DBQueries(
        ).builder_query_update_action_by_id(action_data)
        await db_model.start_connection()
        await db_model.run_query(query_update_action_by_id)
        return True
    except Exception as e:
        print(f"Excepcion en update_auto_action_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_auto_action_by_id: {e}")
    finally:
        await db_model.close_connection()


async def get_auto_action_condition_detail_by_id(condition_detail_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_auto_condition_detail_by_detail_id = DBQueries(
        ).builder_query_get_auto_condition_detail_by_detail_id(condition_detail_id)
        await db_model.start_connection()
        condition_detail_data = await db_model.run_query(query_get_auto_condition_detail_by_detail_id)
        condition_detail_df = pd.DataFrame(
            condition_detail_data).replace(np.nan, None)
        return condition_detail_df
    except Exception as e:
        print(f"Excepcion en get_auto_action_condition_detail_by_id: {e}")

        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_auto_action_condition_detail_by_id: {e}")
    finally:
        await db_model.close_connection()
