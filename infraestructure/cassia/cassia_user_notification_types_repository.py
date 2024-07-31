from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_technologies import CassiaTechnologiesModel
from models.cassia_tech_services import CassiaTechServiceModel
from models.cassia_techs import CassiaTechModel
from schemas import cassia_technologies_schema
from schemas import cassia_service_tech_schema
from schemas import cassia_techs_schema
import pandas as pd
import numpy as np
import utils.traits as traits
from datetime import datetime


async def get_techs() -> pd.DataFrame:
    db_model = DB()
    try:
        get_tech_names_with_service = DBQueries(
        ).query_get_tech_names_with_service
        await db_model.start_connection()
        techs_data = await db_model.run_query(get_tech_names_with_service)
        techs_df = pd.DataFrame(
            techs_data).replace(np.nan, None)
        return techs_df

    except Exception as e:
        print(f"Excepcion en get_techs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_techs: {e}")
    finally:
        await db_model.close_connection()


async def get_notification_types() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_notification_types = DBQueries(
        ).query_get_notification_types
        await db_model.start_connection()
        notification_types_data = await db_model.run_query(query_get_notification_types)
        notification_types_df = pd.DataFrame(
            notification_types_data).replace(np.nan, None)
        return notification_types_df

    except Exception as e:
        print(f"Excepcion en get_notification_types: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_notification_types: {e}")
    finally:
        await db_model.close_connection()


async def get_users_notifications_types() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_user_notification_types = DBQueries(
        ).query_get_user_notification_types
        await db_model.start_connection()
        user_notification_types_data = await db_model.run_query(query_get_user_notification_types)
        user_notification_types_df = pd.DataFrame(
            user_notification_types_data).replace(np.nan, None)
        return user_notification_types_df

    except Exception as e:
        print(f"Excepcion en get_users_notifications_types: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_users_notifications_types: {e}")
    finally:
        await db_model.close_connection()


async def get_users() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_users = DBQueries(
        ).query_get_users
        await db_model.start_connection()
        users_data = await db_model.run_query(query_get_users)
        users_df = pd.DataFrame(
            users_data).replace(np.nan, None)
        return users_df

    except Exception as e:
        print(f"Excepcion en get_users: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_users: {e}")
    finally:
        await db_model.close_connection()


async def get_user(user_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_user = DBQueries(
        ).builder_query_statement_get_user(user_id)
        await db_model.start_connection()
        user_data = await db_model.run_query(query_statement_get_user)
        user_df = pd.DataFrame(
            user_data).replace(np.nan, None)
        return user_df

    except Exception as e:
        print(f"Excepcion en get_user: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_user: {e}")
    finally:
        await db_model.close_connection()


async def get_user_notifications_techs(user_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_user_notifications_techs = DBQueries(
        ).builder_query_statement_get_user_notifications_techs(user_id)
        await db_model.start_connection()
        user_notification_techs_data = await db_model.run_query(query_statement_get_user_notifications_techs)
        user_notification_techs_df = pd.DataFrame(
            user_notification_techs_data).replace(np.nan, None)
        return user_notification_techs_df

    except Exception as e:
        print(f"Excepcion en get_user_notifications_techs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_user_notifications_techs: {e}")
    finally:
        await db_model.close_connection()


async def create_user_notification_type(user_id, cassia_notification_type_id, techs_list) -> pd.DataFrame:
    db_model = DB()
    try:
        query_insert_user_notification_types = DBQueries(
        ).builder_query_insert_user_notification_types(user_id, cassia_notification_type_id, techs_list)
        await db_model.start_connection()
        user_notification_techs_inserted = await db_model.run_query(query_insert_user_notification_types)
        return True

    except Exception as e:
        print(f"Excepcion en create_user_notification_type: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en create_user_notification_type: {e}")
    finally:
        await db_model.close_connection()


async def delete_info(user_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_delete_user_notification_types_by_user_id = DBQueries(
        ).builder_query_statement_delete_user_notification_types_by_user_id(user_id)
        await db_model.start_connection()
        deleted_info_data = await db_model.run_query(query_statement_delete_user_notification_types_by_user_id)
        return True

    except Exception as e:
        print(f"Excepcion en delete_info: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_info: {e}")
    finally:
        await db_model.close_connection()
