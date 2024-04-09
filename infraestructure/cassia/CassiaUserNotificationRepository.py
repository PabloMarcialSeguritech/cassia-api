from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
from models.cassia_slack_user_notification_ import CassiaSlackUserNotification
import pandas as pd
import numpy as np
from datetime import datetime
import pytz


async def get_user_slack_notification(user_id) -> pd.DataFrame:
    db_model = DB()
    try:
        get_user_notification = DBQueries(
        ).builder_query_statement_get_user_slack_notification(user_id)
        await db_model.start_connection()
        user_notification_data = await db_model.run_query(get_user_notification)
        user_notification_df = pd.DataFrame(
            user_notification_data).replace(np.nan, None)
        return user_notification_df
    except Exception as e:
        print(f"Excepcion en get_cassia_event: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_cassia_event: {e}")
    finally:
        await db_model.close_connection()


async def get_user_slack_notifications_count(last_date) -> int:
    db_model = DB()
    try:
        get_user_notification = DBQueries(
        ).builder_query_statement_get_user_slack_notification_count(last_date)
        await db_model.start_connection()
        user_notification_count = await db_model.run_query(get_user_notification)
        user_notification_count_df = pd.DataFrame(
            user_notification_count).replace(np.nan, None)
        if not user_notification_count_df.empty:
            return int(user_notification_count_df['notificaciones'][0])
        else:
            return 0
    except Exception as e:
        print(f"Excepcion en get_cassia_event: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_user_slack_notifications_count: {e}")
    finally:
        await db_model.close_connection()


async def get_total_slack_notifications_count() -> int:
    db_model = DB()
    try:
        query_get_total_notification = DBQueries(
        ).query_get_total_slack_notifications_count
        await db_model.start_connection()
        total_notification_count = await db_model.run_query(query_get_total_notification)
        total_notification_count_df = pd.DataFrame(
            total_notification_count).replace(np.nan, None)
        if not total_notification_count_df.empty:
            return int(total_notification_count_df['notificaciones'][0])
        else:
            return 0
    except Exception as e:
        print(f"Excepcion en get_cassia_event: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_user_slack_notifications_count: {e}")
    finally:
        await db_model.close_connection()


async def get_notifications(skip, limit) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_slack_notifications = DBQueries(
        ).builder_query_statement_get_slack_notifications(skip, limit)
        await db_model.start_connection()
        notifications_data = await db_model.run_query(query_get_slack_notifications)
        notifications_df = pd.DataFrame(
            notifications_data).replace(np.nan, None)
        return notifications_df
    except Exception as e:
        print(f"Excepcion en get_cassia_event: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_notifications: {e}")
    finally:
        await db_model.close_connection()


async def update_user_notification_register(current_session) -> pd.DataFrame:
    db_model = DB()
    try:
        last_date = await get_user_slack_notification(current_session.user_id)
        session = await db_model.get_session()
        now = datetime.now(pytz.timezone(
            'America/Mexico_City'))
        if last_date.empty:
            print("ENTRO EN 1")
            create_register = CassiaSlackUserNotification(
                user_id=current_session.user_id,
                last_date=now
            )
            session.add(create_register)
            await session.commit()
            return True
        else:
            update_user_slack_notification = DBQueries(
            ).builder_query_statement_update_user_slack_notification(current_session.user_id, now)
            print(update_user_slack_notification)
            try:
                await db_model.start_connection()
                print("a")
                result = await db_model.run_query(update_user_slack_notification)
                print("b")
                print("ENTRO EN 2")
                print(result)
                return True
            except Exception as e:
                print(f"Excepcion en update_user_notification_register_2: {e}")
                raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail=f"Excepcion en update_user_notification_register_2: {e}")
            finally:
                await db_model.close_connection()

    except Exception as e:
        print(f"Excepcion en update_user_notification_register: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en update_user_notification_register: {e}")
    finally:
        await session.close()
