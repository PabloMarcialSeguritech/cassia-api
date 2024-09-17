from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text, func
import pytz
import numpy as np
from utils.traits import success_response
from models.cassia_slack_user_notification import CassiaSlackUserNotification
from models.cassia_slack_notification import CassiaSlackNotification
from infraestructure.cassia import CassiaUserNotificationRepository
from fastapi import HTTPException, status
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta

settings = Settings()


""" async def get_count(current_session):
    db_zabix = DB_Zabbix()
    with db_zabix.Session() as session:
        last_date = session.query(CassiaSlackUserNotification).filter(
            CassiaSlackUserNotification.user_id == current_session.user_id
        ).first()
        print(current_session.user_id)
        if last_date:
            notifications = session.query(func.count(
                CassiaSlackNotification.notification_id)).filter(
                    CassiaSlackNotification.message_date > last_date.last_date
            ).scalar()
        else:
            notifications = session.query(func.count(
                CassiaSlackNotification.notification_id)).scalar()
    return success_response(data={'notifications_count': notifications})


async def get_items(skip, limit, current_session):
    db_zabix = DB_Zabbix()
    with db_zabix.Session() as session:
        last_date = session.query(CassiaSlackUserNotification).filter(
            CassiaSlackUserNotification.user_id == current_session.user_id
        ).first()
        df_result = pd.read_sql(session.query(
            CassiaSlackNotification).order_by(CassiaSlackNotification.message_date.desc()).offset(
            skip).limit(limit).statement, session.bind)
        if not df_result.empty:
            if last_date:
                df_result['seen'] = df_result['message_date'].apply(
                    lambda date: 1 if date <= last_date.last_date else 0)
            else:
                df_result['seen'] = df_result['message_date'].apply(
                    lambda date: 0)
            df_result.replace(np.nan, "")
            df_result['eventid'] = df_result['eventid'].apply(
                lambda event: str(event))
            now = datetime.now(pytz.timezone(
                'America/Mexico_City'))
            if not last_date:

                create_register = CassiaSlackUserNotification(
                    user_id=current_session.user_id,
                    last_date=now
                )
                session.add(create_register)
            else:
                last_date.last_date = now
            session.commit()

    return success_response(data=df_result.to_dict(orient='records')) """


async def get_count(current_session, db):
    cassia_user_notification = await CassiaUserNotificationRepository.get_user_slack_notification_pool(current_session.user_id, db)
    if not cassia_user_notification.empty:
        last_date = cassia_user_notification['last_date'][0]
        notifications_count = await CassiaUserNotificationRepository.get_user_slack_notifications_count_pool(
            last_date, db)
    else:
        notifications_count = await CassiaUserNotificationRepository.get_total_slack_notifications_count_pool(db)
    return success_response(data={'notifications_count': notifications_count})


async def get_count_backup(current_session):
    cassia_user_notification = await CassiaUserNotificationRepository.get_user_slack_notification(current_session.user_id)
    if not cassia_user_notification.empty:
        last_date = cassia_user_notification['last_date'][0]
        notifications_count = await CassiaUserNotificationRepository.get_user_slack_notifications_count(
            last_date)
    else:
        notifications_count = await CassiaUserNotificationRepository.get_total_slack_notifications_count()
    return success_response(data={'notifications_count': notifications_count})


async def get_items(skip, limit, current_session):
    cassia_user_notification = await CassiaUserNotificationRepository.get_user_slack_notification(current_session.user_id)
    notifications = await CassiaUserNotificationRepository.get_notifications(
        skip, limit)
    if not notifications.empty:
        if not cassia_user_notification.empty:
            last_date = cassia_user_notification['last_date'][0]
            notifications['seen'] = notifications['message_date'].apply(
                lambda date: 1 if date <= last_date else 0)
        else:
            notifications['seen'] = notifications['message_date'].apply(
                lambda date: 0)
        notifications.replace(np.nan, "")
        notifications['eventid'] = notifications['eventid'].apply(
            lambda event: str(event))

        await CassiaUserNotificationRepository.update_user_notification_register(current_session)

    return success_response(data=notifications.to_dict(orient='records'))
