from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text, func

import numpy as np
from utils.traits import success_response
from models.cassia_slack_user_notification import CassiaSlackUserNotification
from models.cassia_slack_notification import CassiaSlackNotification
from fastapi import HTTPException, status
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta
settings = Settings()


async def get_count(current_session):
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
            CassiaSlackNotification).offset(
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
            if not last_date:
                create_register = CassiaSlackUserNotification(
                    user_id=current_session.user_id,
                    last_date=datetime.now()
                )
                session.add(create_register)
            else:
                last_date.last_date = datetime.now()
            session.commit()
        print(df_result)

    return success_response(data=df_result.to_dict(orient='records'))
