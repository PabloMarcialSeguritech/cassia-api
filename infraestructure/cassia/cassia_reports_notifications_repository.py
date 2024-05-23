from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_ci_document1 import CassiaCIDocument1
import pandas as pd
import numpy as np
import os
import ntpath
import shutil
from datetime import datetime


async def get_reports_to_process(lower_limmit, upper_limit):
    db_model = DB()
    try:
        query_reports_to_process = DBQueries(
        ).builder_query_statement_get_reports_to_process(lower_limmit, upper_limit)
        await db_model.start_connection()
        reports_to_process_data = await db_model.run_query(query_reports_to_process)
        reports_to_process_df = pd.DataFrame(reports_to_process_data)
        return reports_to_process_df

    except Exception as e:
        print(f"Excepcion en get_reports_to_process: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_reports_to_process: {e}")
    finally:
        await db_model.close_connection()


async def get_user_emails_by_report_id(cassia_report_frequency_schedule_id):
    db_model = DB()
    try:
        query_get_user_reports_mails = DBQueries(
        ).builder_query_get_user_reports_mail_by_schedule_id(cassia_report_frequency_schedule_id)
        await db_model.start_connection()
        user_mails_data = await db_model.run_query(query_get_user_reports_mails)
        user_mails_df = pd.DataFrame(user_mails_data)
        return user_mails_df

    except Exception as e:
        print(f"Excepcion en get_user_emails_by_report_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_user_emails_by_report_id: {e}")
    finally:
        await db_model.close_connection()


async def get_report_names():
    db_model = DB()
    try:
        query_get_reports_names = DBQueries(
        ).query_get_cassia_report_names
        await db_model.start_connection()
        reports_names_data = await db_model.run_query(query_get_reports_names)
        reports_names_df = pd.DataFrame(reports_names_data)
        return reports_names_df

    except Exception as e:
        print(f"Excepcion en get_report_names: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_report_names: {e}")
    finally:
        await db_model.close_connection()


async def get_users():
    db_model = DB()
    try:
        query_get_users = DBQueries(
        ).query_get_cassia_users
        await db_model.start_connection()
        get_users_data = await db_model.run_query(query_get_users)
        get_users_df = pd.DataFrame(get_users_data)
        return get_users_df

    except Exception as e:
        print(f"Excepcion en get_users: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_users: {e}")
    finally:
        await db_model.close_connection()


async def get_user_reports():
    db_model = DB()
    try:
        query_get_user_reports = DBQueries(
        ).query_get_cassia_user_reports
        await db_model.start_connection()
        get_user_reports_data = await db_model.run_query(query_get_user_reports)
        get_user_reports_df = pd.DataFrame(get_user_reports_data)
        return get_user_reports_df

    except Exception as e:
        print(f"Excepcion en get_user_reports: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_user_reports: {e}")
    finally:
        await db_model.close_connection()


async def delete_user_reports_by_user_id(user_id):
    db_model = DB()
    try:
        query_delete_user_reports = DBQueries(
        ).builder_query_delete_user_reports(user_id)
        await db_model.start_connection()
        delete_user_reports = await db_model.run_query(query_delete_user_reports)
        return True
    except Exception as e:
        print(f"Excepcion en delete_user_reports_by_user_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_user_reports_by_user_id: {e}")
    finally:
        await db_model.close_connection()


async def delete_user_reports_by_user_ids(user_ids):
    db_model = DB()
    try:
        query_delete_users_reports = DBQueries(
        ).builder_query_delete_users_reports(user_ids)
        await db_model.start_connection()
        delete_users_reports = await db_model.run_query(query_delete_users_reports)
        return True
    except Exception as e:
        print(f"Excepcion en delete_user_reports_by_user_ids: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_user_reports_by_user_ids: {e}")
    finally:
        await db_model.close_connection()


async def insert_user_reports(user_id, report_ids):
    db_model = DB()
    try:
        query_insert_user_reports = DBQueries(
        ).builder_query_insert_user_reports(user_id, report_ids)
        await db_model.start_connection()
        insert_user_reports = await db_model.run_query(query_insert_user_reports)
        return True
    except Exception as e:
        print(f"Excepcion en delete_user_reports_by_user_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_user_reports_by_user_id: {e}")
    finally:
        await db_model.close_connection()


async def insert_user_reports_values(values):
    db_model = DB()
    try:
        query_insert_user_reports_values = DBQueries(
        ).builder_query_insert_user_reports_values(values)
        await db_model.start_connection()
        insert_user_reports = await db_model.run_query(query_insert_user_reports_values)
        return True
    except Exception as e:
        print(f"Excepcion en insert_user_reports_values: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en insert_user_reports_values: {e}")
    finally:
        await db_model.close_connection()
