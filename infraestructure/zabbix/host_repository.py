import infraestructure.database_model as db
from fastapi.exceptions import HTTPException
from fastapi import status
import infraestructure.db_queries_model as db_queries_model
import pandas as pd
import numpy as np


async def get_host_health_detail(host_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (host_id,)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_host_health_detail_data,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_host_view_pool(municipalityId, dispId, subtype_id, db):
    db_queries = db_queries_model.DBQueries()
    if subtype_id is None:
        subtype_id = ''
    stored_procedure_params = (
        f'{municipalityId}', dispId, f'{subtype_id}',)
    try:

        database_response = await db.run_stored_procedure(db_queries.stored_name_get_host_view_data,
                                                          stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_host_view(municipalityId, dispId, subtype_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    if subtype_id is None:
        subtype_id = ''
    stored_procedure_params = (
        f'{municipalityId}', dispId, f'{subtype_id}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_host_view_data,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_host_correlation_pool(host_ids, db):
    db_queries = db_queries_model.DBQueries()

    try:

        database_response = await db.run_query(
            db_queries.builder_query_statement_get_host_correlation(host_ids))
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_host_correlation(host_ids):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(
            db_queries.builder_query_statement_get_host_correlation(host_ids))
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_problems_by_severity_pool(municipalityId, dispId_filter, subtype_id, db):
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipalityId}', f'{dispId_filter}', f'{subtype_id}',)
    try:

        database_response = await db.run_stored_procedure(db_queries.stored_name_problems_severity,
                                                          stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_problems_by_severity(municipalityId, dispId_filter, subtype_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipalityId}', f'{dispId_filter}', f'{subtype_id}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_problems_severity,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_arch_traffic_events_date_close_null_pool(db):
    db_queries = db_queries_model.DBQueries()

    try:

        # PINK
        database_response = await db.run_query(
            db_queries.query_statement_get_arch_traffic_events_date_close_null_test)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_arch_traffic_events_date_close_null():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        # PINK
        database_response = await db_connection.run_query(
            db_queries.query_statement_get_arch_traffic_events_date_close_null_test)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_catalog_city_pool(db):
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = ''
    try:

        database_response = await db.run_stored_procedure(db_queries.stored_name_catalog_city,
                                                          stored_procedure_params)
        print(database_response)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error en get_catalog_city {e}"
        )


async def get_catalog_city():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = ''
    try:
        await db_connection.start_connection()

        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_catalog_city,
                                                                     stored_procedure_params)
        print(database_response)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error en get_catalog_city {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_arch_traffic_events_date_close_null_municipality_pool(municipality, db):
    db_queries = db_queries_model.DBQueries()
    try:
        database_response = await db.run_query(
            db_queries.builder_query_statement_get_arch_traffic_events_date_close_null_municipality_test(municipality))
        alerts_df = pd.DataFrame(database_response).replace(np.nan, None)
        return alerts_df

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_arch_traffic_events_date_close_null_municipality(municipality):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(
            db_queries.builder_query_statement_get_arch_traffic_events_date_close_null_municipality_test(municipality))
        alerts_df = pd.DataFrame(database_response).replace(np.nan, None)
        return alerts_df

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_host_available_ping_loss_pool(municipalityId, dispId, db):

    db_queries = db_queries_model.DBQueries()
    subtype = ''
    stored_procedure_params = (f'{municipalityId}', f'{dispId}', f'{subtype}',)
    try:
        database_response = await db.run_stored_procedure(
            db_queries.stored_name_get_host_available_ping_loss_data,
            stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_host_available_ping_loss(municipalityId, dispId):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    subtype = ''
    stored_procedure_params = (f'{municipalityId}', f'{dispId}', f'{subtype}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(
            db_queries.stored_name_get_host_available_ping_loss_data,
            stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_config_data_by_name(name):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(
            db_queries.builder_query_statement_get_config_data_by_name(name))
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_view_problem_data(municipalityId, dispId, subtype):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    severity = ''
    stored_procedure_params = (
        f'{municipalityId}', f'{dispId}', f'{subtype}', f'{severity}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(
            db_queries.stored_name_get_view_problem_data,
            stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_diagnostic_problems(municipalityId, dispId):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (f'{municipalityId}', f'{dispId}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_diagnostic_problems,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_data_problems(host_ids):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(
            db_queries.builder_query_statement_get_data_problems(host_ids))
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_diagnostic_problems_d(municipality_id, disp_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (f'{municipality_id}', f'{disp_id}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_diagnostic_problems_d,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_total_synchronized():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(
            db_queries.query_statement_get_total_synchronized_data)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_metric_view_h_pool(municipality_id, disp_id, subtype_id, db):
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipality_id}', f'{disp_id}', f'{subtype_id}',)
    try:

        database_response = await db.run_stored_procedure(db_queries.stored_name_get_metric_view_h_data,
                                                          stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_metric_view_h(municipality_id, disp_id, subtype_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipality_id}', f'{disp_id}', f'{subtype_id}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_metric_view_h_data,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_switch_through_put_pool(municipality_id, switch_id, metric_switch_val, db):
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipality_id}', f'{switch_id}', f'{metric_switch_val}',)
    print(stored_procedure_params)
    try:
        database_response = await db.run_stored_procedure(db_queries.stored_name_get_switch_through_put_data,
                                                          stored_procedure_params)

        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )


async def get_switch_through_put(municipality_id, switch_id, metric_switch_val):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipality_id}', f'{switch_id}', f'{metric_switch_val}',)
    print(stored_procedure_params)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_switch_through_put_data,
                                                                     stored_procedure_params)

        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()
