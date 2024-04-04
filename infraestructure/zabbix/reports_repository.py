import infraestructure.database_model as db
from fastapi.exceptions import HTTPException
from fastapi import status
import infraestructure.db_queries_model as db_queries_model


async def get_connectivity_data_m(tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_connectivity_data_m,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_connectivity_data(municipality_id, tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
    f'{municipality_id}', f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_connectivity_data,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_alineacion_group_id():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(db_queries.query_statement_get_alineacion_group_id)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_aligment_report_m(tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_aligment_report_data_m,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_metrics_template(tech_id, alineacion_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(
            db_queries.builder_query_statement_get_metrics_template(tech_id, alineacion_id))
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()

async def get_aligment_report(municipality_id, tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipality_id}', f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_aligment_report_data,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()
