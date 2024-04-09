import infraestructure.database_model as db
from fastapi.exceptions import HTTPException
from fastapi import status
import infraestructure.db_queries_model as db_queries_model


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


async def get_switch_config():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(db_queries.query_statement_get_switch_config_data)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_switch_throughput_config():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(db_queries.query_statement_get_switch_throughput_config_data)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_rfid_config():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(db_queries.query_statement_get_rfid_config_data)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_host_view(municipalityId, dispId, subtype_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    if subtype_id is None:
        subtype_id = ''
    stored_procedure_params = (
        f'{municipalityId}', dispId, f'{subtype_id}',)
    print("stored_procedure_params::::", stored_procedure_params)
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
