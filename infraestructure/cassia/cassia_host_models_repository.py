from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
from models.cassia_host_models import CassiaHostModelsModel
import pandas as pd
import numpy as np
from schemas import cassia_host_models_schema


async def update_host_model(model_id: int, model_data: cassia_host_models_schema.CassiaHostModelSchema, db: DB):
    try:
        query_statement_update_host_model = DBQueries(
        ).builder_query_statement_update_host_model(model_id, model_data)

        host_model_data = await db.run_query(query_statement_update_host_model)
        return True
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_model_by_name_and_brand_id: {e}")


async def get_cassia_host_model_by_name_and_brand_id(model_data: cassia_host_models_schema.CassiaHostModelSchema, db: DB) -> pd.DataFrame:
    host_model_df = pd.DataFrame()
    try:
        query_statement_get_cassia_host_models_by_name_and_brand_id = DBQueries(
        ).builder_query_statement_get_cassia_host_models_by_name_and_brand_id(model_data)
        print(query_statement_get_cassia_host_models_by_name_and_brand_id)

        host_model_data = await db.run_query(query_statement_get_cassia_host_models_by_name_and_brand_id)
        host_model_df = pd.DataFrame(
            host_model_data).replace(np.nan, None)
        print(host_model_df)
        return host_model_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_model_by_name_and_brand_id: {e}")


async def get_cassia_host_models_by_ids(model_ids, db: DB) -> pd.DataFrame:
    host_model_df = pd.DataFrame()
    try:
        query_statement_get_cassia_host_models_by_ids = DBQueries(
        ).builder_query_statement_get_cassia_host_models_by_ids(model_ids)

        host_model_data = await db.run_query(query_statement_get_cassia_host_models_by_ids)
        host_model_df = pd.DataFrame(
            host_model_data).replace(np.nan, None)
        return host_model_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_models_by_ids: {e}")


async def get_cassia_host_model_by_id(model_id, db: DB) -> pd.DataFrame:
    host_model_df = pd.DataFrame()
    try:
        query_statement_create_cassia_host_model = DBQueries(
        ).builder_query_statement_get_cassia_host_model_by_id(model_id)

        host_model_data = await db.run_query(query_statement_create_cassia_host_model)
        host_model_df = pd.DataFrame(
            host_model_data).replace(np.nan, None)
        return host_model_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_model_by_id: {e}")


async def delete_cassia_host_model_by_id(model_id, db) -> pd.DataFrame:
    host_model_df = None
    try:
        query_statement_create_cassia_host_model = DBQueries(
        ).builder_query_statement_delete_cassia_host_model_by_id(model_id)

        host_model_data = await db.run_query(query_statement_create_cassia_host_model)
        return True
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en delete_cassia_host_model_by_id: {e}")


async def create_host_model(model_data: cassia_host_models_schema.CassiaHostModelSchema, db: DB):
    try:
        session = await db.get_session()
        model = CassiaHostModelsModel(
            name_model=model_data.name_model,
            brand_id=model_data.brand_id,
            editable=1
        )
        session.add(model)
        await session.commit()
        await session.refresh(model)
        return model
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en create_host_model: {e}")


async def get_cassia_host_brand_by_id(brand_id, db) -> pd.DataFrame:
    host_brand_df = None
    try:
        query_statement_get_brand_by_id = DBQueries(
        ).builder_query_statement_get_brand_by_id(brand_id)

        host_brand_data = await db.run_query(query_statement_get_brand_by_id)
        host_brand_df = pd.DataFrame(
            host_brand_data).replace(np.nan, None)
        return host_brand_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_brand: {e}")


async def get_cassia_host_models(db: DB) -> pd.DataFrame:
    host_models_df = None
    try:
        query_statement_get_cassia_host_models = DBQueries(
        ).query_statement_get_cassia_host_models

        host_models_data = await db.run_query(query_statement_get_cassia_host_models)
        host_models_df = pd.DataFrame(
            host_models_data).replace(np.nan, None)
        return host_models_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_models: {e}")
