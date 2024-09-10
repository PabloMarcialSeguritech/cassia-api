import infraestructure.database_model as db
from fastapi.exceptions import HTTPException
from fastapi import status
import infraestructure.db_queries_model as db_queries_model
import pandas as pd
import numpy as np


async def get_catalog_city() -> pd.DataFrame:

    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    try:
        await db_connection.start_connection()
        print(db_queries.stored_name_catalog_city)
        catalog_city_data = await db_connection.run_stored_procedure(db_queries.stored_name_catalog_city,
                                                                     ())
        catalog_city_df = pd.DataFrame(catalog_city_data).replace(np.nan, None)
        return catalog_city_df

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error en get_catalog_city {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_device_type_catalog(municipality_id: int) -> pd.DataFrame:
    db_connection = db.DB()
    try:
        sp_name_catalog_devices = db_queries_model.DBQueries().stored_name_catalog_devices_types
        await db_connection.start_connection()

        database_response = await db_connection.run_stored_procedure(sp_name_catalog_devices,
                                                                     (municipality_id,))
        data_df = pd.DataFrame(database_response).replace(np.nan, None)
        return data_df

    except Exception as e:
        print(f"Excepcion en get_device_type_catalog {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_device_type_catalog {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_tech_metrics_catalog(tech_id: int) -> pd.DataFrame:
    db_connection = db.DB()
    try:
        sp_name_catalog_metrics = db_queries_model.DBQueries().stored_name_catalog_metric
        await db_connection.start_connection()

        metrics_data = await db_connection.run_stored_procedure(sp_name_catalog_metrics,
                                                                (tech_id,))
        metrics_df = pd.DataFrame(metrics_data).replace(np.nan, None)
        return metrics_df

    except Exception as e:
        print(f"Excepcion en get_tech_metrics_catalog {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_tech_metrics_catalog {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_device_brands_catalog(tech_id: str) -> pd.DataFrame:
    db_connection = db.DB()
    try:
        sp_name_catalog_metrics = db_queries_model.DBQueries(
        ).stored_name_catalog_devices_brands
        await db_connection.start_connection()

        brands_data = await db_connection.run_stored_procedure(sp_name_catalog_metrics,
                                                               (tech_id,))
        brands_df = pd.DataFrame(brands_data).replace(np.nan, None)
        return brands_df

    except Exception as e:
        print(f"Excepcion en get_device_brands_catalog {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_device_brands_catalog {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_device_models_catalog_by_brand(brand_id: str) -> pd.DataFrame:
    db_connection = db.DB()
    try:
        sp_name_catalog_models = db_queries_model.DBQueries(
        ).stored_name_catalog_models
        await db_connection.start_connection()

        models_data = await db_connection.run_stored_procedure(sp_name_catalog_models,
                                                               (brand_id,))
        models_df = pd.DataFrame(models_data).replace(np.nan, None)
        return models_df

    except Exception as e:
        print(f"Excepcion en get_device_models_catalog_by_brand {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_device_models_catalog_by_brand {e}"
        )
    finally:
        await db_connection.close_connection()
