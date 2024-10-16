from fastapi import HTTPException, status
from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np

# Obtener todos los brands
async def get_all_brands(db: DB):
    query = """
        SELECT brand_id, name_brand, mac_address_brand_OUI, editable
        FROM cassia_host_brand;
    """
    try:
        return await db.run_query(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener los brands: {e}")


async def fetch_brands_by_ids(brand_ids, db):
    brands_df = None
    try:
        query_statement_get_technologies_by_ids = DBQueries(
        ).builder_query_statement_get_brands_by_ids(brand_ids)
        brands_data = await db.run_query(query_statement_get_technologies_by_ids)
        brands_df = pd.DataFrame(
            brands_data).replace(np.nan, None)
        return brands_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_tech_devices_by_ids: {e}")