from fastapi import HTTPException, status
from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np
from schemas import cassia_brand_schema
from models.cassia_host_brands import CassiaHostBrandModel

# Obtener todos los brands
async def get_all_brands(db: DB):
    brands_df = None
    try:
        query_statement_get_brands = DBQueries().query_statement_get_brands
        brands_data = await db.run_query(query_statement_get_brands)
        brands_df = pd.DataFrame(
            brands_data).replace(np.nan, None)
        return brands_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener los brands: {e}")


async def fetch_brands_by_ids(brand_ids, db):
    brands_df = None
    try:
        query_statement_get_brands_by_ids = DBQueries(
        ).builder_query_statement_get_brands_by_ids(brand_ids)
        brands_data = await db.run_query(query_statement_get_brands_by_ids)
        brands_df = pd.DataFrame(
            brands_data).replace(np.nan, None)
        return brands_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_tech_devices_by_ids: {e}")

async def create_brand(brand_name: str, brand_mac_address: str, db: DB):
    brand_df = None
    try:
        query_statement_create_brand = DBQueries(
        ).builder_query_statement_create_brand(brand_name,brand_mac_address )
        await db.run_query(query_statement_create_brand)
        return True
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear el brand: {e}")


async def get_brand_editable(brand_id, db) -> pd.DataFrame:
    brand_df = None
    try:
        query_statement_get_brand = DBQueries(
        ).builder_query_statement_get_brand_by(brand_id)
        brand_data = await db.run_query(query_statement_get_brand)
        brand_df = pd.DataFrame(
            brand_data).replace(np.nan, None)
        return brand_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener el brand en get_brand_editable: {e}")


async def update_host_group_name_and_type_id(hostgroup_id, hostgroup_name, hostgroup_type_id, db):
    try:
        query = DBQueries().builder_update_host_group_name_and_id_type(hostgroup_id, hostgroup_name, hostgroup_type_id)
        await db.run_query(query)
        return True
    except Exception as e:
        print(f"Exception in update_host_group_name_and_type_id: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en update_cassia_maintenance {e}"
        )


async def update_brand(brand_id, name_brand, brand_mac_address, db):
    try:
        query = DBQueries().builder_update_brand(brand_id, name_brand, brand_mac_address)
        await db.run_query(query)
        return True
    except Exception as e:
        print(f"Exception in update_brand: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en update_brand {e}")


async def delete_brand(db, brand_id):
    try:
        query = DBQueries().builder_delete_brand(brand_id)
        await db.run_query(query)
        return True
    except Exception as e:
        print(f"Exception in delete_brand: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en delete_brand {e}")


async def create_host_brand(model_data: cassia_brand_schema.CassiaBrandSchema, db: DB):
    try:
        session = await db.get_session()
        model = CassiaHostBrandModel(
                name_brand=model_data.name_brand,
                mac_address_brand_OUI=model_data.mac_address_brand_OUI,
                editable=1
        )
        session.add(model)
        await session.commit()
        await session.refresh(model)
        return model
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Error en create_host_brand: {e}")