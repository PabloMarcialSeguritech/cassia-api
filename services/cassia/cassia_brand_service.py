from fastapi import HTTPException,status
from infraestructure.database import DB
from infraestructure.cassia import cassia_brand_repository
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export
import csv
import json
import pandas as pd
from io import StringIO, BytesIO

async def fetch_all_brands(db: DB):
    try:
        return await cassia_brand_repository.get_all_brands(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener todos los brands: {e}")


async def export_brands_data(export_data, db):
    if len(export_data.brand_ids) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Selecciona al menos una marca")
    brand_ids = ",".join([str(dispId) for dispId in export_data.brand_ids])
    technologies_data = await cassia_brand_repository.fetch_brands_by_ids(brand_ids, db)
    try:
        now = get_datetime_now_str_with_tz()
        export_file = await generate_file_export(technologies_data, page_name='marcas', filename=f"marcas - {now}",
                                                 file_type=export_data.file_type)
        return export_file
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en export_technologies_data {e}")