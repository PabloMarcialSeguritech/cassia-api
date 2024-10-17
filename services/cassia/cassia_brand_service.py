from fastapi import HTTPException,status
from infraestructure.database import DB
from infraestructure.cassia import cassia_brand_repository
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export
import csv
import json
import pandas as pd
from io import StringIO, BytesIO
from schemas import cassia_brand_schema

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

# Servicio para crear un nuevo brand (editable = 0)
async def create_new_brand(brand_data: cassia_brand_schema.CassiaBrandSchema, db: DB):
    brand_data_dict = brand_data.dict()
    brand_name = brand_data_dict['name_brand']
    brand_mac_address = brand_data_dict['mac_address_brand_OUI']
    try:
        is_correct = await cassia_brand_repository.create_brand(brand_name, brand_mac_address, db)
        if is_correct:
            return success_response(
                message="Marca creada correctamente")
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al crear la marca")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear el brand: {e}")


from fastapi import HTTPException, status


async def modify_brand(db, brand_id, brand_data):
    brand_data_dict = brand_data.dict()

    # Validar si los campos están presentes en brand_data_dict
    if 'name_brand' not in brand_data_dict:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El campo 'name_brand' es requerido."
        )

    # Extraer los datos del brand y validarlos
    brand_name = brand_data_dict['name_brand']
    brand_mac_address = brand_data_dict['mac_address_brand_OUI']  # Si es el campo correcto

    # Validar que el nombre de la marca no esté vacío
    if not brand_name or not isinstance(brand_name, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El nombre de la marca debe ser una cadena de texto válida."
        )

    try:
        # Obtener el DataFrame del repositorio
        brand_df = await  cassia_brand_repository.get_brand_editable(brand_id, db)

        # Verificar si se encontró el registro
        if brand_df.empty:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No existe el Brand."
            )
        # Validar si el Brand es editable
        if brand_df.iloc[0]['editable'] != 1:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Este brand no es editable."
            )

        # Actualizar el brand
        is_correct = await cassia_brand_repository.update_brand(
            brand_id, brand_name, brand_mac_address, db
        )

        if is_correct:
            return success_response(
                message="Marca actualizada correctamente"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al actualizar la marca."
            )

    except HTTPException as e:
        raise e  # Relanzar la excepción si ya es un HTTPException
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al actualizar el brand con ID {brand_id}: {str(e)}"
        )


async def remove_brand(brand_id, db):
    try:
        # Obtener el DataFrame del repositorio
        brand_df = await cassia_brand_repository.get_brand_editable(brand_id, db)

        # Verificar si se encontró el registro
        if brand_df.empty:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No existe el Brand."
            )

        # Validar si el Brand es editable
        if brand_df.iloc[0]['editable'] != 1:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Este brand no es editable."
            )

        # Actualizar el brand
        is_correct = await cassia_brand_repository.delete_brand(db, brand_id)

        if is_correct:
            return success_response(
                message="Marca borrada correctamente"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al eliminar el brand."
            )

    except HTTPException as e:
        raise e  # Relanzar la excepción si ya es un HTTPException
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al eliminar el brand: {e}"
        )
