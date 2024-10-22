from fastapi import HTTPException,status
from infraestructure.database import DB
from infraestructure.cassia import cassia_brand_repository, cassia_user_repository
from models.cassia_host_brands import CassiaHostBrandModel
from models.cassia_user_session import CassiaUserSession
from schemas.cassia_audit_schema import CassiaAuditSchema
from services.cassia import cassia_audit_service
from utils.actions_modules_enum import AuditModule, AuditAction
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export
import csv
import json
import pandas as pd
from io import StringIO, BytesIO
from schemas import cassia_brand_schema
from utils.exports_imports_functions import generate_file_export, get_df_by_filetype
import asyncio
import numpy as np


async def fetch_all_brands(db: DB):
    try:
        brands_df = await cassia_brand_repository.get_all_brands(db)
        if not brands_df.empty:
            brands_df['id'] = brands_df['brand_id']
        return success_response(data=brands_df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al obtener todos los brands: {e}")


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
async def create_new_brand(brand_data: cassia_brand_schema.CassiaBrandSchema, current_user, db: DB):
    brand_data_dict = brand_data.dict()
    brand_name = brand_data_dict['name_brand'].strip()
    brand_mac_address = brand_data_dict['mac_address_brand_OUI'].strip()

    try:
        brands_df = await cassia_brand_repository.get_all_brands(db)

        # Verificar si el nombre de la marca ya existe en el DataFrame
        if brand_name in brands_df['name_brand'].values:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"La marca '{brand_name}' ya existe")

            # Verificar si la dirección MAC ya existe en el DataFrame
        if brand_mac_address in brands_df['mac_address_brand_OUI'].values:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"La dirección MAC '{brand_mac_address}' ya está registrada")

        # Crear la marca usando el repositorio
        new_brand = await cassia_brand_repository.create_host_brand(brand_data, db)

        # Verificar si la marca tiene un ID (lo que indicaría que fue creada con éxito)
        if new_brand.brand_id:
            await create_brand_audit_log(new_brand, current_user, db)
            return success_response(
                message="Marca creada correctamente",
                data=new_brand
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al crear la marca. No se generó un ID."
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al crear el brand: {e}")

async def modify_brand(db, brand_id, brand_data, current_user):
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
        is_correct = await cassia_brand_repository.update_brand(
            brand_id, brand_name, brand_mac_address, db
        )

        if is_correct:
            await update_brand_audit_log(brand_data_dict, brand_df, current_user, db)
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


async def remove_brand(brand_id, current_user, db):
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
            await delete_brand_audit_log(brand_df, current_user, db)
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


async def import_brands_data(file_import, db):
    file_types = ('.csv', '.xlsx', '.xls', '.json')
    if not file_import.filename.endswith(file_types):
        raise HTTPException(
            status_code=400,
            detail="El archivo debe ser un CSV, JSON, XLS o XLSX"
        )
    processed_data = await get_df_by_filetype(file_import, ['name_brand', 'mac_address_brand_OUI'])
    result = processed_data['result']
    if not result:
        exception = processed_data['exception']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al procesar el archivo: {exception}")
    df_import = processed_data['df']
    duplicados = df_import.duplicated(
        subset=['name_brand', 'mac_address_brand_OUI'], keep=False).any()
    if duplicados:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Existen registros duplicados en el archivo.")
    brands_df = await cassia_brand_repository.get_all_brands(db)
    tasks_create = [asyncio.create_task(create_host_brand_by_import(
        db, df_import.iloc[group_data_ind].to_dict(), brands_df)) for group_data_ind in df_import.index]
    df_import_results = pd.DataFrame(
        columns=['brand_id', 'name_brand', 'mac_address_brand_OUI', 'result', 'detail', 'brand_id_creado'])
    for i in range(0, len(tasks_create), 10):

        lote = tasks_create[i:i + 10]
        print(lote)
        # Ejecutar las corutinas de forma concurrente
        resultados = await asyncio.gather(*lote)
        for resultado in resultados:
            print(resultado)
            new_row = pd.DataFrame(resultado, index=[0])
            # Concatenar el nuevo registro al DataFrame original
            df_import_results = pd.concat(
                [df_import_results, new_row], ignore_index=True)
    if not df_import_results.empty:
        df_import_results['brand_id'] = df_import_results['brand_id'].astype(
            'int64')
        df_import_results = df_import_results.replace(np.nan, None)
    return success_response(data=df_import_results.to_dict(orient='records'))

async def create_host_brand_by_import(db: DB, model_data, brands_df):
    response = model_data
    response['result'] = 'No se creo correctamente el registro'
    response['detail'] = ''
    response['brand_id_creado'] = ''
    model_data = cassia_brand_schema.CassiaBrandSchema(
        name_brand=model_data['name_brand'], mac_address_brand_OUI=model_data['mac_address_brand_OUI'])
    # Verificar si el nombre de la marca ya existe en el DataFrame
    if model_data.name_brand in brands_df['name_brand'].values:
        response['detail'] = f"La marca '{model_data.name_brand}' ya existe"
        return response

        # Verificar si la dirección MAC ya existe en el DataFrame
    if model_data.mac_address_brand_OUI in brands_df['mac_address_brand_OUI'].values:
        response['detail'] = f"La dirección MAC '{model_data.mac_address_brand_OUI}' ya está registrada"
        return response
    create_host_brand = await cassia_brand_repository.create_host_brand(model_data, db)
    response['result'] = 'Se creo correctamente el registro'
    response['brand_id_creado'] = create_host_brand.brand_id
    return response

async def create_brand_audit_log(new_brand: CassiaHostBrandModel, current_user: CassiaUserSession, db: DB):

    try:
        module_id = AuditModule.BRANDS.value
        action_id = AuditAction.CREATE.value

        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        detail = f"Se creo una  marca con los siguientes datos, name_brand: {new_brand.name_brand}, mac_address_brand_OUI: {new_brand.mac_address_brand_OUI}"

        cassia_audit_schema = CassiaAuditSchema(
            user_name = user.name,
            user_email = user.mail,
            summary = detail,
            id_audit_action = action_id,
            id_audit_module = module_id
        )

        await cassia_audit_service.create_audit_log(cassia_audit_schema, db)

    except Exception as e:
        print(f"Error en create_brand_audit_log: {e}")


async def delete_brand_audit_log(brand ,
                                 current_user: CassiaUserSession, db: DB):
    try:
        module_id = AuditModule.BRANDS.value
        action_id = AuditAction.DELETE.value

        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        detail = f"Se elimino la marca con los siguientes datos, name_brand: {brand.iloc[0]['name_brand']}, mac_address_brand_OUI: {brand.iloc[0]['mac_address_brand_OUI']}"

        cassia_audit_schema = CassiaAuditSchema(
            user_name=user.name,
            user_email=user.mail,
            summary=detail,
            id_audit_action=action_id,
            id_audit_module=module_id
        )

        await cassia_audit_service.create_audit_log(cassia_audit_schema, db)

    except Exception as e:
        print(f"Error en delete_brand_audit_log: {e}")

async def update_brand_audit_log(brand_data_new: dict,
                                 brand_data_current,
                                 current_user: CassiaUserSession,
                                 db: DB):
    try:
        module_id = AuditModule.BRANDS.value
        action_id = AuditAction.UPDATE.value

        # Obtener el usuario actual
        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        # Verificar que brand_data_current no esté vacío
        if brand_data_current is None or brand_data_new is None:
            raise ValueError("Los datos de la marca actual o nuevos son inválidos o están vacíos.")

        # Si brand_data_current es un DataFrame, usa iloc; de lo contrario, accede directamente al diccionario o lista
        if isinstance(brand_data_current, pd.DataFrame):
            name_brand_current = brand_data_current.iloc[0]['name_brand']
            mac_address_current = brand_data_current.iloc[0]['mac_address_brand_OUI']
        else:
            name_brand_current = brand_data_current['name_brand']
            mac_address_current = brand_data_current['mac_address_brand_OUI']

        # Acceder a los datos de brand_data_new desde el diccionario
        name_brand_new = brand_data_new['name_brand']
        mac_address_new = brand_data_new['mac_address_brand_OUI']

        # Crear el detalle de la auditoría
        detail = (f"Se actualizó una marca. Sus datos anteriores eran, name_brand: {name_brand_current}, "
                  f"mac_address_brand_OUI: {mac_address_current}. "
                  f"Sus nuevos datos son: name_brand {name_brand_new}, "
                  f"mac_address_brand_OUI: {mac_address_new}")

        # Crear el objeto de auditoría
        cassia_audit_schema = CassiaAuditSchema(
            user_name=user.name,
            user_email=user.mail,
            summary=detail,
            id_audit_action=action_id,
            id_audit_module=module_id
        )

        # Crear el registro de auditoría
        await cassia_audit_service.create_audit_log(cassia_audit_schema, db)

    except ValueError as ve:
        print(f"Error de validación: {ve}")
    except Exception as e:
        print(f"Error en update_brand_audit_log: {e}")


