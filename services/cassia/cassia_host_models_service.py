from fastapi import status, HTTPException
from fastapi.responses import FileResponse
from infraestructure.cassia import cassia_host_models_repository
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export, get_df_by_filetype
from fastapi import File
from schemas import cassia_host_models_schema
from infraestructure.database import DB
import asyncio
import pandas as pd
import numpy as np


async def get_host_models_by_brand(brand_id: int, db: DB):
    brand = await cassia_host_models_repository.get_cassia_host_brand_by_id(brand_id, db)
    if brand.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El brand_id proporcionado no existe")
    host_models = await cassia_host_models_repository.get_cassia_host_models_by_brand(brand_id, db)
    if not host_models.empty:
        host_models['id'] = host_models['model_id']
    return success_response(data=host_models.to_dict(orient="records"))


async def get_host_models(db: DB):
    host_models = await cassia_host_models_repository.get_cassia_host_models(db)
    if not host_models.empty:
        host_models['id'] = host_models['model_id']
    return success_response(data=host_models.to_dict(orient="records"))


async def crate_host_model(db: DB, model_data: cassia_host_models_schema.CassiaHostModelSchema):
    host_brand_exist = await cassia_host_models_repository.get_cassia_host_brand_by_id(model_data.brand_id, db)
    if host_brand_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El brand_id proporcionado no existe")
    exist_host_model_name_brand = await cassia_host_models_repository.get_cassia_host_model_by_name_and_brand_id(model_data, db)
    if not exist_host_model_name_brand.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Ya existe un modelo con este nombre perteneciente a esta marca")

    create_host_model = await cassia_host_models_repository.create_host_model(model_data, db)
    return success_response("Modelo creado correctamente", data=create_host_model)


async def delete_host_model(model_id: int, db: DB):
    model = await cassia_host_models_repository.get_cassia_host_model_by_id(model_id, db)
    if model.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El modelo no existe")
    if model['editable'].astype('int64')[0] == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"El modelo con el id proporcionado no se puede eliminar")
    delete_model = await cassia_host_models_repository.delete_cassia_host_model_by_id(model_id, db)
    return success_response(message="El modelo fue eliminado correctamente")


async def export_models_data(export_data: cassia_host_models_schema.CassiaHostModelsExportSchema, db: DB):
    if len(export_data.modelids) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Selecciona al menos un modelo")
    model_ids = ",".join([str(model_id) for model_id in export_data.modelids])
    models_data = await cassia_host_models_repository.get_cassia_host_models_by_ids(model_ids, db)
    try:
        now = get_datetime_now_str_with_tz()
        export_file = await generate_file_export(models_data, page_name='modelos', filename=f"modelos - {now}", file_type=export_data.file_type)
        return export_file
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en export_models_data {e}")


async def import_models_data(file_import: File, db):
    file_types = ('.csv', '.xlsx', '.xls', '.json')
    if not file_import.filename.endswith(file_types):
        raise HTTPException(
            status_code=400,
            detail="El archivo debe ser un CSV, JSON, XLS o XLSX"
        )
    processed_data = await get_df_by_filetype(file_import, ['model_id', 'name_model', 'brand_id'])
    result = processed_data['result']
    if not result:
        exception = processed_data['exception']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al procesar el archivo: {exception}")
    df_import = processed_data['df']
    duplicados = df_import.duplicated(
        subset=['name_model', 'brand_id'], keep=False).any()
    if duplicados:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Existen registros duplicados en el archivo.")
    tasks_create = [asyncio.create_task(crate_host_model_by_import(
        db, df_import.iloc[group_data_ind].to_dict())) for group_data_ind in df_import.index]
    df_import_results = pd.DataFrame(
        columns=['model_id', 'name_model', 'brand_id', 'result', 'detail', 'modelid_creado'])
    for i in range(0, len(tasks_create), 10):

        lote = tasks_create[i:i+10]
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
        df_import_results['model_id'] = df_import_results['model_id'].astype(
            'int64')
        df_import_results['brand_id'] = df_import_results['brand_id'].astype(
            'int64')
        df_import_results = df_import_results.replace(np.nan, None)
    return success_response(data=df_import_results.to_dict(orient='records'))


async def crate_host_model_by_import(db: DB, model_data):
    response = model_data
    response['result'] = 'No se creo correctamente el registro'
    response['detail'] = ''
    response['modelid_creado'] = ''
    model_data = cassia_host_models_schema.CassiaHostModelSchema(
        name_model=model_data['name_model'], brand_id=model_data['brand_id'])
    host_brand_exist = await cassia_host_models_repository.get_cassia_host_brand_by_id(model_data.brand_id, db)
    if host_brand_exist.empty:
        response['detail'] = "El brand_id proporcionado no existe"
        return response
    exist_host_model_name_brand = await cassia_host_models_repository.get_cassia_host_model_by_name_and_brand_id(model_data, db)
    if not exist_host_model_name_brand.empty:
        response['detail'] = "Ya existe un modelo con este nombre perteneciente a esta marca"
        return response
    create_host_model = await cassia_host_models_repository.create_host_model(model_data, db)
    response['result'] = 'Se creo correctamente el registro'
    response['modelid_creado'] = create_host_model.model_id
    return response


async def update_host_model(model_id: int, model_data: cassia_host_models_schema.CassiaHostModelSchema, db: DB):
    model_exist = await cassia_host_models_repository.get_cassia_host_model_by_id(model_id, db)
    if model_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"El modelo con el id proporcionado no existe")
    if model_exist['editable'].astype('int64')[0] == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"El modelo con el id proporcionado no se puede editar")
    brand_exist = await cassia_host_models_repository.get_cassia_host_brand_by_id(model_data.brand_id, db)
    if brand_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"La marca proporcionada no existe")
    model_name_brand_exist = await cassia_host_models_repository.get_cassia_host_model_by_name_and_brand_id(model_data, db)
    if not model_name_brand_exist.empty:
        if model_name_brand_exist['model_id'].astype('int64')[0] != model_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Ya existe un modelo de esta marca con este nombre.")
    update_model = await cassia_host_models_repository.update_host_model(model_id, model_data, db)
    return success_response(message="El modelo fue actualizado correctamente")
