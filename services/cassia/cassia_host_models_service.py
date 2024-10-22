from fastapi import status, HTTPException
from fastapi.responses import FileResponse
from infraestructure.cassia import cassia_host_models_repository, cassia_brand_repository, cassia_user_repository
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from models.cassia_user_session import CassiaUserSession
from schemas.cassia_audit_schema import CassiaAuditSchema
from services.cassia import cassia_audit_service
from utils.actions_modules_enum import AuditModule, AuditAction
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


async def create_host_model(db: DB, model_data: cassia_host_models_schema.CassiaHostModelSchema, current_user):
    host_brand_exist = await cassia_host_models_repository.get_cassia_host_brand_by_id(model_data.brand_id, db)
    if host_brand_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El brand_id proporcionado no existe")
    exist_host_model_name_brand = await cassia_host_models_repository.get_cassia_host_model_by_name_and_brand_id(model_data, db)
    if not exist_host_model_name_brand.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Ya existe un modelo con este nombre perteneciente a esta marca")

    create_host_model = await cassia_host_models_repository.create_host_model(model_data, db)
    await create_model_audit_log(create_host_model, current_user, db)
    return success_response("Modelo creado correctamente", data=create_host_model)


async def delete_host_model(model_id: int, current_user, db: DB):
    model = await cassia_host_models_repository.get_cassia_host_model_by_id(model_id, db)
    if model.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El modelo no existe")
    if model['editable'].astype('int64')[0] == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"El modelo con el id proporcionado no se puede eliminar")
    delete_model = await cassia_host_models_repository.delete_cassia_host_model_by_id(model_id, db)
    await delete_model_audit_log(model, current_user, db)
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


async def update_host_model(model_id: int, model_data: cassia_host_models_schema.CassiaHostModelSchema, current_user, db: DB):
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
    await update_model_audit_log(model_id, model_data, model_exist, current_user, db)
    return success_response(message="El modelo fue actualizado correctamente")


async def create_model_audit_log(model_data: cassia_host_models_schema.CassiaHostModelSchema, current_user: CassiaUserSession, db: DB):

    try:
        module_id = AuditModule.MODELS.value
        action_id = AuditAction.CREATE.value
        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)
        brand_df = await  cassia_brand_repository.get_brand_editable(model_data.brand_id, db)
        brand_type_name = ""
        if not brand_df.empty:
            brand_type_name = brand_df.iloc[0]['name_brand']

        detail = f"Se creo un modelo con los siguientes datos, name_model: {model_data.name_model}, brand_name: {brand_type_name}"

        cassia_audit_schema = CassiaAuditSchema(
            user_name = user.name,
            user_email = user.mail,
            summary = detail,
            id_audit_action = action_id,
            id_audit_module = module_id
        )

        await cassia_audit_service.create_audit_log(cassia_audit_schema, db)

    except Exception as e:
        print(f"Error en create_group_audit_log: {e}")


async def delete_model_audit_log(model ,
                                 current_user: CassiaUserSession, db: DB):
    try:
        module_id = AuditModule.MODELS.value
        action_id = AuditAction.DELETE.value

        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        name_model = ""
        brand_id = None
        brand_df = pd.DataFrame({})
        brand_type_name = ""


        if not model.empty:
            name_model = model.iloc[0]['name_model']
            brand_id = model.iloc[0]['brand_id']

        if brand_id:
            brand_df = await  cassia_brand_repository.get_brand_editable(brand_id, db)

        if not brand_df.empty:
            brand_type_name = brand_df.iloc[0]['name_brand']

        detail = f"Se elimino el modelo con los siguientes datos, name_model: {name_model}, name_brand: {brand_type_name}"

        cassia_audit_schema = CassiaAuditSchema(
            user_name=user.name,
            user_email=user.mail,
            summary=detail,
            id_audit_action=action_id,
            id_audit_module=module_id
        )

        await cassia_audit_service.create_audit_log(cassia_audit_schema, db)

    except Exception as e:
        print(f"Error en delete_group_audit_log: {e}")

async def update_model_audit_log(model_id, model_data: cassia_host_models_schema.CassiaHostModelSchema,
                                 model_data_current,
                                 current_user: CassiaUserSession,
                                 db: DB):
    try:
        module_id = AuditModule.MODELS.value
        action_id = AuditAction.UPDATE.value

        # Obtener el usuario actual
        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        models_df = await cassia_host_models_repository.get_cassia_host_models(db)

        brands_df = await cassia_brand_repository.get_all_brands(db)

        if not brands_df.empty:

            # Acceder correctamente a los campos de tipo de grupo usando 'id' en lugar de 'group_type_id'
            current_brand_type_id = model_data_current.iloc[0]['brand_id']  # Desde el objeto actual
            new_brand_type_id = model_data.brand_id  # Desde el nuevo objeto (ajustado a type_id)

            # Buscar los nombres de los tipos de grupo correspondientes a los IDs en los datos actuales y nuevos
            brand_type_name_current = models_df.loc[models_df['brand_id'] == current_brand_type_id, 'name_brand'].values
            brand_type_name_new = models_df.loc[models_df['brand_id'] == new_brand_type_id, 'name_brand'].values

            # Verificar que se encontró un nombre de tipo de grupo
            if brand_type_name_current.size == 0:
                raise ValueError(f"No se encontró el nombre del tipo del modelo  para el ID {current_brand_type_id}")
            if brand_type_name_new.size == 0:
                raise ValueError(f"No se encontró el nombre del tipo del modelo  para el ID {new_brand_type_id}")

            # Extraer el valor del array resultante
            brand_type_name_current = brand_type_name_current[0]
            brand_type_name_new = brand_type_name_new[0]

            # Crear el detalle de la auditoría con los nombres de los tipos de grupo
            detail = (f"Se actualizó un modelo. Sus datos anteriores eran: model_name {model_data_current.iloc[0]['name_model']}, "
                      f"brand_name: {brand_type_name_current}. "
                      f"Sus nuevos datos son: model_name {model_data.name_model}, "
                      f"brand_name: {brand_type_name_new}.")

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

    except Exception as e:
        print(f"Error en update_model_audit_log: {e}")
