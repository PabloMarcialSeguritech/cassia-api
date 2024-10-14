from fastapi import status, HTTPException
from fastapi.responses import FileResponse
from infraestructure.cassia import cassia_host_groups_repository
from infraestructure.cassia import cassia_group_types_repository
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export, get_df_by_filetype
from fastapi import File
from schemas import cassia_host_groups_schema
from infraestructure.database import DB
import asyncio
import pandas as pd


async def get_host_groups(db: DB):
    host_groups = await cassia_host_groups_repository.get_cassia_host_groups(db)
    return success_response(data=host_groups.to_dict(orient="records"))


async def crate_host_group(db: DB, group_data: cassia_host_groups_schema.CassiaHostGroupSchema):
    host_group_name_exist = await cassia_host_groups_repository.get_cassia_host_group_by_name(
        group_data.name, db)
    if not host_group_name_exist.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El nombre del grupo ya existe")
    host_group_type = await cassia_group_types_repository.get_cassia_group_type_by_id(db, group_data.type_id)
    if host_group_type.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Tipo de grupo no existente")
    try:
        # PASO 1: CREACION DE GRUPO NATIVO DE ZABBIX MEDIANTE API POR HTTP
        zabbix_api = ZabbixApi()

        params = {
            'name': group_data.name
        }
        zabbix_result = await zabbix_api.do_request_new(
            method="hostgroup.create", params=params)
        if not zabbix_result['success']:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Error al crear grupo en Zabbix")
        zabbix_result = zabbix_result['result']
        groupids = zabbix_result['groupids']
        # PASO 2: ASIGNACION DE TIPADO CASSIA A GRUPO NATIVO CREADO
        if len(groupids) < 1:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Error al crear grupo en Zabbix")
        groupid = groupids[0]
        is_type_assigned = await cassia_host_groups_repository.asignar_group_type_cassia_groupid(db, groupid, group_data.type_id)
        if is_type_assigned:
            return success_response(message='Grupo creado correctamente')
        else:
            return success_response(success=False, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, message="Error al asignar el grupo")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en crate_host_group: {e}")


async def delete_host_group(groupid: int, db: DB):
    group = await cassia_host_groups_repository.get_host_group_by_groupid(groupid, db)
    if group.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El host group no existe")
    groupid_relations = await cassia_host_groups_repository.get_groupid_relations_by_groupid(db, groupid)
    if not groupid_relations.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El grupo tiene host asignados por lo que no se puede eliminar")
    try:
        zabbix_api = ZabbixApi()

        params = [groupid]

        result = await zabbix_api.do_request(
            method="hostgroup.delete", params=params)
        if result:
            return success_response(message='Grupo eliminado correctamente')
        else:
            return success_response(success=False, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, message="Error al eliminar el grupo")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en delete_host_group: {e}")


async def export_groups_data(export_data: cassia_host_groups_schema.CassiaHostGroupExportSchema, db: DB):
    if len(export_data.groupids) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Selecciona al menos un grupo")
    groupids = ",".join([str(groupid) for groupid in export_data.groupids])
    groups_data = await cassia_host_groups_repository.get_cassia_host_groups_by_ids(groupids, db)
    try:
        now = get_datetime_now_str_with_tz()
        export_file = await generate_file_export(groups_data, page_name='grupos', filename=f"groups - {now}", file_type=export_data.file_type)
        return export_file
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en export_groups_data {e}")


async def import_groups_data(file_import: File, db):
    file_types = ('.csv', '.xlsx', '.xls', '.json')
    if not file_import.filename.endswith(file_types):
        raise HTTPException(
            status_code=400,
            detail="El archivo debe ser un CSV, JSON, XLS o XLSX"
        )
    processed_data = await get_df_by_filetype(file_import)
    result = processed_data['result']
    if not result:
        exception = result['exception']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al procesar el archivo: {exception}")
    df_import = processed_data['df']
    df_import = df_import.rename(
        columns={'group_name': 'name', 'group_type_id': 'type_id'})
    tasks_create = [asyncio.create_task(crate_host_group_by_import(
        db, df_import.iloc[group_data_ind].to_dict())) for group_data_ind in df_import.index]
    df_import_results = pd.DataFrame(
        columns=['groupid', 'name', 'type_id', 'result', 'detail'])
    for i in range(0, len(tasks_create), 5):
        lote = tasks_create[i:i + 5]
        # Ejecutar las corutinas de forma concurrente
        resultados = await asyncio.gather(*lote)
        for resultado in resultados:
            print(resultado)
            new_row = pd.DataFrame(resultado, index=[0])
            # Concatenar el nuevo registro al DataFrame original
            df_import_results = pd.concat(
                [df_import_results, new_row], ignore_index=True)
    if not df_import_results.empty:
        df_import_results['type_id'] = df_import_results['type_id'].astype(
            'int64')
    return await generate_file_export(data=df_import_results, page_name='Resultados', filename='Resultados importación', file_type='excel')


async def crate_host_group_by_import(db: DB, group_data):
    response = group_data
    response['result'] = 'No se creo correctamente el registro'
    response['detail'] = ''
    host_group_name_exist = await cassia_host_groups_repository.get_cassia_host_group_by_name(
        group_data['name'], db)
    if not host_group_name_exist.empty:
        response['detail'] = 'El nombre del grupo ya existe'
        return response

    host_group_type = await cassia_group_types_repository.get_cassia_group_type_by_id(db, group_data['type_id'])

    if host_group_type.empty:
        response['detail'] = "Tipo de grupo no existente"
        return response

    try:
        # PASO 1: CREACION DE GRUPO NATIVO DE ZABBIX MEDIANTE API POR HTTP

        zabbix_api = ZabbixApi()

        params = {
            'name': group_data['name']
        }
        zabbix_result = await zabbix_api.do_request_new(
            method="hostgroup.create", params=params)

        if not zabbix_result['success']:
            response['detail'] = f"Error al crear grupo en Zabbix"
            return response
        zabbix_result = zabbix_result['result']
        groupids = zabbix_result['groupids']
        # PASO 2: ASIGNACION DE TIPADO CASSIA A GRUPO NATIVO CREADO
        if len(groupids) < 1:
            response['detail'] = "Error al crear grupo en Zabbix"

            return response
        groupid = groupids[0]

        is_type_assigned = await cassia_host_groups_repository.asignar_group_type_cassia_groupid(db, groupid, group_data['type_id'])

        if is_type_assigned:
            response['result'] = 'Se creo el registro'
        else:
            response['result'] = 'Se creo el registro'
            response['detail'] = "El grupo se creo correctamente pero el tipo no se pudo asignar "

        return response
    except Exception as e:
        response['detail'] = f"Error al crear el regitro {e} "
        return response
