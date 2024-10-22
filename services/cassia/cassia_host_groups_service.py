from fastapi import status, HTTPException
from fastapi.responses import FileResponse
from numpy.matlib import empty

from infraestructure.cassia import cassia_host_groups_repository
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from schemas.cassia_audit_schema import CassiaAuditSchema
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export, get_df_by_filetype
from fastapi import File
from schemas import cassia_host_groups_schema
from infraestructure.database import DB
import asyncio
import pandas as pd
import numpy as np
from utils.actions_modules_enum import AuditAction, AuditModule
from models.cassia_user_session import CassiaUserSession
import services.cassia.users_service as users_service
import json
from infraestructure.cassia import cassia_user_repository, cassia_group_types_repository
from services.cassia import cassia_audit_service



async def get_host_groups(db: DB):
    host_groups = await cassia_host_groups_repository.get_cassia_host_groups(db)
    return success_response(data=host_groups.to_dict(orient="records"))


async def create_host_group(db: DB, group_data: cassia_host_groups_schema.CassiaHostGroupSchema, current_user: CassiaUserSession):
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
        if 'error' in zabbix_result:
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
            await create_group_audit_log(group_data, current_user, db)
            return success_response(message='Grupo creado correctamente')
        else:
            return success_response(success=False, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, message="Error al asignar el grupo")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en crate_host_group: {e}")


async def delete_host_group(groupid: int, current_user: CassiaUserSession, db: DB):
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
            await delete_group_audit_log(group, current_user, db)
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
    processed_data = await get_df_by_filetype(file_import, ['groupid', 'group_name', 'group_type_id'])
    result = processed_data['result']
    if not result:
        exception = processed_data['exception']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al procesar el archivo: {exception}")
    df_import = processed_data['df']
    df_import = df_import.rename(
        columns={'group_name': 'name', 'group_type_id': 'type_id'})
    tasks_create = [asyncio.create_task(crate_host_group_by_import(
        db, df_import.iloc[group_data_ind].to_dict())) for group_data_ind in df_import.index]
    df_import_results = pd.DataFrame(
        columns=['groupid', 'name', 'type_id', 'result', 'detail', 'groupid_creado'])
    for i in range(0, len(tasks_create), 10):
        lote = tasks_create[i:i + 10]
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
        df_import_results = df_import_results.replace(np.nan, None)
    return success_response(data=df_import_results.to_dict(orient='records'))


async def crate_host_group_by_import(db: DB, group_data):
    response = group_data
    response['result'] = 'No se creo correctamente el registro'
    response['detail'] = ''
    response['groupid_creado'] = ''
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

        if 'error' in zabbix_result:
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
            response['groupid_creado'] = groupid
        else:
            response['result'] = 'Se creo el registro'
            response['detail'] = "El grupo se creo correctamente pero el tipo no se pudo asignar "
            response['groupid_creado'] = groupid

        return response
    except Exception as e:
        response['detail'] = f"Error al crear el regitro {e} "
        return response


async def update_host_group(group_data, current_user: CassiaUserSession, db):
    group_dict = group_data.dict()
    hostgroup_id = group_dict['groupid']
    hostgroup_name = group_dict['name']
    hostgroup_type_id = group_dict['type_id']

    # Obtener el hostgroup existente
    hostgroup = await cassia_host_groups_repository.get_relation_cassia_host_groups_types_by_group_id(hostgroup_id, db)

    # Verificar si el hostgroup existe
    if not hostgroup.empty:
        # Comprobar si existe un tipo de grupo asociado
        if hostgroup['cassia_group_type_id'].isnull().all():
            is_type_assigned = await cassia_host_groups_repository.asignar_group_type_cassia_groupid(db, hostgroup_id, group_data.type_id)
            if not is_type_assigned:
                return success_response(success=False, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                        message="Error al asignar el grupo")
        # Realizar la actualización
        is_correct = await cassia_host_groups_repository.update_host_group_name_and_type_id(
            hostgroup_id, hostgroup_name, hostgroup_type_id, db)

        if is_correct:
            await update_group_audit_log(group_data, hostgroup, current_user, db)
            return success_response(
                message="HostGroup actualizado correctamente")
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al actualizar el HostGroup")

    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No existe el host group con ID {hostgroup_id}."
        )

async def create_group_audit_log(group_data: cassia_host_groups_schema.CassiaHostGroupSchema, current_user: CassiaUserSession, db: DB):

    try:
        module_id = AuditModule.GROUPS.value
        action_id = AuditAction.CREATE.value

        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        group_type_df = await cassia_group_types_repository.get_cassia_group_type_by_id(db, group_data.type_id)
        group_type_name = ""

        if not group_type_df.empty:
            group_type_name = group_type_df.iloc[0]['name']

        detail = f"Se creo un grupo con los siguientes datos, group_name: {group_data.name}, group_type: {group_type_name}"

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


async def delete_group_audit_log(group ,
                                 current_user: CassiaUserSession, db: DB):
    try:
        module_id = AuditModule.GROUPS.value
        action_id = AuditAction.DELETE.value
        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)
        group_name = ""
        group_id = None

        if not group.empty:
            group_name = group.iloc[0]['name']
            group_id = group.iloc[0]['groupid']


        detail = f"Se elimino el host group con los siguientes datos, group_id: {group_id}, group_name: {group_name}"

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

async def update_group_audit_log(group_data_new: cassia_host_groups_schema.CassiaHostGroupSchema,
                                 group_data_current,
                                 current_user: CassiaUserSession,
                                 db: DB):
    try:
        module_id = AuditModule.GROUPS.value
        action_id = AuditAction.UPDATE.value

        # Obtener el usuario actual
        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        # Obtener los tipos de grupos desde la base de datos en formato DataFrame
        groups_types_df = await cassia_group_types_repository.get_cassia_group_types(db)

        if not groups_types_df.empty:

            # Acceder correctamente a los campos de tipo de grupo usando 'id' en lugar de 'group_type_id'
            current_group_type_id = group_data_current.iloc[0]['cassia_group_type_id']  # Desde el objeto actual
            new_group_type_id = group_data_new.type_id  # Desde el nuevo objeto (ajustado a type_id)

            # Buscar los nombres de los tipos de grupo correspondientes a los IDs en los datos actuales y nuevos
            group_type_name_current = groups_types_df.loc[groups_types_df['id'] == current_group_type_id, 'name'].values
            group_type_name_new = groups_types_df.loc[groups_types_df['id'] == new_group_type_id, 'name'].values

            # Verificar que se encontró un nombre de tipo de grupo
            if group_type_name_current.size == 0:
                raise ValueError(f"No se encontró el nombre del tipo de grupo para el ID {current_group_type_id}")
            if group_type_name_new.size == 0:
                raise ValueError(f"No se encontró el nombre del tipo de grupo para el ID {new_group_type_id}")

            # Extraer el valor del array resultante
            group_type_name_current = group_type_name_current[0]
            group_type_name_new = group_type_name_new[0]

            # Crear el detalle de la auditoría con los nombres de los tipos de grupo
            detail = (f"Se actualizó un grupo. Sus datos anteriores eran: group_name {group_data_current.iloc[0]['name']}, "
                      f"group_type: {group_type_name_current}. "
                      f"Sus nuevos datos son: group_name {group_data_new.name}, "
                      f"group_type: {group_type_name_new}.")

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
        print(f"Error en update_group_audit_log: {e}")



