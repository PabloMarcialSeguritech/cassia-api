from fastapi import status, HTTPException
from fastapi.responses import FileResponse
from infraestructure.cassia import cassia_hosts_repository
from infraestructure.cassia import cassia_group_types_repository
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export, get_df_by_filetype
from fastapi import File
from schemas import cassia_hosts_schema
from infraestructure.database import DB
import asyncio
import pandas as pd
import numpy as np
from types import SimpleNamespace


async def get_hosts(db: DB):
    hosts = await cassia_hosts_repository.get_cassia_hosts(db)
    if not hosts.empty:
        hosts['proxy_hostid'] = hosts['proxy_hostid'].replace(np.nan, 0)
        hosts['proxy_hostid'] = hosts['proxy_hostid'].astype('int64')
        hosts['proxy_hostid'] = hosts['proxy_hostid'].replace(0, None)

        hosts['brand_id'] = hosts['brand_id'].replace(np.nan, 0)
        hosts['brand_id'] = hosts['brand_id'].astype('int64')
        hosts['brand_id'] = hosts['brand_id'].replace(0, None)

        hosts['technology_id'] = hosts['technology_id'].replace(np.nan, 0)
        hosts['technology_id'] = hosts['technology_id'].astype('int64')
        hosts['technology_id'] = hosts['technology_id'].replace(0, None)

    return success_response(data=hosts.to_dict(orient="records"))


async def export_hosts_data(export_data: cassia_hosts_schema.CassiaHostExportSchema, db: DB):
    if len(export_data.hostids) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Selecciona al menos un host")
    hostids = ",".join([str(hostid) for hostid in export_data.hostids])
    hosts_data = await cassia_hosts_repository.get_cassia_hosts_by_ids(hostids, db)
    try:
        now = get_datetime_now_str_with_tz()
        export_file = await generate_file_export(hosts_data, page_name='hosts', filename=f"hosts - {now}", file_type=export_data.file_type)
        return export_file
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en export_groups_data {e}")


async def update_hosts_data(hostid, host_new_data: cassia_hosts_schema.CassiaHostUpdateSchema, db: DB):
    # 1 Obtener host de la base de datos
    host_current_data = await cassia_hosts_repository.get_cassia_hosts_by_ids([hostid], db)
    if host_current_data.empty:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"El host no existe")
    # 2 Actualizar el host data
    update_host_data_result = await update_host_data(hostid, host_current_data.iloc[0].to_dict(), host_new_data, db)
    # 3 Actualizar el inventory data
    # 4 Actualizar la marca y modelo
    # 5 Actualizar interface data
    # 5 Actualizar groups data
    pass


async def update_host_data(hostid, host_current_data: dict, host_new_data: cassia_hosts_schema.CassiaHostUpdateSchema, db):
    response = {'exception': None, 'result': None, 'success': True}
    # 1 Crear subsets de los dos datos
    current_host_data_fields = await get_host_data_fields(host_current_data)
    new_host_data_fields = await get_host_data_fields(host_new_data)
    # 2 Verificar si existen cambios a realizar
    host_data_sets_are_diferent = await verify_new_vs_current_host_data(current_host_data_fields, new_host_data_fields)
    # 3 Actualizar el host data de ser necesario
    if host_data_sets_are_diferent:
        # 4 Actualizar host data
        # {'success': False, 'detail': ''}
        updated_host_data = await cassia_hosts_repository.update_host_data(hostid, host_new_data, db)
        response['success'] = update_host_data['success']
        response['result'] = update_host_data['detail']
        return response
    else:
        response['result'] = 'No fue necesario actualizar los campos de host.'
        return response


async def get_host_data_fields(host_dict):
    host_dict = SimpleNamespace(**host_dict)
    return {
        'host': host_dict.host,
        'name': host_dict.name,
        'proxy_id': host_dict.proxy_id,
        'description': host_dict.description,
        'status': host_dict.status
    }


async def verify_new_vs_current_host_data(current_host_data_fields, new_host_data_fields):
    # Comparamos los valores de los campos del host actual y los nuevos
    for field in current_host_data_fields:
        if current_host_data_fields[field] != new_host_data_fields[field]:
            return True  # Si hay algún campo diferente, retornamos True indicando que hay cambios
    return False  # Si todos los campos son iguales, retornamos False
