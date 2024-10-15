from fastapi import status, HTTPException
from utils.traits import success_response, timestamp_to_date_tz, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export, get_df_by_filetype
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from infraestructure.zabbix import proxies_repository
from schemas import cassia_proxies_schema
from infraestructure.database import DB
import pandas as pd
import asyncio
from fastapi import File
from ipaddress import IPv4Address
from types import SimpleNamespace


async def get_proxies(db):
    proxies = await proxies_repository.get_proxies(db)
    if proxies.empty:
        return success_response(data=[])
    proxies['proxy_id'] = proxies['proxy_id'].astype('int64')
    try:
        zabbix_api = ZabbixApi()

        params = {"output": "extend"}

        result = await zabbix_api.do_request(
            method="proxy.get", params=params)
        if result:
            result_response = pd.DataFrame(result)
            if not result_response.empty:
                result_response = result_response[['proxyid', 'lastaccess']]
                result_response['proxyid'] = result_response['proxyid'].astype(
                    'int64')
                response = pd.merge(
                    proxies, result_response, left_on='proxy_id', right_on='proxyid', how='left')
                if 'proxyid' in response.columns:
                    response.drop(columns=['proxyid'])
                response['lastaccess'] = response['lastaccess'].astype('int64').apply(
                    timestamp_to_date_tz)
                return success_response(data=response.to_dict(orient='records'))
        else:
            proxies['lastaccess'] = None
            return success_response(proxies.to_dict(orient='records'))

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_proxies: {e}")


def check_existence(exist_ip: bool, exist_name: bool):
    if exist_ip and exist_name:
        return "La IP y el nombre del host ya existen."
    elif exist_ip:
        return "La IP del host ya existe."
    elif exist_name:
        return "El nombre del host ya existe."
    else:
        return "Tanto la IP como el nombre no existen."


async def create_proxy(proxy_data: cassia_proxies_schema.CassiaProxiesSchema, db):
    try:
        valid_ip = IPv4Address(proxy_data.ip)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="La IP no es una direccion valida")

    tasks = {
        'exist_ip': asyncio.create_task(proxies_repository.search_interface_by_ip(proxy_data.ip, db)),
        'exist_name': asyncio.create_task(proxies_repository.search_host_by_name(proxy_data.name, db))
    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    exist_ip = not dfs['exist_ip'].empty
    exist_name = not dfs['exist_name'].empty
    if exist_ip or exist_name:
        message_error = check_existence(exist_ip, exist_name)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=message_error)
    try:
        zabbix_api = ZabbixApi()
        status_proxy = "5" if proxy_data.proxy_mode == "Active" else "6"
        params_proxy = {
            "host": proxy_data.name,
            "status": status_proxy,
            "description": proxy_data.description,
        }

        result = await zabbix_api.do_request_new(
            method="proxy.create", params=params_proxy)
        if 'result' in result:
            proxy_id = result['result']['proxyids'][0]
            params_proxy_interface = {
                "hostid": proxy_id,  # ID del proxy recién creado
                "ip": proxy_data.ip,  # La IP que deseas asignar al proxy
                "useip": 1,  # Usar la IP en lugar de DNS
                "dns": "",  # Dejar DNS vacío si se usa la IP
                "port": "10051",
                "main": 1,
                "type": 1,  # Puerto por defecto del proxy de Zabbix
            }

            result_assign_ip = await zabbix_api.do_request_new("hostinterface.create", params_proxy_interface)
            print(result_assign_ip)
            if 'result' in result_assign_ip:
                return success_response(message="Proxy creado correctamente")
            else:
                error = result['error']
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail=f"Error al crear proxy en zabbix: {error}")
        else:
            error = proxy_id = result['error']
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Error al crear proxy en zabbix: {error}")

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en create_proxies: {e}")


async def export_proxies(proxy_data_export: cassia_proxies_schema.CassiaProxiesExportSchema, db: DB):
    if len(proxy_data_export.proxy_ids) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Selecciona al menos un proxy")
    proxy_ids = ",".join([str(proxy_id)
                         for proxy_id in proxy_data_export.proxy_ids])
    proxies_data = await proxies_repository.get_proxies_by_ids(proxy_ids, db)

    try:
        now = get_datetime_now_str_with_tz()
        export_file = await generate_file_export(proxies_data, page_name='proxies', filename=f"proxies - {now}", file_type=proxy_data_export.file_type)
        return export_file
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en export_proxies {e}")


async def import_proxies(file_import: File, db):
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

    tasks_create = [asyncio.create_task(create_proxy_by_import_data(
        df_import.iloc[proxy_data_ind].to_dict(), db)) for proxy_data_ind in df_import.index]

    df_import_results = pd.DataFrame(
        columns=['proxy_id', 'name', 'ip', 'proxy_mode', 'description', 'result', 'detail', 'proxyid_creado'])
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
    now = get_datetime_now_str_with_tz()
    return await generate_file_export(data=df_import_results, page_name='Resultados', filename=f'Resultados importación proxies {now}', file_type='excel')


async def create_proxy_by_import_data(proxy_data, db):
    response = proxy_data
    response['result'] = 'No se creo correctamente el registro'
    response['detail'] = ''
    response['proxyid_creado'] = ''
    proxy_data = SimpleNamespace(**proxy_data)
    try:
        valid_ip = IPv4Address(proxy_data.ip)
    except Exception as e:
        response['detail'] = 'La IP no es una direccion valida'
        return response

    tasks = {
        'exist_ip': asyncio.create_task(proxies_repository.search_interface_by_ip(proxy_data.ip, db)),
        'exist_name': asyncio.create_task(proxies_repository.search_host_by_name(proxy_data.name, db))
    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    exist_ip = not dfs['exist_ip'].empty
    exist_name = not dfs['exist_name'].empty
    if exist_ip or exist_name:
        message_error = check_existence(exist_ip, exist_name)
        response['detail'] = message_error
        return response
    try:
        zabbix_api = ZabbixApi()
        status_proxy = "5" if proxy_data.proxy_mode == "Active" else "6"
        params_proxy = {
            "host": proxy_data.name,
            "status": status_proxy,
            "description": proxy_data.description,
        }

        result = await zabbix_api.do_request_new(
            method="proxy.create", params=params_proxy)
        if 'result' in result:
            proxy_id = result['result']['proxyids'][0]
            params_proxy_interface = {
                "hostid": proxy_id,  # ID del proxy recién creado
                "ip": proxy_data.ip,  # La IP que deseas asignar al proxy
                "useip": 1,  # Usar la IP en lugar de DNS
                "dns": "",  # Dejar DNS vacío si se usa la IP
                "port": "10051",
                "main": 1,
                "type": 1,  # Puerto por defecto del proxy de Zabbix
            }

            result_assign_ip = await zabbix_api.do_request_new("hostinterface.create", params_proxy_interface)
            print(result_assign_ip)
            if 'result' in result_assign_ip:
                response['result'] = 'Proxy creado correctamente'
                response['proxyid_creado'] = proxy_id
                return response
            else:
                error = result['error']
                response[
                    'result'] = 'El proxy fue creado pero no se pudo asignar la interfaz(ip)'
                response['detail'] = f"Error al asignar la ip del proxy en zabbix: {error}"
                response['proxyid_creado'] = proxy_id
                return response

        else:
            error = result['error']
            response['detail'] = f"Error al crear proxy en zabbix: {error}"
            return response

    except Exception as e:
        response['detail'] = f"Error al crear proxy : {e}"
        return response
