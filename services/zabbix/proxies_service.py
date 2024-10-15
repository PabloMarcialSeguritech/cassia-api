from fastapi import status, HTTPException
from utils.traits import success_response, timestamp_to_date_tz, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from infraestructure.zabbix import proxies_repository
from schemas import cassia_proxies_schema
from infraestructure.database import DB
import pandas as pd
import asyncio
from ipaddress import IPv4Address


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
        return "La IP y el nombre del host existen."
    elif exist_ip:
        return "La IP del host existe."
    elif exist_name:
        return "El nombre del host existe."
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

        params = {
            "host": proxy_data.name,  # Nombre del proxy
            # Tipo de proxy: 5 para activo, 6 para pasivo
            "status": 5 if proxy_data.mode == 'active' else 6,
            "interfaces": [
                {
                    "type": 1,  # Tipo de interfaz: 1 para agente Zabbix
                    "main": 1,  # Es la interfaz principal
                    "useip": 1,  # 1 para usar IP (no DNS)
                    "ip": proxy_data.ip,  # Dirección IP del proxy
                    "dns": "",  # Campo DNS vacío cuando se usa IP
                    "port": "10051"  # Puerto del proxy Zabbix
                }
            ],
            "description": proxy_data.description  # Descripción opcional
        }

        result = await zabbix_api.do_request(
            method="proxy.create", params=params)

        if result:
            proxyids = result['proxyids']
            if len(proxyids):
                return success_response()

        else:
            print(result)
            return success_response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, success=False, message='Error al crear proxy')

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
