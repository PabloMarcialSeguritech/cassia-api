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
import numpy as np

from infraestructure.database import DB


async def get_proxies(db):
    proxies = await proxies_repository.get_proxies(db)
    if proxies.empty:
        return success_response(data=[])
    proxies['proxy_id'] = proxies['proxy_id'].astype('int64')
    if not proxies.empty:
        proxies['id'] = proxies['proxy_id']
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
    processed_data = await get_df_by_filetype(file_import, ['proxy_id', 'name', 'ip', 'proxy_mode', 'description'])
    result = processed_data['result']
    if not result:
        exception = processed_data['exception']
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
    if not df_import_results.empty:
        df_import_results = df_import_results.replace(np.nan, None)
    return success_response(data=df_import_results.to_dict(orient='records'))
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


async def delete_proxy(proxyid: int, db: DB):
    # Obtener proxy por su ID desde la base de datos
    proxy = await proxies_repository.get_proxy_by_id(proxyid, db)

    if proxy.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El proxy no existe")

    # Asegúrate de obtener el valor único (primera fila)
    # Obtener el primer valor de la columna 'proxy_id'
    proxy_id = proxy['proxy_id'].iloc[0]

    # Verificar que proxy_id no sea nulo
    if pd.isna(proxy_id):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El proxy ID es inválido o nulo")

    try:
        # Convertir el ID del proxy a entero
        proxy_id = int(proxy_id)  # Convertir explícitamente a entero
        # Verificación
        print(
            f"proxy_id convertido a entero: {proxy_id}, tipo: {type(proxy_id)}")

        # Crear instancia de la API de Zabbix
        zabbix_api = ZabbixApi()

        # Parámetros para eliminar el proxy (enviando como lista)
        params = {"proxyids": proxy_id}
        print(f"Parámetros enviados a Zabbix: {params}")

        # Hacer la solicitud a la API de Zabbix
        result = await zabbix_api.do_request_new(method="proxy.delete", params=params)
        print(result)
        # Verificar si la respuesta tiene el campo 'result'
        if 'error' in result:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail="No se pudo eliminar el proxy en Zabbix")
        elif 'result' in result:
            # Accede a los proxyids eliminados
            deleted_proxies = result['result']['proxyids']
            print(f"Proxies eliminados: {deleted_proxies}")

            if deleted_proxies:
                # El ID del proxy eliminado
                deleted_proxy_id = deleted_proxies[0]
                return success_response(message=f"Proxy {deleted_proxy_id} eliminado con éxito")
                """ return {"message": f"Proxy {deleted_proxy_id} eliminado con éxito"} """
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="No se pudo eliminar el proxy en Zabbix")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al eliminar el proxy: {str(e)}")


async def update_proxy(proxyid: int, proxy_data: cassia_proxies_schema.CassiaProxiesSchema, db):
    # Verifica que exista el proxy
    exist_proxy = await proxies_repository.get_proxy_by_id(proxyid, db)

    if exist_proxy.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El proxy con el id proporcionado no existe")
    try:  # Verificacion de IP valida
        valid_ip = IPv4Address(proxy_data.ip)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="La IP no es una direccion valida")
    # Obtencion de ip y nombre de bd
    tasks = {
        'exist_ip': asyncio.create_task(proxies_repository.search_interface_by_ip(proxy_data.ip, db)),
        'exist_name': asyncio.create_task(proxies_repository.search_host_by_name(proxy_data.name, db))
    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    exist_ip = dfs['exist_ip']
    exist_name = dfs['exist_name']

    # Verificacion de que no exista otro host con la misma ip y nombre
    if not exist_ip.empty:
        if exist_ip['hostid'].astype('int64')[0] != proxyid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="La ip ya esta asignada a otro host")
    if not exist_name.empty:
        if exist_name['hostid'].astype('int64')[0] != proxyid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El nombre ya esta asignado a otro host")

    try:

        zabbix_api = ZabbixApi()
        status_proxy = "5" if proxy_data.proxy_mode == "Active" else "6"
        # Parametros para actualizar proxy en zabbix
        params_proxy = {
            "host": proxy_data.name,
            "status": status_proxy,
            "description": proxy_data.description,
            "proxyid": str(proxyid)
        }
        # Peticion de actualizacion a api de zabbix
        result = await zabbix_api.do_request_new(
            method="proxy.update", params=params_proxy)
        # Verifica si se realizo correctamente la peticion
        if 'result' in result:
            # Obtiene la interface del host
            proxy_interface = await proxies_repository.search_interface_by_hostid(proxyid, db)

            if not proxy_interface.empty:  # Si tiene una interfaz activa actualiza esa interfaz
                interfaceid = proxy_interface['interfaceid'].astype(str)[
                    0]
                params_proxy_interface = {
                    "interfaceid": interfaceid,
                    "ip": proxy_data.ip,  # La IP que deseas asignar al proxy
                    "useip": 1,  # Usar la IP en lugar de DNS
                    "dns": "",  # Dejar DNS vacío si se usa la IP
                    "port": "10051",
                }

                result_update_ip = await zabbix_api.do_request_new("hostinterface.update", params_proxy_interface)

                if 'result' in result_update_ip:
                    return success_response(message="Proxy actualizado correctamente")
                else:
                    return success_response(message="La informacion del proxy se actulizo correctamente, pero la IP no se logro actualizar.")

            else:  # Si no tiene una interfaz activa le crea una interfaz
                params_proxy_interface = {
                    "ip": proxy_data.ip,  # La IP que deseas asignar al proxy
                    "useip": 1,  # Usar la IP en lugar de DNS
                    "dns": "",  # Dejar DNS vacío si se usa la IP
                    "port": "10051",
                    "main": 1,
                    "type": 1,  # Puerto por defecto del proxy de Zabbix
                }
                params_proxy_interface['hostid'] = str(proxyid)

                result_assign_ip = await zabbix_api.do_request_new("hostinterface.create", params_proxy_interface)
                if 'result' in result_assign_ip:
                    return success_response(message="Proxy actualizado correctamente")
                else:
                    return success_response(message="La informacion del proxy se actulizo correctamente, pero la IP no se logro actualizar.")
        else:
            error = result['error']
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Error al crear proxy en zabbix: {error}")

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en create_proxies: {e}")
