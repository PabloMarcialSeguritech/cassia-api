from fastapi import status, HTTPException
from fastapi.responses import FileResponse
from infraestructure.cassia import cassia_hosts_repository
from infraestructure.zabbix import proxies_repository
from infraestructure.cassia import cassia_brand_repository
from infraestructure.cassia import cassia_host_models_repository
from infraestructure.cassia import cassia_host_tech_devices_repository
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
from ipaddress import IPv4Address


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


def check_existence(exist_ip: bool, exist_name: bool):
    if exist_ip and exist_name:
        return "La IP y el nombre del host ya existen."
    elif exist_ip:
        return "La IP del host ya esta ocupada."
    elif exist_name:
        return "El nombre del host ya existe."
    else:
        return "Tanto la IP como el nombre no existen."


async def update_hosts_data(hostid, host_new_data: cassia_hosts_schema.CassiaHostUpdateSchema, db: DB):
    # verificar informacion de schema
    is_valid_info = await validate_info_schema(host_new_data, db, True, hostid)
    # 1 Obtener host de la base de datos
    response = {'exception': None, 'result': '', 'success': False}
    host_current_data = await cassia_hosts_repository.get_cassia_hosts_by_ids([hostid], db)
    if host_current_data.empty:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"El host no existe")
    # 2 Actualizar el host data
    update_host_data_result = await update_host_data(hostid, host_current_data.iloc[0].to_dict(), host_new_data, db)
    if not update_host_data_result['success']:
        message = update_host_data_result['result']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al actualizar hoss {message}")
    response['result'] = update_host_data_result['result']
    # 3 Actualizar el inventory data
    host_inventory_current_data = await cassia_hosts_repository.get_cassia_host_inventory_data_by_id(hostid, db)
    update_host_inventory_result = await update_host_inventory(hostid, host_inventory_current_data[0].to_dict() if not host_inventory_current_data.empty else None, host_new_data, db)
    response['result'] += update_host_inventory_result['result']
    host_brand_model_current_data = await cassia_hosts_repository.get_cassia_brand_model(hostid, db)
    update_host_brand_model_result = await update_host_brand_model(hostid, host_brand_model_current_data[0].to_dict() if not host_brand_model_current_data.empty else None, host_new_data, db)
    response['result'] += update_host_brand_model_result['result']
    # 5 Actualizar interface data
    update_interface_data_result = await update_host_interface_data(hostid, host_new_data, db)
    # 6 Actualizar groups data

    return response


async def update_host_interface_data(hostid, host_new_data, db):
    response = {'result': None, 'success': True}
    # Verificar interface agente
    current_host_interfaces = await cassia_hosts_repository.get_cassia_host_interfaces()
    current_agent_host_interface = response
    pass


async def update_host_brand_model(hostid, host_brand_model_current_data, host_new_data, db):
    response = {'result': None, 'success': True}
    new_host_brand_model_data_fields = get_host_brand_model_data_fields(
        host_new_data)
    if host_brand_model_current_data is not None:  # Si existe la marca y modelo actualizarlos
        current_host_brand_model_data_fields = await get_host_brand_model_data_fields(host_brand_model_current_data)
        host_data_brand_model_sets_are_diferent = await verify_new_vs_current_host_data(current_host_brand_model_data_fields, new_host_brand_model_data_fields)
        if host_data_brand_model_sets_are_diferent:
            updated_brand_model_data = await cassia_hosts_repository.update_host_brand_model_data(hostid, new_host_brand_model_data_fields, host_new_data.alias, db)
            if updated_brand_model_data['success']:
                response['result'] = updated_brand_model_data['detail']
            else:
                response['result'] = updated_brand_model_data['detail']
                response['success'] = False

        else:
            response['result'] = "No fue necesario actualizar la marca y modelo del host. "

    else:  # Crear el registro
        assign_brand_model_result = await cassia_hosts_repository.assign_brand_model_affiliation_by_hostid(hostid, host_new_data, db)
        if not assign_brand_model_result['success']:
            response['detail'] += "No se logro asignar el modelo y marca."
        else:
            response['detail'] += "Se logro asignar el modelo y marca correctamente."
    return response


async def update_host_inventory(hostid, host_inventory_data, host_new_data, db):
    response = {'result': None, 'success': True}
    new_host_inventory_data_fields = get_host_data_inventory_data_fields(
        host_new_data)
    if host_inventory_data is not None:  # Si existe el inventory actualizar
        current_host_inventory_data_fields = await get_host_data_inventory_data_fields(host_inventory_data)

        host_data_inventory_sets_are_diferent = await verify_new_vs_current_host_data(current_host_inventory_data_fields, new_host_inventory_data_fields)
        if host_data_inventory_sets_are_diferent:  # Actualizar inventory porque son diferentes
            # {'success': False, 'detail': '', 'exception': False}
            updated_inventory_data = await cassia_hosts_repository.update_host_inventory_data(hostid, new_host_inventory_data_fields, db)
            if updated_inventory_data['success']:
                response['result'] = updated_inventory_data['detail']
            else:
                response['result'] = updated_inventory_data['detail']
                response['success'] = False
        else:  # No actualizar inventory porque son iguales
            response['result'] = "No fue necesario actualizar el inventory del host. "

    else:  # Si no existe entonces crearlo
        create_host_inventory_result = await create_host_inventory(hostid, new_host_inventory_data_fields)
        if create_host_inventory_result['success']:
            response['result'] = create_host_inventory_result['detail']
            # Actualizar device_id
            created_device_id = await cassia_hosts_repository.update_host_device_id_by_hostid(hostid, new_host_inventory_data_fields.device_id, db)
            if not created_device_id['success']:
                response['result'] += "No se logro asignar el device_id. "
            else:
                response['result'] += "Se asigno correctamente el device_id. "
        else:
            response['result'] = create_host_inventory_result['detail']
            response['success'] = False
    return response


async def create_host_inventory(hostid, host_inventory_data):
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        host_request_params = {
            'hostid': str(hostid),
            'inventory_mode': 0,
            'inventory': {
                'alias': host_inventory_data.alias,
                'location_lat': host_inventory_data.location_lat,
                'location_lon': host_inventory_data.location_lon,
                'serialno_a': host_inventory_data.serialno_a,
                'macaddress_a': host_inventory_data.macaddress_a
            },

        }
        zabbix_api = ZabbixApi()
        zabbix_request_result = await zabbix_api.do_request_new(method='host.update', params=host_request_params)
        if 'result' in zabbix_request_result:
            print(zabbix_request_result)
            response['success'] = True
            response['detail'] = "Host inventory creado correctamente. "
            return response
        else:
            response['detail'] = f"No se pudo crear el inventory: {zabbix_request_result['error']} "
            return response

    except Exception as e:
        response['success'] = False
        response['detail'] = e
        return response


async def update_host_data(hostid, host_current_data: dict, host_new_data: cassia_hosts_schema.CassiaHostUpdateSchema, db):
    response = {'result': None, 'success': True}

    # 1 Crear subsets de los dos datos
    current_host_data_fields = await get_host_data_fields(host_current_data)
    new_host_data_fields = await get_host_data_fields(host_new_data)
    # 2 Verificar si existen cambios a realizar
    host_data_sets_are_diferent = await verify_new_vs_current_host_data(current_host_data_fields, new_host_data_fields)
    # 3 Actualizar el host data de ser necesario
    if host_data_sets_are_diferent:
        # 4 Actualizar host data
        # {'success': False, 'detail': '','exception':False}
        updated_host_data = await cassia_hosts_repository.update_host_data(hostid, host_new_data, db)
        if not update_host_data['success']:
            response['result'] = update_host_data['detail']
            response['success'] = False
        else:
            response['result'] = "La informacion del host fue actualizada. "
    else:
        response['result'] = 'No fue necesario actualizar los campos de host. '
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


async def get_host_data_inventory_data_fields(host_dict):
    host_dict = SimpleNamespace(**host_dict)
    return {
        'device_id': host_dict.device_id,
        'alias': host_dict.alias,
        'location_lat': host_dict.location_lat,
        'location_lon': host_dict.location_lon,
        'serial_no_a': host_dict.serial_no_a,
        'macaddress_a': host_dict.macaddress_a
    }


async def get_host_brand_model_data_fields(host_dict):
    host_dict = SimpleNamespace(**host_dict)
    return {
        'model_id': host_dict.model_id,
        'brand_id': host_dict.brand_id
    }


async def verify_new_vs_current_host_data(current_host_data_fields, new_host_data_fields):
    # Comparamos los valores de los campos del host actual y los nuevos
    for field in current_host_data_fields:
        if current_host_data_fields[field] != new_host_data_fields[field]:
            return True  # Si hay algún campo diferente, retornamos True indicando que hay cambios
    return False  # Si todos los campos son iguales, retornamos False


async def create_host(host_new_data: cassia_hosts_schema.CassiaHostSchema, db: DB):
    response = {'success': True, 'detail': ''}
    # Validar info del schema
    is_valid_info = await validate_info_schema(host_new_data, db, False, None)
    # 2 Crear host con inventory, interface y grupos
    # {'success': False, 'detail': '', 'hostid': 0}
    host_was_created = await create_host_by_zabbix(host_new_data)
    if host_was_created['success']:
        print(host_was_created)
        # Obtener hostid creado
        hostid = host_was_created['hostid']
        response['detail'] = "Se creo el host en zabbix. "
        if host_new_data.device_id is not None:
            # 3 Actualizar device_id en el inventory
            # {'success': False, 'detail': '', 'exception': False}
            created_device_id = await cassia_hosts_repository.update_host_device_id_by_hostid(hostid, host_new_data.device_id, db)
            if not created_device_id['success']:
                response['detail'] += "No se logro asignar el device_id. "
            else:
                response['detail'] += "Se asigno correctamente el device_id. "
        else:
            response['detail'] += "No se asigno ningun device_id. "
        # 4 Crear registro de marca y modelo
        assign_brand_model_result = await cassia_hosts_repository.assign_brand_model_affiliation_by_hostid(hostid, host_new_data, db)
        if not assign_brand_model_result['success']:
            response['detail'] += "No se logro asignar el modelo y marca."
        else:
            response['detail'] += "Se logro asignar el modelo y marca correctamente."
        return success_response(message=f"Host creado con hostid {hostid}", data=response['detail'])
    else:
        detail = host_was_created['detail']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al crear el host en zabbix {detail}")


async def validate_info_schema(host_new_data: cassia_hosts_schema.CassiaHostSchema, db: DB, is_update: bool, hostid):
    # valida ip y puerto de agent ip
    if host_new_data.agent_ip != "":
        try:
            valid_ip = IPv4Address(host_new_data.agent_ip)

        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="La ip Agent no es valida")
        if host_new_data.agent_port == "":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El puerto ip Agent no es valido")
    # valida ip y puerto de snmp ip
    if host_new_data.snmp_ip != "":
        try:
            valid_ip = IPv4Address(host_new_data.snmp_ip)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="La ip SNMP no es valida")
        if host_new_data.snmp_port == "":
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El puerto ip SNMP no es valido")
    # 1 Verificar que el host name no exista
    tasks = {
        'exist_name': asyncio.create_task(proxies_repository.search_host_by_name(host_new_data.name, db))
    }
    # si la marca es proporcionada verificar que existe
    if host_new_data.brand_id is not None:
        tasks['exist_brand_id'] = asyncio.create_task(
            cassia_brand_repository.get_brand_by_id(host_new_data.brand_id, db))
    # si el modelo es proporcionada verificar que existe
    if host_new_data.model_id is not None:
        tasks['exist_model_id'] = asyncio.create_task(
            cassia_host_models_repository.get_cassia_host_model_by_id(host_new_data.model_id, db))
    # si el proxy es proporcionada verificar que existe
    if host_new_data.proxy_id is not None:
        tasks['exist_proxy_id'] = asyncio.create_task(
            proxies_repository.get_proxy_by_id(host_new_data.proxy_id, db))
    # si el device_id es proporcionada verificar que existe
    if host_new_data.device_id is not None:
        tasks['exist_device_id'] = asyncio.create_task(
            cassia_host_tech_devices_repository.get_host_tech_device_by_id(host_new_data.device_id, db))

    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    exist_host_name = dfs['exist_name']
    # Retornar excepciones correspondientes
    if not exist_host_name.empty:
        if is_update:
            if exist_host_name['hostid'].astype('int64')[0] != hostid:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                    detail="El nombre del host ya existe")
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El nombre del host ya existe")
    if host_new_data.brand_id is not None:
        exist_brand_id = dfs['exist_brand_id']
        if exist_brand_id.empty:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="La marca proporcionada no existe")
    if host_new_data.model_id is not None:
        exist_model_id = dfs['exist_model_id']
        if exist_model_id.empty:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El modelo proporcionado no existe")
    if host_new_data.proxy_id is not None:
        exist_proxy_id = dfs['exist_proxy_id']
        if exist_proxy_id.empty:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El proxy proporcionado no existe")
    if host_new_data.device_id is not None:
        exist_device_id = dfs['exist_device_id']
        if exist_device_id.empty:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El device_id proporcionado no existe")
    return True


async def create_host_by_zabbix(host_data: cassia_hosts_schema.CassiaHostSchema):
    response = {'success': False, 'detail': '', 'hostid': 0}
    try:
        groups = [{"groupid": host_group} for host_group in host_data.groupids]
        groups.append({"groupid": host_data.zona_groupid})
        if not len(groups):
            groups = [{
                "groupid": "192"
            }]

        host_request_params = {
            'host': host_data.name,
            'name': host_data.name,
            'description': host_data.description,
            'status': host_data.status,
            'inventory_mode': 0,
            'inventory': {
                'alias': host_data.alias,
                'location_lat': host_data.location_lat,
                'location_lon': host_data.location_lon,
                'serialno_a': host_data.serialno_a,
                'macaddress_a': host_data.macaddress_a
            },
            "groups": groups

        }
        if host_data.proxy_id is not None:
            host_request_params['proxy_hostid'] = host_data.proxy_id
        interfaces = []
        if host_data.agent_ip != "":
            interfaces.append({"type": 1,  # 1 = Agent, 2 = SNMP, 3 = IPMI, 4 = JMX
                               "main": 1,  # Es la interfaz principal
                               "useip": 1,  # Si utiliza IP en lugar de nombre DNS
                               "ip": host_data.agent_ip,  # IP del host
                               "dns": "",  # DNS vacío si useip=1
                               # Puerto de la interfaz (por defecto para Zabbix Agent)
                               "port": str(host_data.agent_port)
                               })
        if host_data.snmp_ip != "":
            interfaces.append({"type": 2,  # 1 = Agent, 2 = SNMP, 3 = IPMI, 4 = JMX
                               "main": 1,  # Es la interfaz principal
                               "useip": 1,  # Si utiliza IP en lugar de nombre DNS
                               "ip": host_data.snmp_ip,  # IP del host
                               "dns": "",  # DNS vacío si useip=1
                               # Puerto de la interfaz (por defecto para Zabbix Agent)
                               "port": str(host_data.snmp_port),
                               "details": {
                                   "version": host_data.snmp_version,  # SNMP version 2c
                                   # SNMP community string (for v1 or v2c)
                                   "community": host_data.snmp_community
                               }
                               })
        if len(interfaces):
            host_request_params['interfaces'] = interfaces
        print(host_request_params)
        zabbix_api = ZabbixApi()
        zabbix_request_result = await zabbix_api.do_request_new(method='host.create', params=host_request_params)
        if 'result' in zabbix_request_result:
            print(zabbix_request_result)
            response['success'] = True
            response['detail'] = zabbix_request_result
            response['hostid'] = zabbix_request_result['result']['hostids'][0]
            return response
        else:
            response['detail'] = zabbix_request_result['error']
            return response

    except Exception as e:
        response['success'] = False
        response['detail'] = e
        return response
