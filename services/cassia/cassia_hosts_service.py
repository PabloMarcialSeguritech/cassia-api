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
import time


async def get_host(hostid, db: DB):
    host = await cassia_hosts_repository.get_cassia_host(hostid, db)
    if not host.empty:
        host['proxy_hostid'] = host['proxy_hostid'].replace(np.nan, 0)
        host['proxy_hostid'] = host['proxy_hostid'].astype('int64')
        host['proxy_hostid'] = host['proxy_hostid'].replace(0, None)

        host['brand_id'] = host['brand_id'].replace(np.nan, 0)
        host['brand_id'] = host['brand_id'].astype('int64')
        host['brand_id'] = host['brand_id'].replace(0, None)

        host['technology_id'] = host['technology_id'].replace(np.nan, 0)
        host['technology_id'] = host['technology_id'].astype('int64')
        host['technology_id'] = host['technology_id'].replace(0, None)
    response = {}
    if not host.empty:
        response = host.to_dict(orient="records")[0]
    return success_response(data=response)


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
    host_current_data = await cassia_hosts_repository.get_cassia_hosts_by_ids(hostid, db)
    if host_current_data.empty:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"El host no existe")
    # 2 Actualizar el host data
    update_host_data_result = await update_host_data(hostid, host_current_data.iloc[0].to_dict(), host_new_data, db)
    if not update_host_data_result['success']:
        message = update_host_data_result['result']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al actualizar host {message}")
    response['result'] = update_host_data_result['result']
    # 3 Actualizar el inventory data
    host_inventory_current_data = await cassia_hosts_repository.get_cassia_host_inventory_data_by_id(hostid, db)
    update_host_inventory_result = await update_host_inventory(hostid, host_inventory_current_data.iloc[0].to_dict() if not host_inventory_current_data.empty else None, host_new_data, db)
    response['result'] += update_host_inventory_result['result']
    host_brand_model_current_data = await cassia_hosts_repository.get_cassia_brand_model(hostid, db)
    update_host_brand_model_result = await update_host_brand_model(hostid, host_brand_model_current_data.iloc[0].to_dict() if not host_brand_model_current_data.empty else None, host_new_data, db)
    response['result'] += update_host_brand_model_result['result']
    # 5 Actualizar interface data
    update_interface_data_result = await update_host_interface_data(hostid, host_new_data, db)
    response['result'] += update_interface_data_result['result']
    # 6 Actualizar groups data
    return success_response(message="Host actualizado correctamente", data=response['result'])


async def update_host_interface_data(hostid, host_new_data, db):
    response = {'result': '', 'success': True}
    # Verificar interface agente
    current_host_interfaces = await cassia_hosts_repository.get_cassia_host_interfaces_by_hostid(hostid, db)

    current_agent_host_interface = current_host_interfaces.loc[current_host_interfaces['type'].astype(
        'str') == '1']
    current_snmp_host_interface = current_host_interfaces.loc[current_host_interfaces['type'].astype(
        'str') == '2']

    if current_agent_host_interface.empty:
        if host_new_data.agent_ip != "":
            # crear una interfaz
            # response = {'success': False, 'detail': '', 'exception': False}
            create_agent_interface_result = await create_interface(hostid, 1, host_new_data.agent_ip, host_new_data.agent_port, None, None)
            response['result'] += create_agent_interface_result['detail']

        else:  # No se hace nada
            response['result'] += 'No fue necesario realizar ninguna accion en la interface agent.'
    else:
        current_agent_host_interface_fields = current_agent_host_interface.iloc[0].to_dict(
        )
        print(current_agent_host_interface_fields)
        if host_new_data.agent_ip == "":  # Eliminar interfaz agent
            print("SE ELIMINARA")
            delete_interface_host_result = await delete_interface_host(current_agent_host_interface_fields['interfaceid'], 1)
            response['result'] += delete_interface_host_result['detail']
        else:  # Actualizar interfaz agent
            # Son nuevos datos y hay que actualizar
            # response = {'success': False, 'detail': '', 'exception': False}
            print("SE ACTULIZARA LA INTERFACE")
            if current_agent_host_interface_fields['ip'] != host_new_data.agent_ip or current_agent_host_interface_fields['port'] != host_new_data.agent_port:
                update_interface_agent_result = await update_host_interface_zabbix(current_agent_host_interface.iloc[0]['interfaceid'], 1, host_new_data.agent_ip, host_new_data.agent_port, None, None)
                response['result'] += update_interface_agent_result['detail']

            else:  # No es necesario actualizar la interfaz
                response['result'] += "No fue necesario actualizar la interface agent del host."
    if current_snmp_host_interface.empty:
        if host_new_data.snmp_ip != "":
            # crear una interfaz
            # response = {'success': False, 'detail': '', 'exception': False}
            create_snmp_interface_result = await create_interface(hostid, 2, host_new_data.snmp_ip, host_new_data.snmp_port, host_new_data.snmp_version, host_new_data.snmp_community)
            response['result'] += create_snmp_interface_result['detail']

        else:  # No se hace nada
            response['result'] += 'No fue necesario realizar ninguna accion en la interface snmp.'
    else:
        current_snmp_host_interface_fields = current_snmp_host_interface.iloc[0].to_dict(
        )
        if host_new_data.snmp_ip == "":  # Eliminar interfaz agent
            delete_interface_host_result = await delete_interface_host(current_snmp_host_interface_fields['interfaceid'], 2)
            response['result'] += delete_interface_host_result['detail']
        else:  # Actualizar interfaz agent
            # Son nuevos datos y hay que actualizar
            print(current_snmp_host_interface_fields)
            if current_snmp_host_interface_fields['ip'] != host_new_data.snmp_ip or current_snmp_host_interface_fields['port'] != host_new_data.snmp_port or current_snmp_host_interface_fields['version'] != host_new_data.snmp_version or current_snmp_host_interface_fields['community'] != host_new_data.snmp_community:
                update_interface_agent_result = await update_host_interface_zabbix(current_snmp_host_interface_fields['interfaceid'], 2, host_new_data.agent_ip, host_new_data.agent_port, host_new_data.snmp_version, host_new_data.snmp_community)
                response['result'] += update_interface_agent_result['detail']

            else:  # No es necesario actualizar la interfaz
                response['result'] += "No fue necesario actualizar la interface snmp del host."
    print("SE EJECUTA TODO")
    return response


async def update_host_interface_zabbix(interface_id, type, ip, port, snmp_version, snmp_community):
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        tipo = "Agent" if type == 1 else 'SNMP'
        host_interface_params = {
            'interfaceid': str(interface_id),
            "type": type,          # Tipo de interfaz: 1 = Zabbix agent, 2 = SNMP, 3 = IPMI, 4 = JMX
            "useip": 1,         # 1 = Usar IP, 0 = Usar DNS
            "ip": ip,  # Dirección IP de la interfaz
            "dns": "",          # DNS (vacío si useip=1)
            # Puerto del agente Zabbix (por defecto 10050)
            "port": str(port)
        }
        if type == 2:
            host_interface_params['details'] = {
                'version': snmp_version,
                'community': snmp_community
            }
        zabbix_api = ZabbixApi()
        zabbix_request_result = await zabbix_api.do_request_new(method='hostinterface.update', params=host_interface_params)
        if 'result' in zabbix_request_result:
            print(zabbix_request_result)
            response['success'] = True
            response['detail'] = f"Interface {tipo} actualizada correctamente. "
            return response
        else:
            response['detail'] = f"No se pudo actualizar la interface {tipo}: {zabbix_request_result['error']} "
            return response

    except Exception as e:
        response['success'] = False
        response['detail'] = e
        return response


async def delete_interface_host(interfaceid, type):
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        tipo = "Agent" if type == 1 else 2
        host_interface_params = [str(interfaceid)]
        zabbix_api = ZabbixApi()
        zabbix_request_result = await zabbix_api.do_request_new(method='hostinterface.delete', params=host_interface_params)
        if 'result' in zabbix_request_result:
            print(zabbix_request_result)
            response['success'] = True
            response['detail'] = f"Interface {tipo} eliminada correctamente. "
            return response
        else:
            response['detail'] = f"No se pudo eliminar la interface {tipo}: {zabbix_request_result['error']} "
            return response

    except Exception as e:
        response['success'] = False
        response['detail'] = e
        return response


async def create_interface(hostid, type, ip, port, snmp_version, snmp_community):
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        tipo = "Agent" if type == 1 else "SNMP"
        host_interface_params = {
            'hostid': str(hostid),
            'type': type,
            'main': 1,
            'useip': 1,
            'ip': ip,
            'dns': '',
            'port': str(port)
        }
        if type == 2:
            host_interface_params['details'] = {
                'version': snmp_version,
                'community': snmp_community
            }
        zabbix_api = ZabbixApi()
        zabbix_request_result = await zabbix_api.do_request_new(method='hostinterface.create', params=host_interface_params)
        if 'result' in zabbix_request_result:
            print(zabbix_request_result)
            response['success'] = True
            response['detail'] = F"Interface {tipo} creada correctamente. "
            return response
        else:
            response['detail'] = f"No se pudo crear la interface {tipo}: {zabbix_request_result['error']} "
            return response

    except Exception as e:
        response['success'] = False
        response['detail'] = e
        return response


async def update_host_brand_model(hostid, host_brand_model_current_data, host_new_data, db):
    response = {'result': None, 'success': True}
    new_host_brand_model_data_fields = await get_host_brand_model_data_fields(
        host_new_data.dict())
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
    new_host_inventory_data_fields = await get_host_data_inventory_data_fields(
        host_new_data.dict())
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
    host_current_data['proxy_id'] = host_current_data['proxy_hostid']
    host_current_data['status'] = host_current_data['status_value']

    current_host_data_fields = await get_host_data_fields(host_current_data)
    new_host_data_fields = await get_host_data_fields(host_new_data.dict())
    # 2 Verificar si existen cambios a realizar
    host_data_sets_are_diferent = await verify_new_vs_current_host_data(current_host_data_fields, new_host_data_fields)
    # 3 Actualizar el host data de ser necesario
    if host_data_sets_are_diferent:
        # 4 Actualizar host data
        # {'success': False, 'detail': '','exception':False}
        updated_host_data_result = await cassia_hosts_repository.update_host_data(hostid, host_new_data, db)
        if not updated_host_data_result['success']:
            response['result'] = updated_host_data_result['detail']
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
        'serialno_a': host_dict.serialno_a,
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
    init_1 = time.time()
    response = {'success': True, 'detail': ''}
    # Validar info del schema
    init = time.time()
    is_valid_info = await validate_info_schema(host_new_data, db, False, None)
    print(f"TIEMPO DE VALIDACION: {time.time()-init}")
    # 2 Crear host con inventory, interface y grupos
    # {'success': False, 'detail': '', 'hostid': 0}
    init = time.time()
    host_was_created = await create_host_by_zabbix(host_new_data)
    print(f"TIEMPO DE CREACION ZABBIX: {time.time()-init} ")
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
        print(f"TIEMPO DE TOTAL: {time.time()-init_1} ")
        return success_response(message=f"Host creado con hostid {hostid}", data=response['detail'])
    else:
        detail = host_was_created['detail']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al crear el host en zabbix {detail}")


async def validate_info_schema(host_new_data: cassia_hosts_schema.CassiaHostSchema, db: DB, is_update: bool, hostid):
    # valida ip y puerto de agent ip
    if host_new_data.agent_ip != "" and host_new_data.agent_ip is not None:
        try:
            valid_ip = IPv4Address(host_new_data.agent_ip)

        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="La ip Agent no es valida")
        if host_new_data.agent_port == "" or host_new_data.agent_port is None:
            host_new_data.agent_port = "10050"
            """ raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El puerto ip Agent no es valido") """
    # valida ip y puerto de snmp ip
    if host_new_data.snmp_ip != "" and host_new_data.snmp_ip is not None:
        try:
            valid_ip = IPv4Address(host_new_data.snmp_ip)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="La ip SNMP no es valida")
        if host_new_data.snmp_port == "" or host_new_data.snmp_port is None:
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
    if host_new_data.zona_groupid is not None:
        tasks['exist_zona_groupid'] = asyncio.create_task(
            cassia_hosts_repository.get_cassia_group_zona_type_by_groupid(host_new_data.zona_groupid, db))

    if host_new_data.groupids is not None and len(host_new_data.groupids) > 0:
        groupids = ",".join([str(groupid)
                            for groupid in host_new_data.groupids])
        tasks['exist_groupids'] = asyncio.create_task(
            cassia_hosts_repository.get_cassia_groups_type_host_by_groupids(groupids, db))

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
    if host_new_data.zona_groupid is not None:
        exist_zona_groupid = dfs['exist_zona_groupid']
        if exist_zona_groupid.empty:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El zona_groupid proporcionado no existe")

    if host_new_data.groupids is not None and len(host_new_data.groupids) > 0:
        exist_groupids = dfs['exist_groupids']
        groups_a_verificar = host_new_data.groupids
        groups_a_verificar = [str(group) for group in groups_a_verificar]
        todos_presentes = exist_groupids['groupid'].astype(
            'str').isin(groups_a_verificar).all()
        if not todos_presentes:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Algunos groupids no existen")
    return True


async def create_host_by_zabbix(host_data: cassia_hosts_schema.CassiaHostSchema):
    response = {'success': False, 'detail': '', 'hostid': 0}
    try:
        groups = []
        if host_data.groupids is not None and len(host_data.groupids) > 0:
            groups = [{"groupid": host_group}
                      for host_group in host_data.groupids]
        if host_data.zona_groupid is not None:
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
        if host_data.agent_ip != "" and host_data.agent_ip is not None:
            interfaces.append({"type": 1,  # 1 = Agent, 2 = SNMP, 3 = IPMI, 4 = JMX
                               "main": 1,  # Es la interfaz principal
                               "useip": 1,  # Si utiliza IP en lugar de nombre DNS
                               "ip": host_data.agent_ip,  # IP del host
                               "dns": "",  # DNS vacío si useip=1
                               # Puerto de la interfaz (por defecto para Zabbix Agent)
                               "port": str(host_data.agent_port)
                               })
        if host_data.snmp_ip != "" and host_data.snmp_ip is not None:
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


async def delete_host(hostid: int, db: DB):
    host = await cassia_hosts_repository.get_cassia_host(hostid, db)
    if host.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El host no existe")
    try:
        params = [str(hostid)]
        zabbix_api = ZabbixApi()
        zabbix_request_result = await zabbix_api.do_request_new(method='host.delete', params=params)
        if 'result' in zabbix_request_result:  # Se elimino el host con inventory, interface y grupos

            hostid_eliminado = zabbix_request_result['result']['hostids'][0]

            # eliminar el registro de cassia_hosts (marca y modelo)
            # response = {'success': False, 'detail': '', 'exception': False}
            get_host_brand_model = await cassia_hosts_repository.get_cassia_brand_model(hostid, db)
            if not get_host_brand_model.empty:
                delete_host_model_brand_result = await cassia_hosts_repository.delete_host_brand_model_by_hostid(hostid, db)
                if delete_host_model_brand_result['success']:
                    print("Se elimino marca y modelo.")
                    return success_response(message=f"Host {hostid} eliminado correctamente.")
                else:
                    print("No se elimino marca y modelo.")
                    return success_response(message=f"Se elimino el host {hostid}. No se pudo eliminar el registro de marca y modelo del host.")
            else:
                print("No fue necesario eliminar marca y modelo.")
                return success_response(message=f"Host {hostid} eliminado correctamente")
        else:
            error = zabbix_request_result['error']
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error al eliminar el host {error}")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al eliminar el host {e}")


async def import_hosts_data(file_import: File, db: DB):
    file_types = ('.csv', '.xlsx', '.xls', '.json')
    if not file_import.filename.endswith(file_types):
        raise HTTPException(
            status_code=400,
            detail="El archivo debe ser un CSV, JSON, XLS o XLSX"
        )
    processed_data = await get_df_by_filetype(file_import,
                                              ['host', 'name', 'proxy_hostid', 'agent_ip', 'agent_port',
                                               'snmp_ip', 'snmp_port', 'snmp_version', 'snmp_community', 'brand_id', 'model_id',
                                               'description', 'status_value',
                                               'technology_id', 'alias', 'location_lon',
                                               'location_lat', 'serialno_a', 'macaddress_a',
                                               'groupids', 'zona_groupid'])
    result = processed_data['result']
    if not result:
        exception = processed_data['exception']
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al procesar el archivo: {exception}")
    df_import = processed_data['df']
    if df_import.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"El archivo esta vacio")
    duplicados = df_import.duplicated(
        subset=['name'], keep=False).any()

    if duplicados:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Existen nombres de host duplicados en el archivo.")

    df_import = df_import.rename(columns={'proxy_hostid': 'proxy_id',
                                          'status_value': 'status',
                                          'technology_id': 'device_id'})
    df_import['snmp_port'] = df_import['snmp_port'].replace(np.nan, 0)
    df_import['snmp_port'] = df_import['snmp_port'].astype('int64')
    df_import['snmp_port'] = df_import['snmp_port'].replace(0, None)
    df_import = df_import.replace(np.nan, None)

    df_import['groupids'] = df_import['groupids'].astype('str').apply(
        lambda x: [i for i in x.split(',')])
    info = [cassia_hosts_schema.CassiaHostSchema(
            **df_import.iloc[ind].to_dict()) for ind in df_import.index]
    tasks_create = [
        asyncio.create_task(create_host_import(info_host, db)) for info_host in info
    ]
    df_import_results = pd.DataFrame(
        columns=['host', 'name', 'proxy_id', 'agent_ip', 'agent_port',
                 'snmp_ip', 'snmp_port', 'snmp_version', 'snmp_community', 'brand_id', 'model_id',
                 'description', 'status',
                 'device_id', 'alias', 'location_lon',
                 'location_lat', 'serialno_a', 'macaddress_a',
                 'groupids', 'zona_groupid'])
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


async def create_host_import(host_new_data: cassia_hosts_schema.CassiaHostSchema, db: DB):
    response = host_new_data.dict()
    print(response)
    response['result'] = 'No se creo correctamente el host.'
    response['detail'] = ''
    response['hostid_creado'] = ''

    init_1 = time.time()
    # Validar info del schema
    init = time.time()
    # result = {'success': False, 'result': ''}
    is_valid_info = await validate_info_schema_import(host_new_data, db, False, None)
    if not is_valid_info['success']:
        response['detail'] = is_valid_info['result']
        return response
    print(f"TIEMPO DE VALIDACION: {time.time()-init}")
    # 2 Crear host con inventory, interface y grupos
    # {'success': False, 'detail': '', 'hostid': 0}
    init = time.time()
    host_was_created = await create_host_by_zabbix(host_new_data)
    print(f"TIEMPO DE CREACION ZABBIX: {time.time()-init} ")
    if host_was_created['success']:
        print(host_was_created)
        # Obtener hostid creado
        hostid = host_was_created['hostid']
        response['hostid_creado'] = hostid
        response['result'] = 'Se creo el host.'
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
        print(f"TIEMPO DE TOTAL: {time.time()-init_1} ")
        return response
    else:
        detail = host_was_created['detail']
        response['detail'] = detail
        return response


async def validate_info_schema_import(host_new_data: cassia_hosts_schema.CassiaHostSchema, db: DB, is_update: bool, hostid):
    result = {'success': False, 'result': ''}

    # valida ip y puerto de agent ip
    if host_new_data.agent_ip != "" and host_new_data.agent_ip is not None:
        try:
            valid_ip = IPv4Address(host_new_data.agent_ip)

        except Exception as e:
            result['result'] += 'La ip Agent no es valida. '
        if host_new_data.agent_port == "" or host_new_data.agent_port is None:
            host_new_data.agent_port = "10050"
            """ raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="El puerto ip Agent no es valido") """
    # valida ip y puerto de snmp ip
    if host_new_data.snmp_ip != "" and host_new_data.snmp_ip is not None:
        try:
            valid_ip = IPv4Address(host_new_data.snmp_ip)
        except Exception as e:
            result['result'] += 'La ip SNMP no es valida. '

        if host_new_data.snmp_port == "" or host_new_data.snmp_port is None:
            result['result'] += 'El puerto ip SNMP no es valido.  '
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
    if host_new_data.zona_groupid is not None:
        tasks['exist_zona_groupid'] = asyncio.create_task(
            cassia_hosts_repository.get_cassia_group_zona_type_by_groupid(host_new_data.zona_groupid, db))

    if host_new_data.groupids is not None and len(host_new_data.groupids) > 0:
        groupids = ",".join([str(groupid)
                            for groupid in host_new_data.groupids])
        tasks['exist_groupids'] = asyncio.create_task(
            cassia_hosts_repository.get_cassia_groups_type_host_by_groupids(groupids, db))

    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    exist_host_name = dfs['exist_name']
    # Retornar excepciones correspondientes
    if not exist_host_name.empty:
        if is_update:
            if exist_host_name['hostid'].astype('int64')[0] != hostid:
                result['result'] += 'El nombre del host ya existe. '
        else:
            result['result'] += 'El nombre del host ya existe. '
    if host_new_data.brand_id is not None:
        exist_brand_id = dfs['exist_brand_id']
        if exist_brand_id.empty:
            result['result'] += 'La marca proporcionada no existe. '
    if host_new_data.model_id is not None:
        exist_model_id = dfs['exist_model_id']
        if exist_model_id.empty:
            result['result'] += 'El modelo proporcionado no existe. '
    if host_new_data.proxy_id is not None:
        exist_proxy_id = dfs['exist_proxy_id']
        if exist_proxy_id.empty:
            result['result'] += 'El proxy proporcionado no existe. '
    if host_new_data.device_id is not None:
        exist_device_id = dfs['exist_device_id']
        if exist_device_id.empty:
            result['result'] += 'El technology_id proporcionado no existe. '
    if host_new_data.zona_groupid is not None:
        exist_zona_groupid = dfs['exist_zona_groupid']
        if exist_zona_groupid.empty:
            result['result'] += 'El zona_groupid proporcionado no existe. '

    if host_new_data.groupids is not None and len(host_new_data.groupids) > 0:
        exist_groupids = dfs['exist_groupids']
        groups_a_verificar = host_new_data.groupids
        groups_a_verificar = [str(group) for group in groups_a_verificar]
        todos_presentes = exist_groupids['groupid'].astype(
            'str').isin(groups_a_verificar).all()
        print(exist_groupids)
        print(groups_a_verificar)
        if not todos_presentes:
            result['result'] += 'Algunos groupids no existen. '
    if result['result'] == '':
        result['success'] = True
    return result
