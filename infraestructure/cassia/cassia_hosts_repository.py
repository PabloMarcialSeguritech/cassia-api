from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries

from fastapi import HTTPException, status
from schemas import cassia_hosts_schema
import pandas as pd
import numpy as np
import time


async def delete_host_brand_model_by_hostid(hostid, db: DB) -> pd.DataFrame:
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        query_statement_delete_host_brand_model_by_hostid = DBQueries(
        ).query_statement_delete_host_brand_model_by_hostid
        init = time.time()
        hosts_brand_model_delete = await db.run_query(query_statement_delete_host_brand_model_by_hostid,
                                                      (hostid,), True)
        print(f"DURACION DB QUERY DELETE DEVICE ID: {time.time()-init}")
        if hosts_brand_model_delete:
            response['success'] = True
            response['detail'] = "Se elimino correctamente la marca y el modelo. "
        else:
            response['detail'] = "No se elimino correctamente la marca y el modelo. "
        return response

    except Exception as e:
        response['detail'] = e
        response['exception'] = True
        return response


async def get_cassia_groups_by_hostid(hostid, db: DB) -> pd.DataFrame:
    hosts_groups_df = None
    try:
        query_statement_get_host_groups_by_hostid = DBQueries(
        ).query_statement_get_host_groups_by_hostid
        init = time.time()
        hosts_groups_data = await db.run_query(query_statement_get_host_groups_by_hostid, (hostid))
        print(f"DURACION DB QUERY HOTS_GROUPS: {time.time()-init}")
        hosts_groups_df = pd.DataFrame(
            hosts_groups_data).replace(np.nan, None)
        return hosts_groups_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_groups_by_hostid: {e}")


async def get_cassia_groups_type_host_by_groupids(groupids, db: DB) -> pd.DataFrame:
    hosts_groups_df = None
    try:
        query_statement_get_host_groups_type_host_by_groupids = DBQueries(
        ).query_statement_get_host_groups_type_host_by_groupids
        init = time.time()
        hosts_groups_data = await db.run_query(query_statement_get_host_groups_type_host_by_groupids, (groupids))
        print(f"DURACION DB QUERY GROUP TYPES: {time.time()-init}")
        hosts_groups_df = pd.DataFrame(
            hosts_groups_data).replace(np.nan, None)
        return hosts_groups_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_groups_type_host_by_groupids: {e}")


async def get_cassia_group_zona_type_by_groupid(groupid, db: DB) -> pd.DataFrame:
    hosts_group_zona_df = None
    try:
        query_statement_get_host_group_type_zona_by_groupid = DBQueries(
        ).query_statement_get_host_group_type_zona_by_groupid
        init = time.time()
        hosts_group_zona_data = await db.run_query(query_statement_get_host_group_type_zona_by_groupid, (groupid))
        print(f"DURACION DB QUERY GROUP ZONA TYPE: {time.time()-init}")
        hosts_group_zona_df = pd.DataFrame(
            hosts_group_zona_data).replace(np.nan, None)
        return hosts_group_zona_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_group_zona_type_by_groupid: {e}")


async def get_cassia_brand_model(hostid, db: DB) -> pd.DataFrame:
    hosts_brand_model_df = None
    try:
        query_statement_get_host_brand_model_by_hostid = DBQueries(
        ).query_statement_get_host_brand_model_by_hostid
        init = time.time()
        hosts_brand_model_data = await db.run_query(query_statement_get_host_brand_model_by_hostid, (hostid))
        print(f"DURACION DB QUERY INTERFACE HOST: {time.time()-init}")
        hosts_brand_model_df = pd.DataFrame(
            hosts_brand_model_data).replace(np.nan, None)
        return hosts_brand_model_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_brand_model: {e}")


async def get_cassia_host_interfaces_by_hostid(hostid, db: DB) -> pd.DataFrame:
    hosts_interfaces_df = None
    try:
        query_statement_get_host_interfaces_by_hostid = DBQueries(
        ).query_statement_get_host_interfaces_by_hostid
        init = time.time()

        hosts_interfaces_data = await db.run_query(query_statement_get_host_interfaces_by_hostid, (hostid))
        print(f"DURACION DB QUERY INTERFACE HOST: {time.time()-init}")
        hosts_interfaces_df = pd.DataFrame(
            hosts_interfaces_data).replace(np.nan, None)
        if hosts_interfaces_df.empty:
            hosts_interfaces_df = pd.DataFrame(
                columns=['interface_id', 'type'])
        return hosts_interfaces_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_interfaces_by_hostid: {e}")


async def update_host_brand_model_data(hostid, new_host_brand_model_data_fields: dict, alias, db: DB) -> pd.DataFrame:
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        query_statement_update_host_inventory_data_by_hostid = DBQueries(
        ).query_statement_update_host_model_brand_by_hostid
        init = time.time()
        hosts_inventory_updated = await db.run_query(query_statement_update_host_inventory_data_by_hostid,
                                                     (new_host_brand_model_data_fields['brand_id'],
                                                      new_host_brand_model_data_fields['model_id'],
                                                      alias,
                                                      hostid,), True)
        print(f"DURACION DB QUERY UPDATE DEVICE ID: {time.time()-init}")
        if hosts_inventory_updated:
            response['success'] = True
            response['detail'] = "Se actualizo correctamente la marca y el modelo. "
        else:
            response['detail'] = "No se actualizo correctamente la marca y el modelo. "
        return response

    except Exception as e:
        response['detail'] = e
        response['exception'] = True
        return response


async def update_host_inventory_data(hostid, new_host_inventory_data_fields: dict, db: DB) -> pd.DataFrame:
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        query_statement_update_host_inventory_data_by_hostid = DBQueries(
        ).query_statement_update_host_inventory_data_by_hostid
        init = time.time()
        hosts_inventory_updated = await db.run_query(query_statement_update_host_inventory_data_by_hostid,
                                                     (new_host_inventory_data_fields['device_id'],
                                                      new_host_inventory_data_fields['alias'],
                                                      new_host_inventory_data_fields['location_lat'],
                                                      new_host_inventory_data_fields['location_lon'],
                                                      new_host_inventory_data_fields['serialno_a'],
                                                      new_host_inventory_data_fields['macaddress_a'],
                                                      hostid,), True)
        print(f"DURACION DB QUERY UPDATE DEVICE ID: {time.time()-init}")
        if hosts_inventory_updated:
            response['success'] = True
            response['detail'] = "Se actualizo correctamente el inventory data. "
        else:
            response['detail'] = "No se actualizo correctamente el inventory data. "
        return response

    except Exception as e:
        response['detail'] = e
        response['exception'] = True
        return response


async def assign_brand_model_affiliation_by_hostid(hostid, host_data: cassia_hosts_schema.CassiaHostSchema, db: DB) -> pd.DataFrame:
    print("1")
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        print("2")
        query_statement_insert_cassia_host = DBQueries(
        ).query_statement_insert_cassia_host
        print(query_statement_insert_cassia_host)
        init = time.time()
        print((hostid, host_data.alias, host_data.brand_id, host_data.model_id))
        cassia_host_insert = await db.run_query(query_statement_insert_cassia_host, (hostid, host_data.alias, host_data.brand_id, host_data.model_id), True)
        print("3")
        print(f"DURACION DB QUERY INSERT marca y modelo: {time.time()-init}")
        print(query_statement_insert_cassia_host)
        print(cassia_host_insert)

        if cassia_host_insert:
            response['success'] = True
            response['detail'] = "Se inserto correctamente el registro"
        else:
            response['detail'] = "No se inserto el registro"
        return response

    except Exception as e:
        response['detail'] = e
        response['exception'] = True
        return response


async def update_host_device_id_by_hostid(hostid, device_id, db: DB) -> pd.DataFrame:
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        query_statement_update_host_device_id_by_hostid = DBQueries(
        ).query_statement_update_host_device_id_by_hostid
        init = time.time()
        hosts_inventory_updated = await db.run_query(query_statement_update_host_device_id_by_hostid, (device_id, hostid), True)
        print(f"DURACION DB QUERY UPDATE DEVICE ID: {time.time()-init}")
        print(query_statement_update_host_device_id_by_hostid)
        print(hosts_inventory_updated)
        if hosts_inventory_updated:
            response['success'] = True
            response['detail'] = "Se actualizo correctamente el registro"
        else:
            response['detail'] = "No se actualizo el registro"
        return response

    except Exception as e:
        response['detail'] = e
        response['exception'] = True
        return response


async def get_cassia_host_inventory_data_by_id(hostid, db: DB) -> pd.DataFrame:
    hosts_inventory_df = None
    try:
        query_statement_get_cassia_hosts = DBQueries(
        ).query_statement_get_host_inventory_by_id
        init = time.time()
        hosts_inventory_data = await db.run_query(query_statement_get_cassia_hosts, (hostid))
        print(f"DURACION DB QUERY HOST: {time.time()-init}")
        hosts_inventory_df = pd.DataFrame(
            hosts_inventory_data).replace(np.nan, None)
        return hosts_inventory_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_hosts: {e}")


async def get_cassia_host(hostid, db: DB) -> pd.DataFrame:
    hosts_df = None
    try:
        query_statement_get_cassia_hosts = DBQueries(
        ).query_statement_get_cassia_host
        init = time.time()
        hosts_data = await db.run_query(query_statement_get_cassia_hosts, (hostid))
        print(f"DURACION DB QUERY HOST: {time.time()-init}")
        hosts_df = pd.DataFrame(
            hosts_data).replace(np.nan, None)
        return hosts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host: {e}")


async def get_cassia_hosts(db: DB) -> pd.DataFrame:
    hosts_df = None
    try:
        query_statement_get_cassia_hosts = DBQueries(
        ).query_statement_get_cassia_hosts
        init = time.time()
        hosts_data = await db.run_query(query_statement_get_cassia_hosts)
        print(f"DURACION DB QUERY HOST: {time.time()-init}")
        hosts_df = pd.DataFrame(
            hosts_data).replace(np.nan, None)
        return hosts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_hosts: {e}")


async def get_cassia_hosts_by_ids(hostids, db: DB) -> pd.DataFrame:
    hosts_df = None
    try:
        query_statement_get_cassia_hosts_by_ids = DBQueries(
        ).builder_query_statement_get_cassia_hosts_by_ids(hostids)
        init = time.time()
        hosts_data = await db.run_query(query_statement_get_cassia_hosts_by_ids)
        print(f"DURACION DB QUERY HOST: {time.time()-init}")
        hosts_df = pd.DataFrame(
            hosts_data).replace(np.nan, None)
        return hosts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_hosts_by_ids: {e}")


async def get_cassia_hosts_export_by_ids(hostids, db: DB) -> pd.DataFrame:
    hosts_df = None
    try:
        query_statement_get_cassia_hosts_by_ids = DBQueries(
        ).builder_query_statement_get_cassia_hosts_export_by_ids(hostids)
        init = time.time()
        hosts_data = await db.run_query(query_statement_get_cassia_hosts_by_ids)
        print(f"DURACION DB QUERY HOST: {time.time()-init}")
        hosts_df = pd.DataFrame(
            hosts_data).replace(np.nan, None)
        return hosts_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_hosts_by_ids: {e}")


async def update_host_data(hostid, host_new_data, db: DB):
    response = {'success': False, 'detail': '', 'exception': False}
    try:
        query_statement_update_host_data = DBQueries(
        ).query_update_host_data
        host_data = host_new_data.dict()
        """ if host_data['proxy_id'] is None or "":
            host_data['proxy_id'] = "NULL" """
        print(query_statement_update_host_data)
        print(host_data)
        result = await db.run_query(query_statement_update_host_data, (
            host_data.get('host', ''),
            host_data.get('name', ''),
            host_data.get('description', ''),
            host_data.get('proxy_id', None),
            host_data.get('status', 0),
            hostid
        ), True)
        print(result)
        if result:
            response['success'] = True
            response['detail'] = "Se actualizo correctamente el registro"
        else:
            response['detail'] = "No se actualizo el registro"
        return response

    except Exception as e:
        response['detail'] = e
        response['exception'] = True
        return response
