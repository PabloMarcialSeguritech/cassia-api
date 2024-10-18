from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np
import time


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


async def get_cassia_hosts_by_ids(hostids: list, db: DB) -> pd.DataFrame:
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


async def update_host_data(hostid, host_new_data, db: DB):
    response = {'success': False, 'detail': ''}
    try:
        query_statement_update_host_data = DBQueries(
        ).query_update_host_data
        result = db.run_query(query_statement_update_host_data, (
            host_new_data.get('host', ''),
            host_new_data.get('name', ''),
            host_new_data.get('description', ''),
            host_new_data.get('proxy_id', None),
            host_new_data.get('status', 0),
            hostid
        ))
        if result:
            response['success'] = True
            response['detail'] = "Se actualizo correctamente el registro"
        else:
            response['detail'] = "No se actualizo el registro"
        return response

    except Exception as e:
        response['detail'] = e
        return response
