from fastapi import HTTPException, status
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np


async def get_proxies(db: DB) -> pd.DataFrame:
    try:
        query_statement_get_proxies = DBQueries().query_statement_get_proxies
        proxies_data = await db.run_query(query_statement_get_proxies)
        proxies_df = pd.DataFrame(proxies_data).replace(np.nan, None)
        return proxies_df
    except Exception as e:
        print(f"Excepcion en get_proxies: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_proxies: {e}")


async def search_interface_by_ip(ip, db: DB) -> pd.DataFrame:
    try:
        query_statement_get_interface_by_ip = DBQueries(
        ).builder_query_statement_get_interface_by_ip(ip)
        proxy_interface_data = await db.run_query(query_statement_get_interface_by_ip)
        proxy_interface_df = pd.DataFrame(
            proxy_interface_data).replace(np.nan, None)
        return proxy_interface_df
    except Exception as e:
        print(f"Excepcion en search_interface_by_ip: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en search_interface_by_ip: {e}")


async def search_host_by_name(name, db: DB) -> pd.DataFrame:
    try:
        query_statement_get_host_by_hostname = DBQueries(
        ).builder_query_statement_get_host_by_hostname(name)
        proxy_data = await db.run_query(query_statement_get_host_by_hostname)
        proxy_df = pd.DataFrame(proxy_data).replace(np.nan, None)
        return proxy_df
    except Exception as e:
        print(f"Excepcion en search_host_by_name: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en search_host_by_name: {e}")
