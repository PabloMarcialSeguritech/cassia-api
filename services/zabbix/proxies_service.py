from fastapi import status, HTTPException
from utils.traits import success_response, timestamp_to_date_tz
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from infraestructure.zabbix import proxies_repository
import pandas as pd


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
