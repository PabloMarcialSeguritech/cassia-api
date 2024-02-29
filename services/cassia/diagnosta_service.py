from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
from utils.traits import success_response
import httpx
from fastapi import HTTPException
settings = Settings()
abreviatura_estado = settings.abreviatura_estado
diagnosta_api_url = settings.cassia_diagnosta_api_url


async def analize_host(hostid_or_ip: str):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{diagnosta_api_url}/analisis/{hostid_or_ip}", timeout=120)
            response.raise_for_status()
            response = response.json()
            desconectados_pd = pd.DataFrame(columns=['hostid',
                                                     'host',
                                                     'afiliacion',
                                                     'name',
                                                     'device_id',
                                                     'ip',
                                                     'b_interes',
                                                     'b_ubicacion',
                                                     'capa',
                                                     'conectado'])
            if 'capaGeneral' in response:
                capa = response['capaGeneral']
                if 'desconectados_dependientes' in capa:
                    desconectados = capa['desconectados_dependientes']
                    problematico = pd.DataFrame(columns=['hostid',
                                                         'host',
                                                         'afiliacion',
                                                         'name',
                                                         'device_id',
                                                         'ip',
                                                         'b_interes',
                                                         'b_ubicacion',
                                                         'capa',
                                                         'conectado'])
                    if 'dispositivo_problematico' in capa:
                        problematico_data = capa['dispositivo_problematico']
                        print(problematico_data)
                        problematico = pd.DataFrame(columns=['hostid',
                                                             'host',
                                                             'afiliacion',
                                                             'name',
                                                             'device_id',
                                                             'ip',
                                                             'b_interes',
                                                             'b_ubicacion',
                                                             'capa',
                                                             'conectado'], data=[problematico_data])
                        problematico['origen'] = 1
                    if len(desconectados):
                        desconectados_pd = pd.DataFrame(columns=['hostid',
                                                                 'host',
                                                                 'afiliacion',
                                                                 'name',
                                                                 'device_id',
                                                                 'ip',
                                                                 'b_interes',
                                                                 'b_ubicacion',
                                                                 'capa',
                                                                 'conectado'], data=desconectados)
                        desconectados_pd['origen'] = 0
                        if not problematico.empty:
                            desconectados_pd = pd.concat(
                                [problematico, desconectados_pd], ignore_index=True).replace(np.nan, '')
                        with DB_Zabbix().Session() as session:
                            tech_names = text("call sp_catDevice(0)")
                            tech_names = pd.DataFrame(
                                session.execute(tech_names))
                            tech_names['dispId'] = tech_names['dispId'].replace(
                                np.nan, 0).astype(int)
                            desconectados_pd['device_id'] = desconectados_pd['device_id'].replace(
                                np.nan, 0).astype(int)
                            tech_names.rename(
                                columns={'name': 'tech_name'}, inplace=True)
                            desconectados_pd = pd.merge(desconectados_pd, tech_names,
                                                        how='left', left_on='device_id', right_on='dispId').replace(np.nan, '')

            return success_response(data=desconectados_pd.to_dict(orient="records"))
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=exc.response.status_code,
                            detail="Error fetching data from the API")
