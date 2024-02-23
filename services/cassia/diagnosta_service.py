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
            return response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=exc.response.status_code,
                            detail="Error fetching data from the API")


async def get_ci_elements_technologies():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""call sp_catCiTechnology()""")
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if not results.empty:
        results['id'] = results['tech_id']
        results['value'] = results['technology']
    session.close()
    return success_response(data=results.to_dict(orient="records"))
