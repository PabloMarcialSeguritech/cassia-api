from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from utils.traits import success_response
import numpy as np
settings = Settings()


def get_municipios():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text("call sp_catCity()")
    municipios = session.execute(statement)
    data = pd.DataFrame(municipios).replace(np.nan, "")
    if len(data) > 0:
        data["id"] = data["groupid"]
    session.close()
    return success_response(data=data.to_dict(orient="records"))


def get_devices():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text("call sp_catDevice('0')")
    devices = session.execute(statement)
    data = pd.DataFrame(devices).replace(np.nan, "")
    if len(data) > 0:
        data["id"] = data["dispId"]
    session.close()
    return success_response(data=data.to_dict(orient="records"))


async def get_devices_by_municipality(municipalityId):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_catDevice('{municipalityId}')")
    devices = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(devices).replace(np.nan, "")
    if len(data) > 0:
        data["id"] = data["dispId"]
    session.close()
    return success_response(data=data.to_dict(orient="records"))


def get_subtypes(techId):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_catMetric('{techId}')")
    subtypes = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(subtypes).replace(np.nan, "")
    if len(data) > 0:

        if len(data.loc[data["template_id"] == 0]) == 0:
            df = {'template_id': 0, "nickname": "NA", "id": 0}
            data = pd.concat(
                [pd.DataFrame(df, index=[0]), data], ignore_index=True)
        data["id"] = data["template_id"]
    session.close()
    return success_response(data=data.to_dict(orient="records"))
