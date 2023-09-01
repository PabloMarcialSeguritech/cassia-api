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
    session.close()
    return success_response(data=data.to_dict(orient="records"))


def get_devices():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text("call sp_catDevice('0')")
    devices = session.execute(statement)
    data = pd.DataFrame(devices).replace(np.nan, "")
    session.close()
    return success_response(data=data.to_dict(orient="records"))


async def get_devices_by_municipality(municipalityId):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_catDevice('{municipalityId}')")
    devices = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(devices).replace(np.nan, "")
    session.close()
    return success_response(data=data.to_dict(orient="records"))


def get_subtypes():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text("call sp_catSubtype()")
    subtypes = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(subtypes).replace(np.nan, "")
    session.close()
    return success_response(data=data.to_dict(orient="records"))
