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

    statement = text("call sp_catCity()")

    municipios = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(municipios).replace(np.nan, "")
    db_zabbix.Session().close()
    db_zabbix.stop()
    return success_response(data=data.to_dict(orient="records"))


def get_devices():
    db_zabbix = DB_Zabbix()
    statement = text("call sp_catDevice()")
    devices = db_zabbix.Session().execute(statement)
    db_zabbix.Session().close()
    db_zabbix.stop()
    data = pd.DataFrame(devices).replace(np.nan, "")
    return success_response(data=data.to_dict(orient="records"))


def get_technologies():
    db_zabbix = DB_Zabbix()
    statement = text("call sp_catTecnologie()")
    technologies = db_zabbix.Session().execute(statement)
    db_zabbix.Session().close()
    db_zabbix.stop()
    data = pd.DataFrame(technologies).replace(np.nan, "")
    return success_response(data=data.to_dict(orient="records"))
