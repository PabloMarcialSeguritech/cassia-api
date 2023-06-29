from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from utils.traits import success_response
settings = Settings()


def get_municipios():
    # t0 = time.time()
    db_zabbix = DB_Zabbix()

    # db = DB_Zabbix.Session()
    statement = text("SELECT * FROM vw_municipio")

    municipios = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(municipios)
    # t1 = time.time()
    # total = t1-t0
    # print(total)
    db_zabbix.Session().close()
    db_zabbix.stop()
    return success_response(data=data.to_dict(orient="records"))
    # return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))


def get_devices():
    db_zabbix = DB_Zabbix()
    statement = text("SELECT * FROM vw_divice")
    devices = db_zabbix.Session().execute(statement)
    db_zabbix.Session().close()
    db_zabbix.stop()
    data = pd.DataFrame(devices)
    return success_response(data=data.to_dict(orient="records"))


def get_technologies():
    db_zabbix = DB_Zabbix()
    statement = text("SELECT * FROM vw_tecnologia")
    technologies = db_zabbix.Session().execute(statement)
    db_zabbix.Session().close()
    db_zabbix.stop()
    data = pd.DataFrame(technologies)
    return success_response(data=data.to_dict(orient="records"))
