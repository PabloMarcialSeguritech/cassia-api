from pyzabbix import ZabbixAPI
from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import time
import json
import numpy as np
settings = Settings()


class ZabbixService():
    zapi: ZabbixAPI

    def __init__(self) -> None:
        self.zapi = ZabbixAPI(settings.zabbix_server_url)
        self.zapi.login(api_token=settings.zabbix_api_token)

    def get_municipios(self):
        groups = pd.DataFrame(self.zapi.hostgroup.get(groupids=[54, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                                                                81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107]))
        groups = groups.sort_values("name")
        return groups.to_dict(orient="records")


def get_municipios():
    t0 = time.time()
    db_zabbix = DB_Zabbix()

    # db = DB_Zabbix.Session()
    statement = text("SELECT * FROM vw_municipio")

    municipios = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(municipios)
    t1 = time.time()
    total = t1-t0
    print(total)
    db_zabbix.stop()
    return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))


def get_devices():
    db_zabbix = DB_Zabbix()
    statement = text("SELECT * FROM vw_divice")
    devices = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(devices)
    return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))


def get_technologies():
    db_zabbix = DB_Zabbix()
    statement = text("SELECT * FROM vw_tecnologia")
    technologies = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(technologies)
    return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))


def get_problems():
    db_zabbix = DB_Zabbix()
    statement = text("SELECT * FROM vw_problems")
    problems = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(problems)
    return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))


def get_problems_filter(municipalityId, tech, hostType):
    db_zabbix = DB_Zabbix()
    statement = text(
        f"call sp_problemView('{municipalityId}','{tech}','{hostType}')")
    problems = db_zabbix.Session().execute(statement)
    # print(problems)
    data = pd.DataFrame(problems)
    data = data.replace(np.nan, "")
    return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))


def get_host_filter(municipalityId, tech, hostType):
    db_zabbix = DB_Zabbix()
    statement = text(
        f"call sp_hostView('{municipalityId}','{tech}','{hostType}')")
    hosts = db_zabbix.Session().execute(statement)
    # print(problems)
    data = pd.DataFrame(hosts)
    data = data.replace(np.nan, "")
    return JSONResponse(content=jsonable_encoder(data.to_dict(orient="records")))
