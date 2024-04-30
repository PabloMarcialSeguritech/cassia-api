from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from utils.traits import success_response
from infraestructure.zabbix import host_repository
from infraestructure.zabbix import host_groups_repository
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


async def get_municipios_async():

    municipios = await host_groups_repository.get_catalog_city()
    if len(municipios) > 0:
        municipios["id"] = municipios["groupid"]
    return success_response(data=municipios.to_dict(orient="records"))


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


async def get_devices_async():
    devices = await host_groups_repository.get_device_type_catalog(0)
    if len(devices) > 0:
        devices["id"] = devices["dispId"]
    return success_response(data=devices.to_dict(orient="records"))


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


async def get_devices_by_municipality_async(municipalityId):
    devices = await host_groups_repository.get_device_type_catalog(int(municipalityId))
    if len(devices) > 0:
        devices["id"] = devices["dispId"]
    return success_response(data=devices.to_dict(orient="records"))


async def get_devices_by_municipality_map(municipalityId):
    db_zabbix = DB_Zabbix()

    session = db_zabbix.Session()
    statement = text(f"call sp_catDevice('{municipalityId}')")
    devices = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(devices).replace(np.nan, "")
    df = {'dispId': 0, "name": "TODAS",
          "id": 0}

    if len(data) > 0:
        data["id"] = data["dispId"]
        if len(data.loc[data["dispId"] == "0"]) == 0:
            data = pd.concat(
                [pd.DataFrame(df, index=[0]), data], ignore_index=True)
    else:
        data = pd.DataFrame(df, index=[0])
    session.close()
    return success_response(data=data.to_dict(orient="records"))


async def get_devices_by_municipality_map_async(municipalityId):
    devices = await host_groups_repository.get_device_type_catalog(int(municipalityId))
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_catDevice('{municipalityId}')")
    devices = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(devices).replace(np.nan, "") """
    df = {'dispId': 0, "name": "TODAS",
          "id": 0}

    if len(devices) > 0:
        devices["id"] = devices["dispId"]
        if len(devices.loc[devices["dispId"] == "0"]) == 0:
            devices = pd.concat(
                [pd.DataFrame(df, index=[0]), devices], ignore_index=True)
    else:
        devices = pd.DataFrame(df, index=[0])
    return success_response(data=devices.to_dict(orient="records"))


def get_subtypes(techId):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_catMetric('{techId}')")
    subtypes = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(subtypes).replace(np.nan, "")
    df = {'template_id': "0", "nickname": "NA",
          "id": 0, "value": "0", 'group_id': 0}
    if len(data) > 0:

        if len(data.loc[data["template_id"] == "0"]) == 0:
            data = pd.concat(
                [pd.DataFrame(df, index=[0]), data], ignore_index=True)
        data["id"] = data["group_id"]
        data["value"] = data["template_id"]
    else:
        data = pd.DataFrame(df, index=[0])
    session.close()
    print(data.head())
    return success_response(data=data.to_dict(orient="records"))


async def get_subtypes_async(techId):
    subtypes = await host_groups_repository.get_tech_metrics_catalog(int(techId))
    df = {'template_id': "0", "nickname": "NA",
          "id": 0, "value": "0", 'group_id': 0}
    if len(subtypes) > 0:
        if len(subtypes.loc[subtypes["template_id"] == "0"]) == 0:
            subtypes = pd.concat(
                [pd.DataFrame(df, index=[0]), subtypes], ignore_index=True)
        subtypes["id"] = subtypes["group_id"]
        subtypes["value"] = subtypes["template_id"]
    else:
        subtypes = pd.DataFrame(df, index=[0])
    return success_response(data=subtypes.to_dict(orient="records"))


def get_brands(techId):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    techId = "" if techId == "0" else techId
    statement = text(f"call sp_catBrand('{techId}')")
    brands = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(brands).replace(np.nan, "")
    df = {'brand_id': "0", "name_brand": "TODAS",
          "id": 0, "value": "TODAS"}
    if len(data) > 0:
        if len(data.loc[data["brand_id"] == "0"]) == 0:
            data = pd.concat(
                [pd.DataFrame(df, index=[0]), data], ignore_index=True)
        data["id"] = data["brand_id"]
        data["value"] = data["name_brand"]
    else:
        data = pd.DataFrame(df, index=[0])
    session.close()
    return success_response(data=data.to_dict(orient="records"))


async def get_brands_async(techId):
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session() """
    techId = "" if techId == "0" else techId
    brands = await host_groups_repository.get_device_brands_catalog(techId)
    """ statement = text(f"call sp_catBrand('{techId}')")
    brands = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(brands).replace(np.nan, "") """
    df = {'brand_id': "0", "name_brand": "TODAS",
          "id": 0, "value": "TODAS"}
    if len(brands) > 0:
        if len(brands.loc[brands["brand_id"] == "0"]) == 0:
            brands = pd.concat(
                [pd.DataFrame(df, index=[0]), brands], ignore_index=True)
        brands["id"] = brands["brand_id"]
        brands["value"] = brands["name_brand"]
    else:
        brands = pd.DataFrame(df, index=[0])
    return success_response(data=brands.to_dict(orient="records"))


def get_models(brand_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_catModel('{brand_id}')")
    subtypes = db_zabbix.Session().execute(statement)
    data = pd.DataFrame(subtypes).replace(np.nan, "")
    df = {'model_id': "0", "name_model": "TODOS",
          "id": 0, "value": "TODOS"}
    if len(data) > 0:
        if len(data.loc[data["model_id"] == "0"]) == 0:
            data = pd.concat(
                [pd.DataFrame(df, index=[0]), data], ignore_index=True)
        data["id"] = data["model_id"]
        data["value"] = data["name_model"]
    else:
        data = pd.DataFrame(df, index=[0])
    session.close()
    print(data.head())
    return success_response(data=data.to_dict(orient="records"))


async def get_models_async(brand_id):
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_catModel('{techId}')")
    subtypes = db_zabbix.Session().execute(statement) """
    models = await host_groups_repository.get_device_models_catalog_by_brand(brand_id)
    """ data = pd.DataFrame(subtypes).replace(np.nan, "") """
    df = {'model_id': "0", "name_model": "TODOS",
          "id": 0, "value": "TODOS"}
    if len(models) > 0:
        if len(models.loc[models["model_id"] == "0"]) == 0:
            models = pd.concat(
                [pd.DataFrame(df, index=[0]), models], ignore_index=True)
        models["id"] = models["model_id"]
        models["value"] = models["name_model"]
    else:
        models = pd.DataFrame(df, index=[0])
    """ session.close()
    print(data.head()) """
    return success_response(data=models.to_dict(orient="records"))
