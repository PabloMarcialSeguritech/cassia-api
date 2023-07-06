from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from utils.traits import success_response
import numpy as np
settings = Settings()


def get_host_filter(municipalityId, tech, hostType):
    db_zabbix = DB_Zabbix()
    statement1 = text(
        f"call sp_hostView('{municipalityId}','{tech}','{hostType}')")
    hosts = db_zabbix.Session().execute(statement1)
    # print(problems)
    data = pd.DataFrame(hosts)
    data = data.replace(np.nan, "")
    hostids = data["hostid"].values.tolist()
    hostids = tuple(hostids)
    statement2 = text(
        f"""
        SELECT hc.correlarionid,
        hc.hostidP,
        hc.hostidC,
        (SELECT location_lat from host_inventory where hostid=hc.hostidP) as init_lat,
        (SELECT location_lon from host_inventory where hostid=hc.hostidP) as init_lon,
        (SELECT location_lat from host_inventory where hostid=hc.hostidC) as end_lat,
        (SELECT location_lon from host_inventory where hostid=hc.hostidC) as end_lon
        from host_correlation hc
        where (SELECT location_lat from host_inventory where hostid=hc.hostidP) IS NOT NULL 
        and
        (
        hc.hostidP in {hostids}
        or hc.hostidC in {hostids})
        """
    )
    corelations = db_zabbix.Session().execute(statement2)
    statement3 = text(
        f"CALL sp_problembySev('{municipalityId}','{tech}','{hostType}')")
    problems_by_sev = db_zabbix.Session().execute(statement3)
    data3 = pd.DataFrame(problems_by_sev).replace(np.nan, "")
    statement4 = text(
        f"call sp_hostAvailPingLoss('{municipalityId}','{tech}','{hostType}')")
    hostAvailables = db_zabbix.Session().execute(statement4)
    data4 = pd.DataFrame(hostAvailables).replace(np.nan, "")
    db_zabbix.Session().close()
    data2 = pd.DataFrame(corelations)
    data2 = data2.replace(np.nan, "")

    response = {"hosts": data.to_dict(
        orient="records"), "relations": data2.to_dict(orient="records"),
        "problems_by_severity": data3.to_dict(orient="records"),
        "host_availables": data4.to_dict(orient="records"),
    }
    # print(response)
    return success_response(data=response)


def get_host_correlation_filter(host_group_id):
    db_zabbix = DB_Zabbix()
    statement = text(
        f"""
        SELECT hc.correlarionid,
        hc.hostidP,
        hc.hostidC,
        (SELECT location_lat from host_inventory where hostid=hc.hostidP) as init_lat,
        (SELECT location_lon from host_inventory where hostid=hc.hostidP) as init_lon,
        (SELECT location_lat from host_inventory where hostid=hc.hostidC) as end_lat,
        (SELECT location_lon from host_inventory where hostid=hc.hostidC) as end_lon
        from host_correlation hc
        where (SELECT location_lat from host_inventory where hostid=hc.hostidP) IS NOT NULL 
        and
        (
        {host_group_id} in (SELECT groupid from hosts_groups hg where hg.hostid=hc.hostidP)
        or {host_group_id} in (SELECT groupid from hosts_groups hg where hg.hostid=hc.hostidC)
        )
        """
    )
    corelations = db_zabbix.Session().execute(statement)
    db_zabbix.Session().close()
    data = pd.DataFrame(corelations)
    data = data.replace(np.nan, "")
    return success_response(data=data.to_dict(orient="records"))
