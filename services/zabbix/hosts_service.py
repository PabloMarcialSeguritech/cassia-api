from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from utils.traits import success_response
import numpy as np
settings = Settings()


def get_host_filter(municipalityId, dispId, subtype_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    """ if subtype_id == "376276" or subtype_id == "375090":
        subtype_host_filter = '376276,375090'
    else:
        subtype_host_filter = subtype_id """
    if dispId == "11":
        dispId_filter = "11,2"
    else:
        dispId_filter = dispId
    statement1 = text(
        f"call sp_hostView('{municipalityId}','{dispId}','{subtype_id}')")
    if dispId == "11":
        statement1 = text(
            f"call sp_hostView('{municipalityId}','{dispId},2','')")
    hosts = session.execute(statement1)

    # print(problems)
    data = pd.DataFrame(hosts)
    data = data.replace(np.nan, "")

    if len(data) > 1:
        hostids = tuple(data['hostid'].values.tolist())
    else:
        if len(data) == 1:
            hostids = f"({data['hostid'][0]})"
        else:
            hostids = "(0)"

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
        and hc.hostidC in {hostids})
        """
    )
    corelations = session.execute(statement2)
    statement3 = text(
        f"CALL sp_problembySev('{municipalityId}','{dispId_filter}','{subtype_id}')")
    problems_by_sev = session.execute(statement3)
    data3 = pd.DataFrame(problems_by_sev).replace(np.nan, "")
    statement4 = text(
        f"call sp_hostAvailPingLoss('{municipalityId}','{dispId}','')")
    hostAvailables = session.execute(statement4)
    data4 = pd.DataFrame(hostAvailables).replace(np.nan, "")
    data2 = pd.DataFrame(corelations)
    data2 = data2.replace(np.nan, "")
    # aditional data
    subgroup_data = []
    """ statement5 = ""
    statement5 = text(
        f"CALL sp_viewAlignment('{municipalityId}','11','376276,375090')")
    subgroup_data = db_zabbix.Session().execute(statement5)
    data5 = pd.DataFrame(subgroup_data).replace(np.nan, "")
    if data5.empty:
        data5["hostid"] = [0]
        data5["Alineacion"] = [0]
    alineaciones = data5[["hostid", "Alineacion"]]
    nuevo = data2
    if not data2.empty:
        nuevo = data2.merge(alineaciones, left_on="hostidC",
                            right_on="hostid", how="left").replace(np.nan, 0) """
    statement6 = ""
    if subtype_id != "":
        statement6 = text(
            f"CALL sp_viewAlignment('{municipalityId}','{dispId}','{subtype_id}')")
    if statement6 != "":
        subgroup_data = session.execute(statement6)
    data6 = pd.DataFrame(subgroup_data).replace(np.nan, "")
    print(data6.shape[0])
    global_host_available = text(
        f"call sp_hostAvailPingLoss('0','{dispId}','')")
    global_host_available = pd.DataFrame(
        session.execute(global_host_available))
    session.close()
    response = {"hosts": data.to_dict(
        orient="records"), "relations": data2.to_dict(orient="records"),
        "problems_by_severity": data3.to_dict(orient="records"),
        "host_availables": data4.to_dict(orient="records",),
        "subgroup_info": data6.to_dict(orient="records"),
        "global_host_availables": global_host_available.to_dict(orient="records")
    }
    # print(response)
    return success_response(data=response)


def get_host_correlation_filter(host_group_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
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
    corelations = session.execute(statement)
    session.close()
    data = pd.DataFrame(corelations)
    data = data.replace(np.nan, "")
    return success_response(data=data.to_dict(orient="records"))


async def get_host_metrics(host_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select DISTINCT i.templateid,i.itemid  from items i where hostid={host_id} AND i.templateid IS NOT NULL")
    template_ids = session.execute(statement)
    template_ids = pd.DataFrame(template_ids).replace(np.nan, "")

    if len(template_ids) > 1:
        item_ids = pd.DataFrame(template_ids['itemid'])
        template_ids = tuple(template_ids['templateid'].values.tolist())

    else:
        if len(template_ids) == 1:
            item_ids = pd.DataFrame(template_ids['itemid'])
            template_ids = f"({template_ids['templateid'][0]})"
        else:
            template_ids = "(0)"
            item_ids = pd.DataFrame()

    statement = text(f"""
    SELECT h.hostid,i.itemid, i.templateid,i.name,
from_unixtime(vl.clock,'%d/%m/%Y %H:%i:%s')as Date,
vl.value as Metric FROM hosts h
INNER JOIN items i ON (h.hostid  = i.hostid)
INNER JOIN  vw_lastValue_history vl  ON (i.itemid=vl.itemid)
WHERE  h.hostid = {host_id} AND i.templateid in {template_ids}
""")
    metrics = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    session.close()
    return success_response(data=metrics.to_dict(orient="records"))


async def get_host_alerts(host_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"""
SELECT from_unixtime(p.clock,'%d/%m/%Y %H:%i:%s' ) as Time,
	p.severity,h.hostid,h.name Host,hi.location_lat as latitude,hi.location_lon as longitude,
	it.ip,p.name Problem, IF(ISNULL(p.r_eventid),'PROBLEM','RESOLVED') Estatus, p.eventid,p.r_eventid,
	IF(p.r_clock=0,'',From_unixtime(p.r_clock,'%d/%m/%Y %H:%i:%s' ) )'TimeRecovery',
	p.acknowledged Ack,IFNULL(a.Message,'''') AS Ack_message FROM hosts h	
	INNER JOIN host_inventory hi ON (h.hostid=hi.hostid)
	INNER JOIN interface it ON (h.hostid=it.hostid)
	INNER JOIN items i ON (h.hostid=i.hostid)
	INNER JOIN functions f ON (i.itemid=f.itemid)
	INNER JOIN triggers t ON (f.triggerid=t.triggerid)
	INNER JOIN problem p ON (t.triggerid = p.objectid)
	LEFT JOIN acknowledges a ON (p.eventid=a.eventid)
	WHERE  h.hostid={host_id}
	ORDER BY p.clock  desc 
    limit 20
""")

    alerts = session.execute(statement)
    alerts = pd.DataFrame(alerts).replace(np.nan, "")

    session.close()
    return success_response(data=alerts.to_dict(orient="records"))
