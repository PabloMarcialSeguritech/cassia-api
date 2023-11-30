from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
from utils.traits import success_response
import numpy as np
from paramiko import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import AuthenticationException, BadHostKeyException, SSHException
from cryptography.fernet import Fernet
from models.interface_model import Interface as InterfaceModel
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
import socket

from fastapi.exceptions import HTTPException
from fastapi import status

settings = Settings()


def get_host_filter(municipalityId, dispId, subtype_id):
    if subtype_id == "0":
        subtype_id = ""
    if dispId == "0":
        dispId = ""

    print(subtype_id)

    print(type(subtype_id))
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    switch_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_id").first()
    switch_id = "12"
    switch_troughtput = False
    if switch_config:
        switch_id = switch_config.value

    metric_switch_val = "Interface Bridge-Aggregation_: Bits"
    metric_switch = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_throughtput").first()
    if metric_switch:
        metric_switch_val = metric_switch.value
    if subtype_id == metric_switch_val:
        subtype_id = ""

        if dispId == switch_id:
            switch_troughtput = True

    """ if subtype_id == "376276" or subtype_id == "375090":
        subtype_host_filter = '376276,375090'
    else:
        subtype_host_filter = subtype_id """
    """ arcos_band = False """
    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
    if subtype_id == "3":
        subtype_id = ""
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
    if dispId == rfid_id:
        if municipalityId == '0':
            alertas_rfid = session.query(CassiaArchTrafficEvent).filter(
                CassiaArchTrafficEvent.closed_at == None,
            ).all()

        else:
            statement = text("call sp_catCity()")
            municipios = session.execute(statement)
            municipios = pd.DataFrame(municipios).replace(np.nan, "")

            municipio = municipios.loc[municipios['groupid'].astype(str) ==
                                       municipalityId]
            if not municipio.empty:
                municipio = municipio['name'].item()
            else:
                municipio = ''

            alertas_rfid = session.query(CassiaArchTrafficEvent).filter(
                CassiaArchTrafficEvent.closed_at == None,
                CassiaArchTrafficEvent.municipality == municipio
            ).all()
        alertas_rfid = pd.DataFrame([(
            r.severity,
        ) for r in alertas_rfid], columns=['severity'])
        print(alertas_rfid.to_string())
        alertas_rfid = alertas_rfid.groupby(
            ['severity'])['severity'].count().rename_axis('Severities').reset_index()
        alertas_rfid.rename(
            columns={'severity': 'Severities', 'Severities': 'severity'}, inplace=True)
        problems_count = pd.concat([data3, alertas_rfid], ignore_index=True)
        data3 = problems_count.groupby(
            ['severity']).sum().reset_index()

    statement4 = text(
        f"call sp_hostAvailPingLoss('{municipalityId}','{dispId}','')")
    hostAvailables = session.execute(statement4)
    data4 = pd.DataFrame(hostAvailables).replace(np.nan, "")
    data2 = pd.DataFrame(corelations)
    data2 = data2.replace(np.nan, "")
    # aditional data
    subgroup_data = []

    statement6 = ""
    if subtype_id != "":
        statement6 = text(
            f"CALL sp_MetricViewH('{municipalityId}','{dispId}','{subtype_id}')")
    if switch_troughtput:
        statement6 = text(
            f"call sp_switchThroughtput('{municipalityId}','{switch_id}','{metric_switch_val}')")
        print(statement6)
    if statement6 != "":
        subgroup_data = session.execute(statement6)
    data6 = pd.DataFrame(subgroup_data).replace(np.nan, "")

    global_host_available = text(
        f"call sp_hostAvailPingLoss('0','{dispId}','')")
    global_host_available = pd.DataFrame(
        session.execute(global_host_available))
    session.close()
    response = {"hosts": data.to_dict(
        orient="records"), "relations": data2.to_dict(orient="records"),
        "problems_by_severity": data3.to_dict(orient="records"),
        "host_availables": data4.to_dict(orient="records", ),
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
vl.value as Metric  FROM hosts h
INNER JOIN items i ON (h.hostid  = i.hostid)
INNER JOIN  vw_lastValue_history vl  ON (i.itemid=vl.itemid)
WHERE  h.hostid = {host_id} AND i.templateid in {template_ids}
UNION
SELECT h.hostid,i.itemid, i.templateid,i.name,
from_unixtime(vl.clock,'%d/%m/%Y %H:%i:%s')as Date,
vl.value as Metric  FROM hosts h
INNER JOIN items i ON (h.hostid  = i.hostid)
INNER JOIN  vw_lastValue_history_uint vl  ON (i.itemid=vl.itemid)
WHERE  h.hostid = {host_id} AND (i.name like 'Interface Bridge-Aggregation_: Speed'
OR i.name like 'Interface Bridge-Aggregation_: Bits%')
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
    alertas_rfid = session.query(CassiaArchTrafficEvent).filter(
        CassiaArchTrafficEvent.closed_at == None,
        CassiaArchTrafficEvent.hostid == host_id
    ).all()
    alertas_rfid = pd.DataFrame([(
        r.created_at,
        r.severity,
        r.hostid,
        r.hostname,
        r.latitude,
        r.longitude,
        r.ip,
        r.message,
        r.status,
        r.cassia_arch_traffic_events_id,
        '',
        '',
        0,
        ''
    )
        for r in alertas_rfid], columns=['Time', 'severity', 'hostid',
                                         'Host', 'latitude', 'longitude',
                                                 'ip',
                                                 'Problem', 'Estatus',
                                                 'eventid',
                                                 'r_eventid',
                                                 'TimeRecovery',
                                                 'Ack',
                                                 'Ack_message'])
    data = pd.concat([alertas_rfid, alerts],
                     ignore_index=True).replace(np.nan, "")

    session.close()
    return success_response(data=data.to_dict(orient="records"))


async def get_host_arcos(host_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"""
SELECT  DISTINCT h.hostid,h.name Host,hi.location_lat as latitude,hi.location_lon as longitude,
	it.ip FROM hosts h	
	INNER JOIN host_inventory hi ON (h.hostid=hi.hostid)
	INNER JOIN interface it ON (h.hostid=it.hostid)
	INNER JOIN items i ON (h.hostid=i.hostid)
	WHERE h.hostid ={host_id}
""")
    host = session.execute(statement)
    host = pd.DataFrame(host).replace(np.nan, "")
    session.close()
    if len(host) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Host id not exist"
        )
    statement = text(f"""
SELECT m.Nombre as Municipio, a.Nombre as Arco, r.Descripcion,
r.Estado, a2.UltimaLectura,
ISNULL(cl.lecturas,0)  as Lecturas,
a.Longitud,a.Latitud,
a2.Carril,
r.Ip 
FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
where r.Ip = '{host["ip"].values[0]}'
order by a.Longitud,a.Latitud
""")
    db_c5 = DB_C5()
    session_c5 = db_c5.Session()
    arcos = session_c5.execute(statement)
    arcos = pd.DataFrame(arcos).replace(np.nan, "")
    data = pd.DataFrame()
    if len(arcos) > 0:
        data = pd.merge(host, arcos, left_on="ip", right_on="Ip")

    session_c5.close()
    return success_response(data=data.to_dict(orient="records"))


def reboot(hostid):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    interface = session.query(InterfaceModel).filter(
        InterfaceModel.hostid == hostid).first()
    if interface is None:
        return success_response(message="Host no encontrado")
    else:
        ssh_host = settings.ssh_host_client
        ssh_user = decrypt(settings.ssh_user_client, settings.ssh_key_gen)
        ssh_pass = decrypt(settings.ssh_pass_client, settings.ssh_key_gen)
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        data = {
            "reboot": "false",
            "hostid": str(interface.hostid),
            "ip": interface.ip
        }
        try:
            ssh_client.connect(
                hostname=ssh_host, username=ssh_user.decode(), password=ssh_pass.decode())
            _stdin, _stdout, _stderr = ssh_client.exec_command("reboot")
            error_lines = _stderr.readlines()
            if not error_lines:
                data['reboot'] = 'true'
            else:
                print(error_lines)
        except (BadHostKeyException, AuthenticationException, SSHException, socket.error) as e:
            print(e)
        return success_response(data=data)


def encrypt(plaintext, key):
    fernet = Fernet(key)
    return fernet.encrypt(plaintext.encode())


def decrypt(encriptedText, key):
    fernet = Fernet(key)
    return fernet.decrypt(encriptedText.encode())
