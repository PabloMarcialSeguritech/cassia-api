from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
from utils.traits import success_response, failure_response
import numpy as np
from paramiko import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import AuthenticationException, BadHostKeyException, SSHException
from cryptography.fernet import Fernet
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
from models.cassia_actions import CassiaAction as CassiaActionModel
from models.interface_model import Interface
from models.cassia_action_log import CassiaActionLog
import socket
from datetime import datetime
import pytz
from fastapi.exceptions import HTTPException
from fastapi import status
from services.cassia.configurations_service import get_configuration
from infraestructure.db_queries_model import DBQueries
import json
import re
import time
from pythonping import ping
from services.zabbix import hosts_service_
from infraestructure.zabbix import AlertsRepository

settings = Settings()


async def get_host_filter(municipalityId, dispId, subtype_id):

    if subtype_id == "0":
        subtype_id = ""
    if dispId == "0":
        dispId = ""
    if dispId == "-1":
        dispId = ''

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
        """ print(alertas_rfid.to_string()) """
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
    print(statement4)
    data4 = pd.DataFrame(hostAvailables).replace(np.nan, "")
    """ dependientes = text(
        f"call sp_diagnostic_problemsD('{municipalityId}','{dispId}')")
    dependientes = pd.DataFrame(
        session.execute(dependientes)).replace(np.nan, '') """
    downs_filtro = await downs_count(municipalityId, dispId, subtype_id, session)
    if not data4.empty:
        print(data4)
        if data4['Down'][0] is None:
            downs_totales = 0
        else:
            downs_totales = int(data4['Down'][0])
        origenes = len(downs_filtro)
        data4['Downs_origen'] = origenes

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
        """ print(statement6) """
    if statement6 != "":
        subgroup_data = session.execute(statement6)
    data6 = pd.DataFrame(subgroup_data).replace(np.nan, "")

    global_host_available = text(
        f"call sp_hostAvailPingLoss('0','{dispId}','')")
    global_host_available = pd.DataFrame(
        session.execute(global_host_available))
    dependientes_global = text(f"call sp_diagnostic_problemsD('0','{dispId}')")
    dependientes_global = pd.DataFrame(
        session.execute(dependientes_global)).replace(np.nan, '')
    downs_global = await downs_count(0, dispId, '', session)
    if not global_host_available.empty:
        downs_totales = int(global_host_available['Down'][0])
        origenes = len(downs_global)
        global_host_available['Downs_origen'] = origenes
        """ global_host_available['Downs_dependientes'] = dependientes_conteo """
    """ print(global_host_available.to_string()) """

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


async def downs_count(municipalityId, dispId, subtype, session):
    severities = "6"
    if subtype == "0":
        subtype = ""

    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
    lpr_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "lpr_id").first()
    lpr_id = "1"
    if lpr_config:
        lpr_id = lpr_config.value
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    """ if tech_host_type == "11":
        tech_host_type = "11,2" """
    """ if subtype != "" and tech_host_type == "":
        tech_host_type = "0" """
    switch_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_id").first()
    switch_id = "12"

    if switch_config:
        switch_id = switch_config.value

    metric_switch_val = "Interface Bridge-Aggregation_: Bits"
    metric_switch = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_throughtput").first()
    if metric_switch:
        metric_switch_val = metric_switch.value
    if subtype == metric_switch_val:
        subtype = ""
    statement = text(
        f"call sp_viewProblem('{municipalityId}','{dispId}','{subtype}','')")

    problems = session.execute(statement)
    data = pd.DataFrame(problems).replace(np.nan, "")
    ping_loss_message = session.query(CassiaConfig).filter(
        CassiaConfig.name == "ping_loss_message").first()
    ping_loss_message = ping_loss_message.value if ping_loss_message else "Unavailable by ICMP ping"
    if not data.empty:
        data['tipo'] = [0 for i in range(len(data))]
        data.loc[data['Problem'] ==
                 ping_loss_message, 'tipo'] = 1
        data['local'] = [0 for i in range(len(data))]
        data['dependents'] = [0 for i in range(len(data))]
        data['alert_type'] = ["" for i in range(len(data))]

    downs_origen = text(
        f"""call sp_diagnostic_problems1('{municipalityId}','{dispId}')""")
    downs_origen = pd.DataFrame(session.execute(downs_origen))
    if not downs_origen.empty:
        """ data['tipo'] = [0 for i in range(len(data))]
        data.loc[data['hostid'].astype(int).isin(
            downs_origen['hostid'].tolist()), 'tipo'] = 1
        data['local'] = [0 for i in range(len(data))]
        data.loc[data['hostid'].astype(int).isin(
            downs_origen['hostid'].tolist()), 'local'] = 0
        data['dependents'] = [0 for i in range(len(data))] """
        data_problems = text(
            """select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_arch_traffic_events_2 cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_2 cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id 
where cate.closed_at is NULL and cate.hostid in :hostids""")
        """select cate.*,cdp.dependents  from cassia_arch_traffic_events cate
left join cassia_diagnostic_problems cdp on cdp.eventid=cate.cassia_arch_traffic_events_id 
where cate.closed_at is NULL and cate.hostid in :hostids """
        """ print(data_problems) """
        data_problems = pd.DataFrame(session.execute(
            data_problems, {'hostids': downs_origen['hostid'].tolist()})).replace(np.nan, 0)

        if not data_problems.empty:
            """ data_problems['TimeRecovery'] = [
                '' for i in range(len(data_problems))] """
            data_problems['r_eventid'] = [
                '' for i in range(len(data_problems))]
            data_problems['Ack'] = [0 for i in range(len(data_problems))]
            """ data_problems['Ack_message'] = [
                '' for i in range(len(data_problems))] """
            data_problems['manual_close'] = [
                1 for i in range(len(data_problems))]
            data_problems['dependents'] = [
                0 for i in range(len(data_problems))]
            data_problems['local'] = [
                1 for i in range(len(data_problems))]
            data_problems['tipo'] = [
                1 for i in range(len(data_problems))]
            data_problems.drop(columns={'updated_at', 'tech_id'}, inplace=True)
            data_problems['created_at'] = pd.to_datetime(
                data_problems['created_at'])
            data_problems["created_at"] = data_problems['created_at'].dt.strftime(
                '%d/%m/%Y %H:%M:%S')
            data_problems.rename(columns={
                'created_at': 'Time',
                'closed_at': 'TimeRecovery',
                'hostname': 'Host',
                'message': 'Problem',
                'status': 'Estatus',
                'cassia_arch_traffic_events_id': 'eventid',
            }, inplace=True)

            if severities != "":
                severities = severities.split(',')
                severities = [int(severity) for severity in severities]
            else:
                severities = [1, 2, 3, 4, 5, 6]
            if 6 in severities:
                downs = data_problems[data_problems['Problem']
                                      == ping_loss_message]
            data_problems = data_problems[data_problems['severity'].isin(
                severities)]
            if 6 in severities:
                data_problems = pd.concat([data_problems, downs],
                                          ignore_index=True).replace(np.nan, "")

            data = pd.concat([data_problems, data],
                             ignore_index=True).replace(np.nan, "")
    dependientes_filtro = text(
        f"call sp_diagnostic_problemsD('{municipalityId}','{dispId}')")
    dependientes_filtro = pd.DataFrame(
        session.execute(dependientes_filtro)).replace(np.nan, '')
    """ host = dependientes_filtro[dependientes_filtro['hostid'] == 16143]
        print(host.to_string())
        print(dependientes_filtro) """
    if not dependientes_filtro.empty:
        indexes = data[data['Problem'] == ping_loss_message]
        indexes = indexes[indexes['hostid'].isin(
            dependientes_filtro['hostid'].to_list())]
        data.loc[data.index.isin(indexes.index.to_list()), 'tipo'] = 0

    sincronizados_totales = text("""select * from cassia_diagnostic_problems_2 cdp 
where cdp.closed_at is NULL""")

    sincronizados_totales = pd.DataFrame(
        session.execute(sincronizados_totales)).replace(np.nan, 0)
    if not sincronizados_totales.empty:
        if not data.empty:
            for ind in data.index:
                if data['Problem'][ind] == ping_loss_message:
                    dependientes = sincronizados_totales[sincronizados_totales['hostid_origen']
                                                         == data['hostid'][ind]]
                    """ print(dependientes) """
                    dependientes['depends_hostid'] = dependientes['depends_hostid'].astype(
                        'int')
                    dependientes = dependientes[dependientes['depends_hostid'] != 0]
                    dependientes = dependientes.drop_duplicates(
                        subset=['depends_hostid'])
                    data.loc[data.index == ind,
                             'dependents'] = len(dependientes)

    if not data.empty:
        now = datetime.now(pytz.timezone('America/Mexico_City'))
        data['fecha'] = pd.to_datetime(data['Time'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
            pytz.timezone('America/Mexico_City'))
        data['diferencia'] = now-data['fecha']
        data['dias'] = data['diferencia'].dt.days
        data['horas'] = data['diferencia'].dt.components.hours
        data['minutos'] = data['diferencia'].dt.components.minutes
        """ print(data['diferencia']) """
        data.loc[data['alert_type'].isin(
            ['rfid', 'lpr']), 'Problem'] = data.loc[data['alert_type'].isin(['rfid', 'lpr']), ['dias', 'horas', 'minutos']].apply(lambda x:
                                                                                                                                  f"Este host no ha tenido lecturas por más de {x['dias']} dias {x['horas']} hrs {x['minutos']} min" if x['dias'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['horas']} hrs {x['minutos']} min" if x['horas'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['minutos']} min", axis=1)
        data = data.drop(columns=['diferencia'])
        data['diferencia'] = data.apply(
            lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
        data.drop_duplicates(
            subset=['hostid', 'Problem'], inplace=True)
        """ print(data.to_string()) """

        """ data['Problem'] = data.apply(lambda x: x['diferencia'] if x['alert_type'] in [
                                     'rfid', 'lpr'] else x['Problem']) """
    if not data.empty:
        data = data[data['Estatus'] == 'PROBLEM']
        if not data.empty:
            data = data[data['tipo'] == 1]
    return data


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
    statement = text(f"""
call sp_hostHealt({host_id});
""")
    print(statement)

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
    data = session.execute(statement)
    data = pd.DataFrame(data).replace(np.nan, "")
    ping_loss_message = session.query(CassiaConfig).filter(
        CassiaConfig.name == "ping_loss_message").first()
    ping_loss_message = ping_loss_message.value if ping_loss_message else "Unavailable by ICMP ping"
    if not data.empty:
        data['local'] = 0
        data['tipo'] = 0
        data.loc[data['Problem'] ==
                 ping_loss_message, 'tipo'] = 1
        data['dependents'] = 0
        data['alert_type'] = ""
    else:
        data = pd.DataFrame(columns=['Time', 'severity', 'hostid', 'Host', 'latitude',
                            'longitude', 'ip', 'Problem', 'Estatus', 'r_eventid', 'Ack_message'])
    data_problems = text(
        f"""select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message  from cassia_arch_traffic_events_2 cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_2 cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id 
where cate.closed_at is NULL and cate.hostid ={host_id} order by cate.created_at desc limit 20
""")
    data_problems = pd.DataFrame(
        session.execute(data_problems)).replace(np.nan, '')

    if not data_problems.empty:
        """ data_problems['TimeRecovery'] = [
            '' for i in range(len(data_problems))] """
        data_problems['r_eventid'] = [
            '' for i in range(len(data_problems))]
        data_problems['Ack'] = [0 for i in range(len(data_problems))]
        """ data_problems['Ack_message'] = [
            '' for i in range(len(data_problems))] """
        data_problems['manual_close'] = [
            1 for i in range(len(data_problems))]
        data_problems['dependents'] = [
            0 for i in range(len(data_problems))]
        data_problems['local'] = [
            1 for i in range(len(data_problems))]
        data_diagnosta = text(
            f"select local_eventid from cassia_diagnostic_problems_2 where hostid={host_id}")
        data_diagnosta = pd.DataFrame(
            session.execute(data_diagnosta)).replace(np.nan, '')
        data_problems['tipo'] = [
            0 for i in range(len(data_problems))]
        data_problems.drop(columns={'updated_at', 'tech_id'}, inplace=True)
        data_problems['created_at'] = pd.to_datetime(
            data_problems['created_at'])
        data_problems["created_at"] = data_problems['created_at'].dt.strftime(
            '%d/%m/%Y %H:%M:%S')
        data_problems.rename(columns={
            'created_at': 'Time',
            'closed_at': 'TimeRecovery',
            'hostname': 'Host',
            'message': 'Problem',
            'status': 'Estatus',
            'cassia_arch_traffic_events_id': 'eventid',
        }, inplace=True)

        data = pd.concat([data_problems, data],
                         ignore_index=True).replace(np.nan, "")
        if not data_diagnosta.empty:
            data.loc[data['eventid'].isin(
                data_diagnosta['local_eventid'].to_list()), 'tipo'] = 1
    dependientes_filtro = text(
        f"call sp_diagnostic_problemsD('0','')")
    dependientes_filtro = pd.DataFrame(
        session.execute(dependientes_filtro)).replace(np.nan, '')
    if not dependientes_filtro.empty:
        indexes = data[data['Problem'] == ping_loss_message]
        indexes = indexes[indexes['hostid'].isin(
            dependientes_filtro['hostid'].to_list())]
        data.loc[data.index.isin(indexes.index.to_list()), 'tipo'] = 0

    sincronizados_totales = text("""select * from cassia_diagnostic_problems_2 cdp 
where cdp.closed_at is NULL""")

    sincronizados_totales = pd.DataFrame(
        session.execute(sincronizados_totales)).replace(np.nan, 0)
    if not sincronizados_totales.empty:
        if not data.empty:
            for ind in data.index:
                if data['Problem'][ind] == ping_loss_message:
                    dependientes = sincronizados_totales[sincronizados_totales['hostid_origen']
                                                         == data['hostid'][ind]]
                    print(dependientes)
                    dependientes['depends_hostid'] = dependientes['depends_hostid'].astype(
                        'int')
                    dependientes = dependientes[dependientes['depends_hostid'] != 0]
                    dependientes = dependientes.drop_duplicates(
                        subset=['depends_hostid'])
                    data.loc[data.index == ind,
                             'dependents'] = len(dependientes)

    if not data.empty:
        now = datetime.now(pytz.timezone('America/Mexico_City'))
        data['fecha'] = pd.to_datetime(data['Time'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
            pytz.timezone('America/Mexico_City'))
        data['diferencia'] = now-data['fecha']
        data['dias'] = data['diferencia'].dt.days
        data['horas'] = data['diferencia'].dt.components.hours
        data['minutos'] = data['diferencia'].dt.components.minutes
        data.loc[data['alert_type'].isin(
            ['rfid', 'lpr']), 'Problem'] = data.loc[data['alert_type'].isin(['rfid', 'lpr']), ['dias', 'horas', 'minutos']].apply(lambda x:
                                                                                                                                  f"Este host no ha tenido lecturas por más de {x['dias']} dias {x['horas']} hrs {x['minutos']} min" if x['dias'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['horas']} hrs {x['minutos']} min" if x['horas'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['minutos']} min", axis=1)
        data = data.drop(columns=['diferencia'])
        data['diferencia'] = data.apply(
            lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
        data = data.sort_values(by='fecha', ascending=False)

    if not data.empty:
        exceptions = await AlertsRepository.get_exceptions(
            data['hostid'].to_list(), session)
        data = pd.merge(data, exceptions, on='hostid',
                        how='left').replace(np.nan, None)
    session.close()
    return success_response(data=data.to_dict(orient="records"))


async def get_host_alerts_(host_id):
    alerts = await AlertsRepository.get_host_alerts(host_id)
    return success_response(data=alerts.to_dict(orient="records"))


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
    query = DBQueries().builder_query_statement_get_host_traffic_by_ip_v2_gto(
        host["ip"].values[0]) if settings.rfid_arcos_gto else DBQueries().builder_query_statement_get_host_traffic_by_ip_v1(host["ip"].values[0])
    statement = text(query)
    db_c5 = DB_C5()
    session_c5 = db_c5.Session()
    arcos = session_c5.execute(statement)
    arcos = pd.DataFrame(arcos).replace(np.nan, "")
    data = pd.DataFrame()
    if len(arcos) > 0:
        data = pd.merge(host, arcos, left_on="ip", right_on="Ip")

    session_c5.close()
    return success_response(data=data.to_dict(orient="records"))


def encrypt(plaintext, key):
    fernet = Fernet(key)
    return fernet.encrypt(plaintext.encode())


def decrypt(encriptedText, key):
    fernet = Fernet(key)
    return fernet.decrypt(encriptedText.encode())


def get_info_actions(ip):
    response = dict()
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_GetInfoAccion('{ip}')")
    print("a")
    aps = session.execute(statement)
    print("b")
    data = pd.DataFrame(aps).replace(np.nan, "")
    response['actions'] = []
    if 'comand' in data.columns:
        data = data.drop(columns=['comand'])
    session.close()

    if not data.empty:
        response['actions'] = data.to_dict(orient='records')

    return success_response(data=response)


def get_credentials(ip):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_getCredentials('{ip}')")
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    session.close()
    return data.to_dict(orient="records")


async def prepare_action(ip, id_action, user_session):
    with DB_Zabbix().Session() as session:
        ping_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == 'ping_id').first()
        if ping_id:
            ping_id = int(ping_id.value)
        else:
            ping_id = 0

    if id_action == ping_id:
        return await hosts_service_.run_action_(ip, id_action, '')
    else:
        db_zabbix = DB_Zabbix()
        session = db_zabbix.Session()
        action = session.query(CassiaActionModel).filter(
            CassiaActionModel.action_id == id_action).first()
        session.close()
        if action is None:
            return failure_response(message="ID acción necesaria")
        dict_credentials_list = get_credentials(ip)
        if dict_credentials_list is None or not dict_credentials_list:
            with DB_Zabbix().Session() as session:
                log = {'action_id': id_action,
                       'clock': datetime.now(pytz.timezone('America/Mexico_City')),
                       'session_id': user_session.session_id.hex,
                       'interface_id': None,
                       'result': 0,
                       'comments': f"Credenciales no encontradas para la ip {ip}"}
                log_register = CassiaActionLog(
                    **log
                )
                session.add(log_register)
                session.commit()
            return failure_response(message="Credenciales no encontradas")

        return run_action(ip, action.comand, get_credentials(ip), action.verification_id, action.action_id,
                          user_session)


def run_action(ip, command, dict_credentials_list, verification_id, action_id, user_session):
    dict_credentials = dict_credentials_list[0]
    ssh_host = ip
    ssh_user = decrypt(dict_credentials['usr'], settings.ssh_key_gen)
    ssh_pass = decrypt(dict_credentials['psswrd'], settings.ssh_key_gen)
    ssh_client = SSHClient()
    ssh_client.set_missing_host_key_policy(AutoAddPolicy())
    data = {
        "action": "false",
        "ip": ip
    }

    with DB_Zabbix().Session() as session:
        try:
            log = {'action_id': action_id,
                   'clock': datetime.now(pytz.timezone('America/Mexico_City')),
                   'session_id': user_session.session_id.hex,
                   'interface_id': dict_credentials['interfaceid'] if (
                       'interfaceid' in dict_credentials) else 1,
                   'result': 0,
                   'comments': None}

            # Check if the command contains the word "ping"
            if "ping" in command:
                # Extract the IP address from the ping command
                ping_ip = extract_ip_from_ping_command(command)
                if ping_ip:
                    data['ip'] = ping_ip
                else:
                    log['result'] = 0
                    log['comments'] = f'Unable to extract IP address from the ping command.'
                    action_log = CassiaActionLog(**log)
                    session.add(action_log)
                    session.commit()
                    print("Unable to extract IP address from the ping command.")
                    return failure_response(message="Comando de ping invalido")

            ssh_client.connect(
                hostname=ssh_host, username=ssh_user.decode(), password=ssh_pass.decode())

            _stdin, _stdout, _stderr = ssh_client.exec_command(command)

            error_lines = _stderr.readlines()
            out = _stdout

            if not error_lines:
                data['action'] = 'true'
                log['result'] = 1
                if "reboot" in command or "shutdown /r /f /t 0" in command:

                    # Esperar al servidor que este offline
                    timeout_offline = 120000  # Ajustar el timeout
                    start_time_offline = time.time()
                    while True:
                        try:
                            response = ping(data['ip'], count=1)
                            if not response.success():
                                offline_time = time.time() - start_time_offline
                                print(
                                    f"Accion reboot - el servidor esta offline. Tiempo fuera de linea: {offline_time} segundos.")
                                break
                        except Exception as e:
                            print(f"Error durante ping: {str(e)}")

                        if time.time() - start_time_offline > timeout_offline:
                            print("Tiempo fuera de linea agotado.")
                            log['result'] = 0
                            log['comments'] = f'Tiempo fuera de linea agotado.'
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message="Verificación de reboot tiempo de espera agotado")

                        # Ajustar el intervalo entre intentos de ping
                        time.sleep(10)

                    # Esperar a que el servidor se encuentre online
                    timeout_online = 120000  # Ajustar el timeout
                    start_time_online = time.time()
                    while True:
                        try:
                            response = ping(data['ip'], count=1)
                            if response.success():
                                online_time = time.time() - start_time_online
                                print(
                                    f"Servidor esta en linea de nuevo. Tiempo online: {online_time} segundos.")
                                data['total_time'] = offline_time + online_time
                                break
                        except Exception as e:
                            print(f"Error durante el ping: {str(e)}")

                        if time.time() - start_time_online > timeout_online:
                            print(
                                "Timeout alcanzado. El servidor no volvio a estar online.")
                            log['result'] = 0
                            log['comments'] = f'Timeout alcanzado. El servidor no volvio a estar online.'
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message="Verificación de reboot tiempo de espera agotado")

                        # Ajustar el intervalo entre intentos de ping
                        time.sleep(10)

                match verification_id:
                    case 0:
                        output_ping = _stdout.read().decode(encoding="utf-8")
                        statistics_match = re.search(
                            r'(\d+ packets transmitted, \d+ received, 100% packet loss, time \d+ms)',
                            output_ping)
                        if statistics_match:
                            data['action'] = 'false'
                            log['result'] = 0
                            log['comments'] = "El ping no tuvo respuesta (100% de pérdida de paquetes)"
                    case 3:
                        data['result'] = get_status(_stdout.read().decode())
                    case 4:
                        data['result'] = check_status(
                            command, ssh_client, 'Started', 'iniciado')
                    case 5:
                        data['result'] = check_status(
                            command, ssh_client, 'Stopped', 'parado')
                    case 6:
                        data['result'] = check_status(
                            command, ssh_client, 'Started', 'reiniciado')
                    case 7:
                        result_response = check_status_sql_server(
                            _stdout, ssh_client)
                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = f'Servicio no instalado o servidor no disponible'
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=f"Problema de conexión al servidor",
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 8:
                        service_name = _stdout.read().decode(encoding="utf-8")
                        result_response = start_stop_sql_server_windows(
                            service_name, ssh_client, 'start', 'RUNNING',
                            "Error al iniciar el servicio. Tiempo limite de espera excedido.")

                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 9:
                        service_name = _stdout.read().decode(encoding="utf-8")
                        result_response = start_stop_sql_server_windows(
                            service_name, ssh_client, 'stop', 'STOPPED',
                            "Error al detener el servicio. Tiempo limite de espera excedido.")

                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 10:
                        service_name = _stdout.read().decode(encoding="utf-8")
                        result_response_stop = start_stop_sql_server_windows(
                            service_name, ssh_client, 'stop', 'STOPPED',
                            "Error al detener el servicio. Tiempo limite de espera excedido.")
                        if not result_response_stop['status']:
                            log['result'] = 0
                            log['comments'] = result_response_stop['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_stop['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        result_response_start = start_stop_sql_server_windows(
                            service_name, ssh_client, 'start', 'RUNNING',
                            "Error al iniciar el servicio. Tiempo limite de espera excedido.")
                        if not result_response_start['status']:
                            log['result'] = 0
                            log['comments'] = result_response_start['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_start['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        data['result'] = "Servicio reiniciado correctamente"
                    case 11:
                        data['result'] = get_status_windows(
                            _stdout.read().decode('utf-8'))
                    case 12:
                        result_response = check_start_stop_windows_service(
                            'GenetecServer', ssh_client, 'start', 'RUNNING',
                            'Error al iniciar el servicio. Tiempo limite de espera excedido.')
                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 13:
                        result_response = check_start_stop_windows_service(
                            'GenetecServer', ssh_client, 'stop', 'STOPPED',
                            'Error al detener el servicio. Tiempo limite de espera excedido.')
                        if not result_response['status']:
                            log['result'] = 0
                            log['comments'] = result_response['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")
                        data['result'] = result_response['result']
                    case 14:
                        result_response_stop = start_stop_windows_service(
                            "GenetecServer", ssh_client, 'stop', 'STOPPED',
                            "Error al detener el servicio. Tiempo limite de espera excedido.")
                        if not result_response_stop['status']:
                            log['result'] = 0
                            log['comments'] = result_response_stop['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_stop['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        result_response_start = start_stop_windows_service(
                            "GenetecServer", ssh_client, 'start', 'RUNNING',
                            "Error al iniciar el servicio. Tiempo limite de espera excedido.")
                        if not result_response_start['status']:
                            log['result'] = 0
                            log['comments'] = result_response_start['message_error']
                            action_log = CassiaActionLog(**log)
                            session.add(action_log)
                            session.commit()
                            return failure_response(message=result_response_start['message_error'],
                                                    recommendation="revisar que tenga conexión estable a la dirección del servidor y que el servidor tenga el servicio instalado")

                        data['result'] = "Servicio reiniciado correctamente"

                action_log = CassiaActionLog(**log)
                session.add(action_log)
                session.commit()
                return success_response(data=data)
            else:
                error_msg = " ".join(error_lines)
                if "service could not be found" in error_msg:
                    return failure_response(message=f"Servicio no encontrado",
                                            recommendation="revisar que el servidor tenga disponible el servicio")
                print(
                    f"Problema de conexión al servidor. Detalles: {error_msg}")
                log['comments'] = error_msg
                action_log = CassiaActionLog(**log)
                session.add(action_log)
                session.commit()
                return failure_response(message=f"Problema de conexión al servidor",
                                        recommendation="revisar que tenga conexión estable ala dirección del servidor")

        except BadHostKeyException as e:

            error_msg = f"Error de clave de host"
            print(f"Error de clave de host: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg)

        except AuthenticationException as e:

            error_msg = "Error de autenticación"
            print(f"Error de autenticación: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg,
                                    recommendation="revisar que estén correctas las credenciales ssh")

        except SSHException as e:

            error_msg = "Problema de conexión por SSH"
            print(f"Problema de conexión por SSH: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg)

        except socket.error as e:

            error_msg = "Error de conexión de socket"
            print(f"Error de conexión de socket: {str(e)}")
            log['comments'] = error_msg
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=error_msg)
        except Exception as e:
            print(e)
            log['comments'] = f'Error desconocido: {e}'
            action_log = CassiaActionLog(**log)
            session.add(action_log)
            session.commit()
            return failure_response(message=e)
        finally:

            ssh_client.close()


def get_credentials_for_proxy(ip):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_proxy_credential('{ip}')")
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    session.close()
    return data.to_dict(orient="records")


def extract_ip_from_ping_command(command):
    # Use regular expression to find the IP address in the ping command
    match = re.search(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', command)
    if match:
        return match.group()
    else:
        return None


def check_status(command, ssh_client, command_verification, accion):
    service = command.split()
    service = service[-1]
    _stdin, _stdout, _stderr = ssh_client.exec_command(
        f"systemctl status {service}")
    result_status = _stdout.read().decode()
    result_status = result_status.splitlines()
    result_status = result_status[-1]
    if command_verification in result_status:
        return f"Servicio {service} {accion} correctamente"
    else:
        return "Accion no realizada correctamente"


def get_status(cadena):
    lineas = cadena.splitlines()
    result = ""
    for linea in lineas:
        if "Active" in linea:
            if "running" in linea:
                result = "Servicio activo (El servicio está en ejecución y funcionando normalmente)"
            if "inactive" in linea:
                result = "Servicio inactivo (El servicio no está en ejecución y no se puede iniciar)"
            if "activating" in linea:
                result = "Activando servicio (El servicio está en proceso de iniciar)"
            if "deactivating" in linea:
                result = "Desactivando servicio (El servicio está en proceso de detenerse)"
            if "failed" in linea:
                result = "Fallo en el servicio (El servicio ha fallado al iniciar o ha sido detenido debido a un error)"
            if "exited" in linea:
                result = "Servicio finalizado (El servicio se ha detenido de manera normal después de haber estado en ejecución)"
            if "listening" in linea:
                result = "Escuchando servicio (El servicio está escuchando y esperando conexiones)"
            if "plugged" in linea:
                result = "Servicio enchufado (Indica que el servicio está enchufado en el socket de activación)"
    return result


def get_status_windows(cadena):
    lineas = cadena.splitlines()
    result = ""
    for linea in lineas:
        if "STATE" in linea:
            if "RUNNING" in linea:
                result = "Servicio activo (El servicio está en ejecución)"
            if "STOPPED" in linea:
                result = "Servicio inactivo (El servicio no está en ejecución)"
            if "PAUSED" in linea:
                result = "El servicio está pausado."
            if "START_PENDING" in linea:
                result = "El servicio está en proceso de inicio"
            if "STOP_PENDING" in linea:
                result = "El servicio está en proceso de detención"
            if "CONTINUE_PENDING" in linea:
                result = "El servicio está en proceso de reanudación después de estar pausado."
            if "PAUSE_PENDING" in linea:
                result = "El servicio está en proceso de pausa."
    return result


def check_status_sql_server(out, ssh_client):
    service_name = out.read().decode(encoding="utf-8")
    result_response = {'status': 0, 'result': '', 'message_error': ''}
    if len(service_name) <= 0:
        result_response["message_error"] = 'El servicio no existe en este host'
        return result_response
    service_name = service_name[14:]

    comando = f"sc query {service_name}"
    _stdin, _stdout, _stderr = ssh_client.exec_command(comando)
    error_lines = _stderr.readlines()
    """ sc query type= service state= all | findstr /C:"MSSQL" """
    if not error_lines:
        result_response['status'] = 1
        result = _stdout.read().decode(encoding="utf-8")
        result = result.encode().decode(
            'unicode_escape')
        result = get_status_windows(result)
        result_response['result'] = result
        return result_response
    else:
        error_msg = " ".join(error_lines)
        result_response['message_error'] = error_msg
        print(
            f"Problema de conexión al servidor. Detalles: {error_msg}")
        return result_response


def start_stop_sql_server_windows(service_name, ssh_client, comand_st, verification_status, error_message):
    result_response = {'status': 0, 'result': '', 'message_error': ''}
    if len(service_name) <= 0:
        result_response["message_error"] = 'El servicio no existe en este host'
        return result_response
    service_name = service_name[14:]

    comando = f"sc {comand_st} {service_name}"
    _stdin, _stdout, _stderr = ssh_client.exec_command(comando)

    error_lines = _stderr.readlines()

    if not error_lines:

        cont = 0
        while True:

            comand = f"sc query {service_name}"

            _stdin, _stdout, _stderr = ssh_client.exec_command(comand)
            out = _stdout.read().decode(encoding="utf-8")

            status = check_status_windows(
                out, verification_status)

            if status:
                break
            time.sleep(2)
            cont += 1
            if cont > 120:
                result_response['status'] = 0
                result_response['message_error'] = error_message
                return result_response

        result_response['status'] = 1

        match comand_st:
            case 'stop':

                result_response['result'] = "El servicio se ha detenido correctamente"
            case 'start':
                result_response['result'] = "El servicio se ha iniciado correctamente"

        return result_response
    else:
        error_msg = " ".join(error_lines)
        result_response['message_error'] = error_msg
        print(
            f"Problema de conexión al servidor. Detalles: {error_msg}")
        return result_response


def start_stop_windows_service(service_name, ssh_client, comand_st, verification_status, error_message):
    result_response = {'status': 0, 'result': '', 'message_error': ''}

    comando = f"sc {comand_st} {service_name}"
    _stdin, _stdout, _stderr = ssh_client.exec_command(comando)

    error_lines = _stderr.readlines()

    if not error_lines:

        cont = 0
        while True:

            comand = f"sc query {service_name}"

            _stdin, _stdout, _stderr = ssh_client.exec_command(comand)
            out = _stdout.read().decode(encoding="utf-8")

            status = check_status_windows(
                out, verification_status)

            if status:
                break
            time.sleep(2)
            cont += 1
            if cont > 120:
                result_response['status'] = 0
                result_response['message_error'] = error_message
                return result_response

        result_response['status'] = 1

        match comand_st:
            case 'stop':

                result_response['result'] = "El servicio se ha detenido correctamente"
            case 'start':
                result_response['result'] = "El servicio se ha iniciado correctamente"

        return result_response
    else:
        error_msg = " ".join(error_lines)
        result_response['message_error'] = error_msg
        print(
            f"Problema de conexión al servidor. Detalles: {error_msg}")
        return result_response


def check_status_windows(cadena, status):
    lineas = cadena.splitlines()
    result = False
    for linea in lineas:
        if "STATE" in linea:
            if status in linea:
                return True
    return result


def check_start_stop_windows_service(service_name, ssh_client, comand_st, verification_status, error_message):
    result_response = {'status': 0, 'result': '', 'message_error': ''}
    cont = 0
    while True:
        comand = f"sc query {service_name}"
        _stdin, _stdout, _stderr = ssh_client.exec_command(comand)
        status = check_status_windows(
            _stdout.read().decode(encoding="utf-8"), verification_status)
        if status:
            break
        time.sleep(2)
        cont += 1
        error_lines = _stderr.readlines()
        if cont > 120:
            result_response['status'] = 0
            result_response['message_error'] = error_message
            return result_response
        if error_lines:
            result_response['status'] = 0
            result_response['message_error'] = error_lines
            return result_response

    result_response['status'] = 1

    match comand_st:
        case 'stop':
            result_response['result'] = "El servicio se ha detenido correctamente"
        case 'start':
            result_response['result'] = "El servicio se ha iniciado correctamente"

    return result_response
