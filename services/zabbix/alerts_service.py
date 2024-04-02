from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_Prueba
from sqlalchemy import text
from fastapi.responses import JSONResponse, FileResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from models.cassia_exception_agency import CassiaExceptionAgency
from models.cassia_exceptions import CassiaExceptions
from models.problem_record import ProblemRecord
from models.problem_records_history import ProblemRecordHistory
from models.cassia_tickets import CassiaTicket as CassiaTicketModel
from models.cassia_acknowledge import CassiaAcknowledge
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
import schemas.cassia_ticket_schema as cassia_ticket_schema
import numpy as np
from datetime import datetime
from utils.traits import success_response
from fastapi import status, File, UploadFile
from fastapi.responses import FileResponse
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
from models.cassia_arch_traffic_events_2 import CassiaArchTrafficEvent2
from models.cassia_event_acknowledges import CassiaEventAcknowledge
from infraestructure.zabbix import AlertsRepository
import os
import ntpath
import shutil
import pytz
import pyzabbix
from pyzabbix.api import ZabbixAPI
import tempfile
import os
import ntpath
settings = Settings()


def process_alerts_local(data, municipalityId, session, tech_id, severities, tipo):
    if municipalityId == '0':
        alertas = session.query(CassiaArchTrafficEvent).filter(
            CassiaArchTrafficEvent.closed_at == None,
            CassiaArchTrafficEvent.tech_id == tech_id
        ).all()
        alertas = pd.DataFrame([(
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
            '',
            0,
            tipo,
            1,
            0
        )
            for r in alertas], columns=['Time', 'severity', 'hostid',
                                        'Host', 'latitude', 'longitude',
                                        'ip',
                                        'Problem', 'Estatus',
                                        'eventid',
                                        'r_eventid',
                                        'TimeRecovery',
                                        'Ack',
                                        'Ack_message',
                                        "manual_close", 'alert_type', 'local', 'dependents'])
        if not alertas.empty:
            alertas['Time'] = pd.to_datetime(alertas['Time'])
            alertas["Time"] = alertas['Time'].dt.strftime(
                '%d/%m/%Y %H:%M:%S')
            if severities != "":
                severities = severities.split(',')
                severities = [int(severity) for severity in severities]
            else:
                severities = [1, 2, 3, 4, 5]
            alertas = alertas[alertas['severity'].isin(
                severities)]

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
        alertas = session.query(CassiaArchTrafficEvent).filter(
            CassiaArchTrafficEvent.closed_at == None,
            CassiaArchTrafficEvent.municipality == municipio,
            CassiaArchTrafficEvent.tech_id == tech_id
        ).all()
        alertas = pd.DataFrame([(
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
            '',
            0,
            tipo,
            1,
            0
        )
            for r in alertas], columns=['Time', 'severity', 'hostid',
                                        'Host', 'latitude', 'longitude',
                                        'ip',
                                        'Problem', 'Estatus',
                                        'eventid',
                                        'r_eventid',
                                        'TimeRecovery',
                                        'Ack',
                                        'Ack_message',
                                        "manual_close", 'alert_type', 'local', 'dependents'])
        if not alertas.empty:
            alertas['Time'] = pd.to_datetime(alertas['Time'])
            alertas["Time"] = alertas['Time'].dt.strftime(
                '%d/%m/%Y %H:%M:%S')
            if severities != "":
                severities = severities.split(',')
                severities = [int(severity) for severity in severities]
            else:
                severities = [1, 2, 3, 4, 5]
            alertas = alertas[alertas['severity'].isin(
                severities)]
    if not alertas.empty:

        acks = text("""select cea.eventid , cea.message as message from (
select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) ceaa
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid""")
        acks = pd.DataFrame(session.execute(acks)).replace(np.nan, '')
        if not acks.empty:
            alertas = pd.merge(alertas, acks, left_on='eventid',
                               right_on='eventid', how='left')
            alertas.drop(columns=['Ack_message'], inplace=True)
            alertas.rename(columns={'message': 'Ack_message'}, inplace=True)
    data = pd.concat([alertas, data],
                     ignore_index=True).replace(np.nan, "")
    return data


async def get_problems_filter(municipalityId, tech_host_type=0, subtype="", severities=""):

    if subtype == "0":
        subtype = ""
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
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
    ping_loss_message = session.query(CassiaConfig).filter(
        CassiaConfig.name == "ping_loss_message").first()
    ping_loss_message = "Unavailable by ICMP ping"
    if ping_loss_message:
        ping_loss_message = ping_loss_message.value
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    """ if tech_host_type == "11":
        tech_host_type = "11,2" """
    if subtype != "" and tech_host_type == "":
        tech_host_type = "0"
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
        f"call sp_viewProblem('{municipalityId}','{tech_host_type}','{subtype}','{severities}')")

    problems = session.execute(statement)
    data = pd.DataFrame(problems).replace(np.nan, "")

    if not data.empty:
        data['tipo'] = [0 for i in range(len(data))]
        data.loc[data['Problem'] ==
                 ping_loss_message, 'tipo'] = 1
        data['local'] = [0 for i in range(len(data))]
        data['dependents'] = [0 for i in range(len(data))]
        data['alert_type'] = ["" for i in range(len(data))]

    if tech_host_type == lpr_id:
        data = process_alerts_local(
            data, municipalityId, session, lpr_id, severities, 'lpr')
    if tech_host_type == rfid_id:
        data = process_alerts_local(
            data, municipalityId, session, rfid_id, severities, 'rfid')
    downs_origen = text(
        f"""call sp_diagnostic_problems1('{municipalityId}','{tech_host_type}')""")
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
                0 for i in range(len(data_problems))]
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
        f"call sp_diagnostic_problemsD('{municipalityId}','{tech_host_type}')")
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
        exceptions = await AlertsRepository.get_exceptions(
            data['hostid'].to_list(), session)
        data = pd.merge(data, exceptions, on='hostid',
                        how='left').replace(np.nan, None)
    session.close()

    return success_response(data=data.to_dict(orient="records"))


async def get_problems_filter_report(municipalityId, tech_host_type=0, subtype="", severities=""):
    with DB_Zabbix().Session() as session:
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
        if subtype != "" and tech_host_type == "":
            tech_host_type = "0"
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
            f"call sp_viewProblem('{municipalityId}','{tech_host_type}','{subtype}','{severities}')")

        problems = session.execute(statement)
        data = pd.DataFrame(problems).replace(np.nan, "")
        ping_loss_message = session.query(CassiaConfig).filter(
            CassiaConfig.name == "ping_loss_message").first()
        ping_loss_message = "Unavailable by ICMP ping"
        if ping_loss_message:
            ping_loss_message = ping_loss_message.value
        if not data.empty:
            data['tipo'] = [0 for i in range(len(data))]
            data.loc[data['Problem'] ==
                     ping_loss_message, 'tipo'] = 1
            data['local'] = [0 for i in range(len(data))]
            data['dependents'] = [0 for i in range(len(data))]
            data['alert_type'] = ["" for i in range(len(data))]
        if tech_host_type == lpr_id:
            data = process_alerts_local(
                data, municipalityId, session, lpr_id, severities, 'lpr')
        if tech_host_type == rfid_id:
            data = process_alerts_local(
                data, municipalityId, session, rfid_id, severities, 'rfid')
        downs_origen = text(
            f"""call sp_diagnostic_problems1('{municipalityId}','{tech_host_type}')""")
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
            print(data_problems)
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
                    0 for i in range(len(data_problems))]
                data_problems['dependents'] = [
                    0 for i in range(len(data_problems))]
                data_problems['local'] = [
                    1 for i in range(len(data_problems))]
                data_problems['tipo'] = [
                    1 for i in range(len(data_problems))]
                data_problems.drop(
                    columns={'updated_at', 'tech_id'}, inplace=True)
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
            f"call sp_diagnostic_problemsD('{municipalityId}','{tech_host_type}')")
        dependientes_filtro = pd.DataFrame(
            session.execute(dependientes_filtro)).replace(np.nan, '')
        if not dependientes_filtro.empty:
            indexes = data[data['Problem'] ==
                           ping_loss_message]
            indexes = indexes[indexes['hostid'].isin(
                dependientes_filtro['hostid'].to_list())]
            data.loc[data.index.isin(
                indexes.index.to_list()), 'tipo'] = 0
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
            print("a")
            data['fecha'] = pd.to_datetime(data['Time'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
                pytz.timezone('America/Mexico_City'))
            print("b")
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
            data.drop_duplicates(
                subset=['hostid', 'Problem'], inplace=True)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
                xlsx_filename = temp_file.name
                with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:
                    data = data.sort_values(by='fecha', ascending=False)
                    data = data.drop(columns=['diferencia', 'fecha'])
                    data.to_excel(
                        writer, sheet_name='Data', index=False)
        else:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
                xlsx_filename = temp_file.name
                with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:
                    data.to_excel(
                        writer, sheet_name='Data', index=False)

    return FileResponse(xlsx_filename, headers={"Content-Disposition": "attachment; filename=alertas.xlsx"}, media_type="application/vnd.ms-excel", filename="alertas.xlsx")


""" Exception Agencies """


""" Exception Agencies """


async def get_exception_agencies():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"SELECT * FROM cassia_exception_agencies where deleted_at IS NULL")
    rows = session.execute(statement)
    session.close()
    rows = pd.DataFrame(rows).replace(np.nan, "")
    if len(rows) > 0:
        rows["id"] = rows["exception_agency_id"]
    return success_response(data=rows.to_dict(orient="records"))


def create_exception_agency(exception_agency: exception_agency_schema.CassiaExceptionAgencyBase):
    db_zabbix = DB_Zabbix()
    exception_agency_new = CassiaExceptionAgency(
        name=exception_agency.name,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )
    session = db_zabbix.Session()
    session.add(exception_agency_new)
    session.commit()
    session.refresh(exception_agency_new)
    session.close()
    db_zabbix.stop()
    return success_response(message="Registro guardado correctamente",
                            success=True,
                            data=exception_agency_schema.ExceptionAgency(
                                exception_agency_id=exception_agency_new.exception_agency_id,
                                name=exception_agency_new.name,
                                created_at=exception_agency_new.created_at,
                                updated_at=exception_agency_new.updated_at,
                                deleted_at=exception_agency_new.deleted_at
                            ),
                            status_code=status.HTTP_201_CREATED)


def update_exception_agency(exception_agency_id: int, exception_agency: exception_agency_schema.CassiaExceptionAgencyBase):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(CassiaExceptionAgency).filter(
        CassiaExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    if exception_agency_search.name == exception_agency.name:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Update at least one field"
        )
    exception_agency_search.name = exception_agency.name
    exception_agency_search.updated_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Updated",
                            data=exception_agency_search)


def delete_exception_agency(exception_agency_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(CassiaExceptionAgency).filter(
        CassiaExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    exception_agency_search.deleted_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Deleted")


""" Exceptions """


async def get_exceptions():
    with DB_Zabbix().Session() as session:
        try:
            rows = session.query(CassiaExceptions).all()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal error : {e}"
            )
    return success_response(data=rows)


async def create_exception(exception: exception_schema.CassiaExceptionsBase, current_user_session):
    with DB_Zabbix().Session() as session:
        query = text("SELECT hostid from hosts where hostid = :hostid")
        host = session.execute(query, {'hostid': exception.hostid})
        if host.fetchone() is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"El host con el id proporcionado no existe"
            )
        exception_dict = exception.dict()
        exception_dict['session_id'] = current_user_session
        exception_dict['closed_at'] = None
        new_exception = CassiaExceptions(**exception_dict)
        session.add(new_exception)
        session.commit()
        session.refresh(new_exception)
    return success_response(message="Excepcion creada correctamente",
                            data=new_exception,
                            status_code=status.HTTP_201_CREATED)


async def close_exception(exception_id, exception_data, current_user_session):
    with DB_Zabbix().Session() as session:
        exception = session.query(CassiaExceptions).filter(
            CassiaExceptions.exception_id == exception_id).first()
        if not exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"La Excepcion con el id proporcionado no existe"
            )
        exception.closed_at = exception_data.closed_at
        session.commit()
        session.refresh(exception)
    return success_response(message="Excepcion cerrada correctamente",
                            data=exception,
                            status_code=status.HTTP_200_OK)

""" def update_exception(exception_agency_id: int, exception_agency: exception_agency_schema.ExceptionAgencyBase):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(ExceptionAgency).filter(
        ExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    if exception_agency_search.name == exception_agency.name:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Update at least one field"
        )
    exception_agency_search.name = exception_agency.name
    exception_agency_search.updated_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Updated",
                            data=exception_agency_search)


def delete_exception(exception_agency_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(ExceptionAgency).filter(
        ExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    exception_agency_search.deleted_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Deleted")

 """
""" Change status """


def change_status(problemid: int, estatus: str, current_user_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    problem_record = session.query(ProblemRecord).filter(
        ProblemRecord.problemid == problemid).first()
    if not problem_record:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Problem Record not exists",
        )
    if problem_record.estatus == "Excepcion":
        excepcion = session.query(ExceptionModel).filter(
            ExceptionModel.problemrecord_id == problem_record.problemrecord_id).first()
        excepcion.deleted_at = datetime.now()
        session.commit()
        session.refresh(excepcion)
    match estatus:
        case "En curso":
            problem_record.estatus = "En curso"
            if problem_record.taken_at is None:
                problem_record.taken_at = datetime.now()
                problem_record.user_id = current_user_id
        case "Cerrado":
            problem_record.closed_at = datetime.now()
            if problem_record.taken_at is None:
                problem_record.taken_at = datetime.now()
                problem_record.user_id = current_user_id
            problem_record.estatus = "Cerrado"
        case "Soporte 2do Nivel":
            if problem_record.taken_at is None:
                problem_record.taken_at = datetime.now()
                problem_record.user_id = current_user_id
            problem_record.estatus = "Soporte 2do Nivel"

    session.commit()
    session.refresh(problem_record)
    session.close()
    return success_response(message="Estatus actualizado correctamente",
                            data=problem_record)


async def create_message(problemid: int, message: str | None, current_user_id: int, file: UploadFile | None):
    if message is None and file is None:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The request must contain a message or a File",
        )
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    problem = session.query(ProblemRecord).filter(
        ProblemRecord.problemid == problemid).first()
    if not problem:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Problem not exists",
        )
    file_dest = None
    if file:
        upload_dir = os.path.join(
            os.getcwd(), f"uploads/{problem.problemrecord_id}")
        # Create the upload directory if it doesn't exist
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)

            # get the destination path
        file_dest = os.path.join(upload_dir, file.filename)
        print(file_dest)

        # copy the file contents
        with open(file_dest, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

    prh = ProblemRecordHistory(
        problemrecord_id=problem.problemrecord_id,
        user_id=current_user_id,
        message=message,
        file_route=file_dest,
        created_at=datetime.now()
    )
    session.add(prh)
    session.commit()
    session.refresh(prh)
    session.close()
    return success_response(message="Mensaje guardado correctamente",
                            data=prh)


async def get_messages(problemid: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    problem = session.query(ProblemRecord).filter(
        ProblemRecord.problemid == problemid).first()
    if not problem:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Problem not exists",
        )
    history = session.query(ProblemRecordHistory).filter(
        ProblemRecordHistory.problemrecord_id == problem.problemrecord_id,
        ProblemRecordHistory.deleted_at == None
    ).all()
    session.close()
    db_zabbix.stop()
    return success_response(
        data=history)


async def download_file(message_id: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    message_file = session.query(ProblemRecordHistory).filter(
        ProblemRecordHistory.problemsHistory_id == message_id).first()

    if not message_file:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The message not exists",
        )

    if not message_file.file_route:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The file not exists",
        )

    if os.path.exists(message_file.file_route):
        filename = path_leaf(message_file.file_route)
        return FileResponse(path=message_file.file_route, filename=filename)
    session.close()
    return success_response(
        message="File not found",
        success=False,
        status_code=status.HTTP_404_NOT_FOUND
    )


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


async def register_ack(eventid, message, current_session, close, is_zabbix_event):
    if int(is_zabbix_event):
        db_zabbix = DB_Zabbix()
        session = db_zabbix.Session()
        statement = text(
            f"select eventid  from events p where eventid ='{eventid}'")
        problem = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
        if problem.empty:
            session.close()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The eventid not exists",
            )

        try:
            api_zabbix = ZabbixAPI(settings.zabbix_server_url)
            api_zabbix.login(user=settings.zabbix_user,
                             password=settings.zabbix_password)

        except:
            session.close()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al concectar con Zabbix",
            )

        try:
            params = {
                "eventids": eventid,
                "action": 5 if close else 4,
                "message": message
            }

            response = api_zabbix.do_request(method='event.acknowledge',
                                             params=params)
            ackid = text(
                f"select acknowledgeid from acknowledges order by acknowledgeid desc limit 1")
            ackid = pd.DataFrame(session.execute(ackid)).replace(np.nan, "")
            if ackid.empty:
                session.close()
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Error en la consulta en la tabla de acknowledges",
                )
            ackid = int(ackid['acknowledgeid'].values[0])+1
            cassia_acknowledge = CassiaAcknowledge(
                acknowledge_id=ackid,
                user_id=current_session.user_id
            )
            session.add(cassia_acknowledge)
            session.commit()
            session.refresh(cassia_acknowledge)
        except:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al crear el acknownledge",
            )

        return success_response(message="Acknowledge registrado correctamente")
    else:
        return await register_ack_cassia(eventid, message, current_session, close)


async def get_acks(eventid, is_cassia_event):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    if int(is_cassia_event):
        statement = text(
            f"select eventid,clock  from cassia_event_acknowledges p where eventid ='{eventid}'")
    else:
        statement = text(
            f"select eventid,clock  from events p where eventid ='{eventid}'")

    problem = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    if problem.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The eventid not exists",
        )
    try:
        acks = text(f"call sp_acknowledgeList1({eventid}, {is_cassia_event});")
        acks = pd.DataFrame(session.execute(acks)).replace(np.nan, "")
        acks['tickets'] = ['' for ack in range(len(acks))]
        statement = text(
            f"select * from cassia_tickets where event_id ='{eventid}'")
        tickets = pd.DataFrame(session.execute(statement)).replace(np.nan, "")

    except:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Error in call of process sp_acknowledgeList",
        )
    finally:
        session.close()

    now = datetime.now(pytz.timezone(
        'America/Mexico_City')).replace(tzinfo=None)
    clock_problem = problem.iloc[0]['clock']
    print("clock_problem:", clock_problem)

    if not int(is_cassia_event):
        clock_problem = datetime.fromtimestamp(
            clock_problem, pytz.timezone('America/Mexico_City')).replace(tzinfo=None)

    diff = now-clock_problem
    acumulated_cassia = round(diff.days*24 + diff.seconds/3600, 4)

    resume = {
        'acumulated_cassia': acumulated_cassia,
        'acumulated_ticket': 0,
        'date': now.strftime("%d/%m/%Y %H:%M:%S"),
    }
    if not acks.empty:
        resume["acumulated_ticket"] = []
        for ind in tickets.index:
            clock = tickets.iloc[ind]['clock']
            diff = now-clock
            hours = round(diff.days*24 + diff.seconds/3600, 4)
            print(hours)
            resume["acumulated_ticket"].append({'tracker_id': str(tickets['tracker_id'][ind]),
                                                'ticket_id': str(tickets['ticket_id'][ind]),
                                                'accumulated': hours})

            print(clock <= pd.to_datetime(acks["Time"]
                  [0], format="%d/%m/%Y %H:%M:%S"))
            acks.loc[clock <= pd.to_datetime(acks["Time"], format="%d/%m/%Y %H:%M:%S"),
                     'tickets'] = acks['tickets']+', '+str(tickets['tracker_id'][ind])

    response = dict()
    response.update(resume)
    response.update({'history': acks.to_dict(orient="records")})
    response.update({'tickets': tickets.to_dict(orient='records')})

    return success_response(data=response)


async def get_tickets(eventid):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select * from cassia_tickets where event_id ='{eventid}'")
    tickets = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    session.close()
    return success_response(data=tickets.to_dict(orient="records"))


async def link_ticket(ticket_data: cassia_ticket_schema.CassiaTicketBase, current_user_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select eventid  from events p where eventid ='{ticket_data.event_id}'")
    problem = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    if problem.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The eventid not exists",
        )
    ticket = CassiaTicketModel(
        tracker_id=ticket_data.tracker_id,
        user_id=current_user_session.user_id,
        clock=ticket_data.clock,
        created_at=datetime.now(),
        event_id=ticket_data.event_id
    )
    session.add(ticket)
    session.commit()
    session.refresh(ticket)
    session.close()

    return success_response(data=ticket)


async def delete_ticket(ticket_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ticket = session.query(CassiaTicketModel).filter(
        CassiaTicketModel.ticket_id == ticket_id
    ).first()
    if not ticket:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The ticket not exists",
        )
    session.delete(ticket)
    session.commit()
    session.close()

    return success_response(message="El ticket fue eliminado correctamente")


async def register_ack_cassia(eventid, message, current_session, close):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select cassia_arch_traffic_events_id  from cassia_arch_traffic_events p where cassia_arch_traffic_events_id ='{eventid}'")
    problem = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    if problem.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The eventid CASSIA not exists",
        )
    cassia_event_acknowledge = CassiaEventAcknowledge(
        userid=current_session.user_id,
        eventid=eventid,
        message=message,
        action=5 if close else 4
    )
    session.add(cassia_event_acknowledge)
    session.commit()
    session.refresh(cassia_event_acknowledge)

    return success_response(message="Acknowledge CASSIA registrado correctamente")
