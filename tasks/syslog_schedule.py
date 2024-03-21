import pandas as pd
from models.cassia_lpr_events import CassiaLprEvent as CassiaEventModel
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
from rocketry import Grouper
from utils.settings import Settings
from utils.db import DB_Zabbix, DB_Syslog
from sqlalchemy import text
import numpy as np
from datetime import datetime, timedelta
import pytz
import time
# Creating the Rocketry app
syslog_schedule = Grouper()

# Creating some tasks
SETTINGS = Settings()
syslog = SETTINGS.cassia_syslog
traffic_syslog = SETTINGS.cassia_traffic_syslog


@syslog_schedule.cond('syslog')
def is_syslog():
    return syslog


@syslog_schedule.cond('traffic_syslog')
def is_traffic_syslog():
    return traffic_syslog


@syslog_schedule.task(("every 30 seconds & syslog"), execution="thread")
async def update_syslog_data():
    db_zabbix = DB_Zabbix()
    with db_zabbix.Session() as session, DB_Syslog().Session() as session_syslog:
        try:
            batch_size = 900
            offset = 0
            update_batch = 3600
            update_ids_len = 0
            record_ids = []

            while True:

                # Consulta para seleccionar registros sin procesar en bloques de 100
                statement = text("""
                    SELECT ID, DeviceReportedTime as devicedReportedTime, deviceIP as ip, FromHost, Message as message, SysLogTag 
                    FROM SystemEvents 
                    WHERE in_cassia IS NULL
                    AND SysLogTag="PlateReader(Verbose)"
                    AND deviceIP IS NOT NULL
                    AND DeviceReportedTime>'2024-02-15 10:00:00'
                    ORDER BY ID
                    LIMIT :batch_size OFFSET :offset
                """)
                # Aplicar la consulta con el tamaño de lote y el offset actual
                syslog_records = pd.DataFrame(session_syslog.execute(
                    statement, {'batch_size': batch_size, 'offset': offset}))

                # Salir del bucle si no hay más registros
                if syslog_records.empty and update_ids_len == 0:
                    break

                # Construir lista de registros para bulk insert
                """ event_records = [
                    CassiaEventModel(
                        devicedReportedTime=record['DeviceReportedTime'],
                        ip=record['deviceIP'],
                        FromHost=record['FromHost'],
                        message=record['Message'],
                        SysLogTag=record['SysLogTag']
                    ) for _, record in syslog_records.iterrows()
                ] """

                # Iniciar transacción para Zabbix

                # Bulk insert para Zabbix

                data_to_insert = syslog_records.drop(columns=['ID'])
                data_to_insert = data_to_insert.to_dict(orient='records')
                session.bulk_insert_mappings(
                    CassiaEventModel, data_to_insert)
                session.commit()

                if not syslog_records.empty:
                    record_ids += syslog_records['ID'].tolist()
                update_ids_len = len(record_ids)

                if update_ids_len >= update_batch or (syslog_records.empty):
                    # Construir lista de IDs para la actualización en Syslog

                    # Iniciar transacción para Syslog

                    # Actualizar la base de datos de Syslog
                    update_statement = text("""
                            UPDATE SystemEvents
                            SET in_cassia=1
                            WHERE ID IN :ids
                        """)

                    session_syslog.execute(
                        update_statement, params={'ids': record_ids})
                    session_syslog.commit()

                    update_ids_len = 0
                    record_ids = []
                    # Incrementar el offset para la siguiente iteración
                offset += batch_size

        except Exception as e:
            # Manejar errores según sea necesario
            print(f"Error: {e}")


@syslog_schedule.task(("every 30 seconds & traffic_syslog"), execution="thread")
async def get_traffic_syslog_data():
    now = datetime.now(pytz.timezone('America/Mexico_City'))
    db_zabbix = DB_Zabbix()
    with db_zabbix.Session() as session:
        lpr_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == "lpr_id").first()
        lpr_id = "1" if not lpr_id else lpr_id.value
        statement = text(f"""
SELECT count(h.hostid) as readings ,h.hostid ,h.host as name,hi.location_lat as latitude,hi.location_lon as longitude,
i.ip, cm.name as municipality FROM hosts h 
INNER JOIN host_inventory hi  on h.hostid=hi.hostid 
inner join interface i on h.hostid =i.hostid 
inner join cassia_lpr_events cle on i.ip=cle.ip 
INNER JOIN hosts_groups hg on h.hostid= hg.hostid 
inner join cat_municipality cm on hg.groupid =cm.groupid 
where hi.device_id={lpr_id} and cle.SysLogTag ='PlateReader(Verbose)'
and devicedReportedTime between DATE_ADD(now(),INTERVAL -300 SECOND) and DATE_ADD(now(),INTERVAL -270 SECOND)
group by h.host ,i.ip,cle.ip, cm.name ,h.hostid 
order by count(h.hostid) desc""")
        data = pd.DataFrame(session.execute(statement)).replace(np.nan, "")

        data['date'] = [now for i in range(len(data))]
        sql = data.to_sql('cassia_arch_traffic_lpr', con=db_zabbix.engine,
                          if_exists='append', index=False)
        session.commit()


@syslog_schedule.task(("every 60 seconds & traffic_syslog"), execution="thread")
async def clean_data_lpr():
    with DB_Zabbix().Session() as session:
        date = datetime.now(pytz.timezone(
            'America/Mexico_City')) - timedelta(minutes=70)
        statement = text(f"""
    DELETE FROM cassia_arch_traffic_lpr
    WHERE cassia_arch_traffic_lpr_id in(
                     SELECT * FROM (SELECT cassia_arch_traffic_lpr_id FROM cassia_arch_traffic_lpr
        WHERE date<'{date}') AS p)""")
        delete_rows = session.execute(statement)
        session.commit()


@syslog_schedule.task(("every 60 seconds & traffic_syslog"), execution="thread")
async def trigger_lpr_alerts():
    with DB_Zabbix().Session() as session:
        lpr_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == "lpr_id").first()
        lpr_id = "1" if not lpr_id else lpr_id.value
        statement = text(f"""
select date from cassia_arch_traffic_lpr order by date desc limit 1 """)
        last_date = pd.DataFrame(session.execute(statement))

        if not last_date.empty:
            last_date = last_date['date'].iloc[0]
        else:
            last_date = datetime.now(pytz.timezone(
                'America/Mexico_City'))
        rangos = [60, 45, 30, 20]
        alerts_defined = []
        alertas = pd.DataFrame()
        lprs = text(f"""SELECT h.hostid ,h.host as name,hi.location_lat as latitude,hi.location_lon as longitude,
i.ip, cm.name as municipality FROM hosts h 
INNER JOIN host_inventory hi  on h.hostid=hi.hostid 
inner join interface i on h.hostid =i.hostid
INNER JOIN hosts_groups hg on h.hostid= hg.hostid 
inner join cat_municipality cm on hg.groupid =cm.groupid 
where hi.device_id=1""")
        lprs = pd.DataFrame(session.execute(lprs)).replace(np.nan, "")

        for rango in rangos:
            statement = text(f"""
               select date from cassia_arch_traffic_lpr order by date asc limit 1 
           """)
            first_date = pd.DataFrame(session.execute(statement))

            if not first_date.empty:
                first_date = first_date['date'].iloc[0]
            else:
                first_date = datetime.now(pytz.timezone(
                    'America/Mexico_City'))
            minutes = (last_date-first_date).total_seconds()/60.0
            if minutes >= rango:
                date = last_date - timedelta(minutes=rango)

                statement2 = text(f"""
                   SELECT hostid FROM cassia_arch_traffic_lpr where date between
                   '{date}' and '{last_date}'   
               """)
                result = pd.DataFrame(session.execute(statement2))
                result = result.drop_duplicates(subset=['hostid'])
                hosts = []
                if not result.empty:
                    hosts = result['hostid'].to_list()
                result_alert = lprs[~lprs['hostid'].isin(hosts)]
                result_alert = result_alert[~result_alert['hostid'].isin(
                    alerts_defined)]
                alerts_defined = alerts_defined + \
                    result_alert['hostid'].values.tolist()

                result_alert['alerta'] = [
                    f"Este host no ha tenido lecturas por más de " for i in range(len(result_alert))]
                result_alert['severidad'] = [1 if rango == 20 else 2 if rango ==
                                             30 else 3 if rango == 45 else 4 for i in range(len(result_alert))]

                alertas = pd.concat([alertas, result_alert], ignore_index=True)
        for ind in alertas.index:
            alerta = session.query(CassiaArchTrafficEvent).filter(
                CassiaArchTrafficEvent.hostid == alertas['hostid'][ind],
                CassiaArchTrafficEvent.closed_at == None,
                CassiaArchTrafficEvent.alert_type == 'lpr'
            ).first()
            if not alerta:
                alerta_created = CassiaArchTrafficEvent(
                    hostid=alertas['hostid'][ind],
                    created_at=datetime.now(pytz.timezone(
                        'America/Mexico_City')),
                    severity=alertas['severidad'][ind],
                    message=alertas['alerta'][ind],
                    status='Creada',
                    latitude=alertas['latitude'][ind],
                    longitude=alertas['longitude'][ind],
                    municipality=alertas['municipality'][ind],
                    ip=alertas['ip'][ind],
                    hostname=alertas['name'][ind],
                    tech_id=lpr_id,
                    alert_type='lpr'
                )
                session.add(alerta_created)
            else:
                if alerta.severity != alertas['severidad'][ind]:
                    alerta.severity = alertas['severidad'][ind]
                    alerta.message = alertas['alerta'][ind]
                    alerta.updated_at = datetime.now(pytz.timezone(
                        'America/Mexico_City'))
                    alerta.status = "Severidad actualizada"
            session.commit()


@syslog_schedule.task(("every 60 seconds & traffic_syslog"), execution="thread")
async def trigger_alerts_lpr_close():
    with DB_Zabbix().Session() as session:
        lpr_config = session.query(CassiaConfig).filter(
            CassiaConfig.name == "lpr_id").first()
        lpr_id = "1"
        if lpr_config:
            lpr_id = lpr_config.value
        a_cerrar = text(f"""SELECT * FROM 
                        cassia_arch_traffic_events
                        where closed_at is NULL
                        and tech_id='{lpr_id}'
                        and alert_type='lpr'
                        and message!='Unavailable by ICMP ping'
                        and hostid in (
                        SELECT DISTINCT hostid FROM cassia_arch_traffic_lpr where readings>0)""")
        a_cerrar = pd.DataFrame(session.execute(a_cerrar)).replace(np.nan, '')
        ids = [0]
        if not a_cerrar.empty:
            ids = a_cerrar['cassia_arch_traffic_events_id'].astype(
                'int').to_list()
            statement = text(f"""
            UPDATE cassia_arch_traffic_events
            set closed_at='{datetime.now(pytz.timezone(
                'America/Mexico_City'))}',
            updated_at='{datetime.now(pytz.timezone(
                'America/Mexico_City'))}',
            status='Cerrada automaticamente'
            where cassia_arch_traffic_events_id
            in :ids
            """)

            session.execute(statement, params={'ids': ids})
            session.commit()
