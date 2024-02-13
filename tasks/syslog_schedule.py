import pandas as pd
from models.cassia_lpr_events import CassiaLprEvent as CassiaEventModel
from models.cassia_config import CassiaConfig
from rocketry import Grouper
from utils.settings import Settings
from utils.db import DB_Zabbix, DB_Syslog
from sqlalchemy import text
import numpy as np
from datetime import datetime
import pytz
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
    with DB_Zabbix().Session() as session, DB_Syslog().Session() as session_syslog:
        try:
            batch_size = 1000
            offset = 0
            while True:
                # Consulta para seleccionar registros sin procesar en bloques de 100
                statement = text("""
                    SELECT ID, DeviceReportedTime, deviceIP, FromHost, Message, SysLogTag 
                    FROM SystemEvents 
                    WHERE in_cassia IS NULL
                    AND SysLogTag="PlateReader(Verbose)"
                    AND deviceIP IS NOT NULL
                    ORDER BY ID
                    LIMIT :batch_size OFFSET :offset
                """)
                # Aplicar la consulta con el tamaño de lote y el offset actual
                syslog_records = pd.DataFrame(session_syslog.execute(
                    statement, {'batch_size': batch_size, 'offset': offset}))

                # Salir del bucle si no hay más registros
                if syslog_records.empty:
                    break

                # Construir lista de registros para bulk insert
                event_records = [
                    CassiaEventModel(
                        devicedReportedTime=record['DeviceReportedTime'],
                        ip=record['deviceIP'],
                        FromHost=record['FromHost'],
                        message=record['Message'],
                        SysLogTag=record['SysLogTag']
                    ) for _, record in syslog_records.iterrows()
                ]

                # Iniciar transacción para Zabbix

                # Bulk insert para Zabbix
                session.bulk_save_objects(event_records)
                session.commit()

                # Construir lista de IDs para la actualización en Syslog
                record_ids = syslog_records['ID'].tolist()

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
and devicedReportedTime between DATE_ADD(now(),INTERVAL -30 SECOND) and now()
group by h.host ,i.ip,cle.ip, cm.name ,h.hostid 
order by count(h.hostid) desc""")
        data = pd.DataFrame(session.execute(statement)).replace(np.nan, "")

        data['date'] = [now for i in range(len(data))]
        sql = data.to_sql('cassia_arch_traffic_lpr', con=db_zabbix.engine,
                          if_exists='append', index=False)
        session.commit()
