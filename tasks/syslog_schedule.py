import pandas as pd
from models.cassia_lpr_events import CassiaLprEvent as CassiaEventModel
from rocketry import Grouper
from utils.settings import Settings
from utils.db import DB_Zabbix, DB_Syslog
from sqlalchemy import text

# Creating the Rocketry app
syslog_schedule = Grouper()

# Creating some tasks
SETTINGS = Settings()
syslog = SETTINGS.cassia_syslog


@syslog_schedule.cond('syslog')
def is_syslog():
    return syslog


@syslog_schedule.task(("every 30 seconds & syslog"), execution="thread")
async def update_syslog_data():
    db_syslog = DB_Syslog()
    session_syslog = db_syslog.Session()
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    try:
        batch_size = 1000
        offset = 0

        while True:
            # Consulta para seleccionar registros sin procesar en bloques de 1000
            statement = text("""
                SELECT ID, DeviceReportedTime, deviceIP, FromHost, Message, SysLogTag 
                FROM SystemEvents 
                WHERE in_cassia IS NULL
                ORDER BY ID
                LIMIT :batch_size OFFSET :offset
            """)

            # Aplicar la consulta con el tamaño de lote y el offset actual
            syslog_records = pd.DataFrame(
                session_syslog.execute(statement, {'batch_size': batch_size, 'offset': offset}))

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
            with session.begin():
                # Bulk insert para Zabbix
                session.bulk_save_objects(event_records)
                session.commit()

            # Construir lista de IDs para la actualización en Syslog
            record_ids = syslog_records['ID'].tolist()

            # Iniciar transacción para Syslog
            with session_syslog.begin():
                # Actualizar la base de datos de Syslog
                update_statement = text("""
                    UPDATE SystemEvents
                    SET in_cassia=1
                    WHERE ID IN :ids
                """)
                session_syslog.execute(update_statement, params={'ids': record_ids})
                session_syslog.commit()

            # Incrementar el offset para la siguiente iteración
            offset += batch_size

    except Exception as e:
        # Manejar errores según sea necesario
        print(f"Error: {e}")

    finally:
        # Cerrar sesiones al salir del bloque 'with'
        session.close()
        session_syslog.close()
