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
        # Consulta para seleccionar registros sin procesar en bloques de 1000
        statement = text("""
                SELECT ID, DeviceReportedTime, deviceIP, FromHost, Message, SysLogTag 
                FROM SystemEvents 
                WHERE in_cassia IS NULL
                ORDER BY ID
                LIMIT :batch_size OFFSET :offset
            """)

        # Procesar los registros en lotes
        batch_size = 1000
        offset = 0
        while True:
            # Aplicar la consulta con el tamaño de lote y el offset actual
            syslog_records = pd.DataFrame(session_syslog.execute(statement, {'batch_size': batch_size, 'offset': offset}))

            # Salir del bucle si no hay más registros
            if syslog_records.empty:
                break

            for index, record in syslog_records.iterrows():
                try:
                    # Actualizar la base de datos de Zabbix
                    event_record = CassiaEventModel(
                        devicedReportedTime=record['DeviceReportedTime'],
                        ip=record['deviceIP'],
                        FromHost=record['FromHost'],
                        message=record['Message'],
                        SysLogTag=record['SysLogTag']
                    )
                    session.add(event_record)

                    # Actualizar la base de datos de Syslog
                    update_statement = text(f"""
                            UPDATE SystemEvents
                            SET in_cassia=1
                            WHERE ID={record['ID']}
                        """)
                    session_syslog.execute(update_statement)

                    # Confirmar los cambios para la sesión de Zabbix
                    session.commit()
                    session_syslog.commit()

                except Exception as e:
                    # Manejar errores según sea necesario
                    session.rollback()
                    session_syslog.rollback()
                    print(f"Error: {e}")

            # Incrementar el offset para la siguiente iteración
            offset += batch_size

    except Exception as e:
        # Manejar errores según sea necesario
        print(f"Error: {e}")

    finally:
        # Cerrar sesiones al salir del bloque 'with'
        session.close()
        session_syslog.close()
