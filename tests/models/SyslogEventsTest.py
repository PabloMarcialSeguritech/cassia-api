import unittest
from utils.db import DB_Syslog
from sqlalchemy import text
import pandas as pd


'''
Clase de prueba para la obtencion de info de syslog

'''


def obtener_system_events():
    db_syslog = DB_Syslog()
    session_syslog = db_syslog.Session()
    statement = text(f"""
        SELECT DeviceReportedTime, deviceIP, FromHost, Message, SysLogTag 
        FROM SystemEvents 
        WHERE in_cassia IS NULL
        ORDER BY ID
        LIMIT 1
    """)

    data = pd.DataFrame(session_syslog.execute(statement))
    return data

class SyslogEventsTest(unittest.TestCase):

    def test_obtener_system_events(self):
        print("> Entrando a test_obtener_system_events <")
        data = obtener_system_events()
        print(data)
        #for item in list:
        #    print("----------")
        #    for permission in item.permissions:
        #        print("permission:", permission.name)
