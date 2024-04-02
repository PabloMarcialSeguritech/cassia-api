import pandas as pd
from utils.db import DB_Zabbix, DB_Prueba
from sqlalchemy import text
import numpy as np
from datetime import datetime
import pytz


async def get_exceptions(hostids: list, session):
    try:
        query = text(
            "select ce.*,cea.name as agency_name from cassia_exceptions ce inner join cassia_exception_agencies cea on cea.exception_agency_id=ce.exception_agency_id where hostid in :hostids and closed_at is null")
        """ print(hostids)
        print(query) """
        exceptions = pd.DataFrame(session.execute(
            query, {'hostids': hostids})).replace(np.nan, "")
        print(exceptions)
        if exceptions.empty:
            exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                               'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid', 'agency_name', 'exception_message'])
        else:
            now = datetime.now(pytz.timezone('America/Mexico_City'))
            exceptions['created_at'] = pd.to_datetime(exceptions['created_at'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
                pytz.timezone('America/Mexico_City'))
            exceptions['diferencia'] = now-exceptions['created_at']
            exceptions['dias'] = exceptions['diferencia'].dt.days
            exceptions['horas'] = exceptions['diferencia'].dt.components.hours
            exceptions['minutos'] = exceptions['diferencia'].dt.components.minutes
            exceptions = exceptions.drop(columns=['diferencia',])
            exceptions['diferencia'] = exceptions.apply(
                lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
            exceptions['exception_message'] = exceptions.apply(
                lambda x: f"Agencia: {x['agency_name']} --- Afectado desde: {x['diferencia']} --- Nota: {x['description']}", axis=1)
            exceptions = exceptions.drop(
                columns=['diferencia', 'dias', 'horas', 'minutos'])
        return exceptions
    except Exception as e:
        print(f"Excepcion generada en get_exceptions: {e}")
        exceptions = pd.DataFrame(columns=['exception_agency_id', 'description',
                                           'created_at', 'closed_at', 'session_id', 'exception_id', 'hostid', 'agency_name', 'exception_message'])
        return exceptions
