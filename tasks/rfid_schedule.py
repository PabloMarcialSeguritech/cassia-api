import unicodedata
from rocketry import Grouper
from rocketry.conds import every
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
import utils.settings as settings
# Creating the Rocketry app
rfid_schedule = Grouper()
# Creating some tasks
SETTINGS = settings.Settings()
traffic = SETTINGS.cassia_traffic


@rfid_schedule.cond('traffic')
def is_traffic():
    return traffic


@rfid_schedule.task(("every 30 seconds & traffic"), execution="thread")
async def get_rfid_data():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    db_c5 = DB_C5()
    session_c5 = db_c5.Session()
    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
    statement = text(f"call sp_hostView('0','{rfid_id}','')")
    rfid_devices = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    session.close()
    """ print(rfid_devices.head())
    print(len(rfid_devices)) """

    statement = text(f"""
    SELECT m.Nombre as Municipio, a.Nombre as Arco, r.Descripcion,
r.Estado, a2.UltimaLectura,
cl.lecturas as Lecturas,
a.Longitud,a.Latitud,
r.Ip 
FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(second,-30,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
order by a.Longitud,a.Latitud
""")

    try:
        arcos = pd.DataFrame(session_c5.execute(statement)).replace(np.nan, 0)
        arcos_grouped = arcos.groupby(['Ip'])[['Lecturas', 'Ip']
                                              ].sum().rename_axis('ip').reset_index()
        arcos_grouped = arcos_grouped[['ip', 'Lecturas']]
        arcos_grouped.rename(columns={"Lecturas": 'lecturas'}, inplace=True)
        longitudes_ips = arcos[['Longitud', 'Latitud', 'Ip', 'Municipio']]
        longitudes_ips['Municipio'] = longitudes_ips['Municipio'].apply(lambda x: unicodedata.normalize('NFD', str(x))
                                                                        .encode('ascii', 'ignore')
                                                                        .decode("utf-8"))
        longitudes_ips.rename(
            columns={'Ip': 'ip'}, inplace=True)
        longitudes_ips.drop_duplicates(inplace=True)
        """ print(longitudes_ips.to_string()) """
        arcos_grouped = arcos_grouped.merge(
            longitudes_ips, on='ip', how='left')
        """ print(arcos_grouped.to_string())
        print(len(arcos_grouped)) """
    finally:
        session_c5.close()
        """ print("Se cerro la sesion") """
    asd = rfid_devices.merge(arcos_grouped, on='ip', how='left')
    asd['latitude'] = asd[['latitude', 'Latitud']].apply(
        lambda x: x['Latitud'] if x['latitude'] == '--' else x['latitude'], axis=1)
    asd['longitude'] = asd[['longitude', 'Longitud']].apply(
        lambda x: x['Longitud'] if x['longitude'] == '--' else x['longitude'], axis=1)
    asd['date'] = [datetime.now(pytz.timezone(
        'America/Mexico_City')) for i in range(len(asd))]
    asd = asd[['hostid', 'Host', 'latitude',
               'longitude', 'ip', 'lecturas', 'date', 'Municipio']]
    asd.rename(columns={'Host': 'name', 'lecturas': 'readings',
               'Municipio': 'municipality'}, inplace=True)
    asd.replace(np.nan, 0, inplace=True)
    """ print(asd.to_string()) """
    sql = asd.to_sql('cassia_arch_traffic', con=db_zabbix.engine,
                     if_exists='append', index=False)


@rfid_schedule.task(("every 60 seconds & traffic"), execution="thread")
async def clean_data():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    date = datetime.now(pytz.timezone(
        'America/Mexico_City')) - timedelta(minutes=62)

    statement = text(f"""
    DELETE FROM cassia_arch_traffic
    WHERE cassia_arch_traffic_id in(
                     SELECT * FROM (SELECT cassia_arch_traffic_id FROM cassia_arch_traffic
        WHERE date<'{date}') AS p
    ) 
""")
    delete_rows = session.execute(statement)
    session.commit()

    session.close()


@rfid_schedule.task(("every 60 seconds & traffic"), execution="thread")
async def trigger_alerts():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"""
    select date from cassia_arch_traffic order by date desc limit 1 
""")
    last_date = pd.DataFrame(session.execute(statement))

    if not last_date.empty:
        last_date = last_date['date'].iloc[0]
    else:
        last_date = datetime.now(pytz.timezone(
            'America/Mexico_City'))
    rangos = [60, 45, 30, 20]
    alerts_defined = []
    alertas = pd.DataFrame()
    for rango in rangos:
        statement = text(f"""
            select date from cassia_arch_traffic order by date asc limit 1 
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
            statement = text(f"""
                SELECT hostid,readings FROM cassia_arch_traffic where date between
                '{date}' and '{last_date}'   
            """)
            statement2 = text(f"""
                SELECT hostid,latitude,longitude,municipality,ip,name FROM cassia_arch_traffic where date between
                '{date}' and '{last_date}'   
            """)
            result = pd.DataFrame(session.execute(statement2))
            result_copy = pd.DataFrame(session.execute(statement))
            result_copy = result_copy[['hostid', 'readings']]

            result_copy = result_copy.groupby(['hostid']).sum().reset_index()

            result_copy = result_copy[result_copy['readings'] < 1]

            result_copy = result_copy[~result_copy['hostid'].isin(
                alerts_defined)]
            alerts_defined = alerts_defined + \
                result_copy['hostid'].values.tolist()

            result_copy['alerta'] = [
                f"Este host no ha tenido lecturas en los ultimos {rango} minutos" for i in range(len(result_copy))]
            result_copy['severidad'] = [1 if rango == 20 else 2 if rango ==
                                        30 else 3 if rango == 45 else 4 for i in range(len(result_copy))]

            result = result.drop_duplicates()
            result_copy = result_copy.merge(
                result, on='hostid', how='left')

            """ print(result_copy.to_string()) """
            """ print(result_copy.to_string()) """
            alertas = pd.concat([alertas, result_copy], ignore_index=True)
    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
    for ind in alertas.index:
        alerta = session.query(CassiaArchTrafficEvent).filter(
            CassiaArchTrafficEvent.hostid == alertas['hostid'][ind],
            CassiaArchTrafficEvent.closed_at == None
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
                tech_id=rfid_id

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

    session.close()


@rfid_schedule.task(("every 60 seconds & traffic"), execution="thread")
async def trigger_alerts_close():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
    statement = text(f"""
    select date from cassia_arch_traffic order by date desc limit 1 
""")
    last_date = pd.DataFrame(session.execute(statement))

    if not last_date.empty:
        last_date = last_date['date'].iloc[0]
    else:
        last_date = datetime.now(pytz.timezone(
            'America/Mexico_City'))
    rangos = [60, 45, 30, 20]
    alerts_defined = []
    alertas = pd.DataFrame()
    for rango in rangos:
        statement = text(f"""
            select date from cassia_arch_traffic order by date asc limit 1 
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
            statement = text(f"""
                SELECT hostid,readings FROM cassia_arch_traffic where date between
                '{date}' and '{last_date}'   
            """)
            result = pd.DataFrame(session.execute(statement))
            result = result.groupby(['hostid']).sum().reset_index()
            result = result[result['readings'] < 1]
            result = result[~result['hostid'].isin(alerts_defined)]

            alerts_defined = alerts_defined+result['hostid'].values.tolist()
            result['alerta'] = [
                f"Este host no ha tenido lecturas en los ultimos {rango} minutos" for i in range(len(result))]
            result['severidad'] = [1 if rango == 20 else 2 if rango ==
                                   30 else 3 if rango == 45 else 4 for i in range(len(result))]

            alertas = pd.concat([alertas, result])
    """ print(alertas.to_string()) """
    abiertos = text(f"""SELECT cassia_arch_traffic_events_id,hostid FROM 
                    cassia_arch_traffic_events
                    where closed_at is NULL
                    and tech_id='{rfid_id}'
                    and message!='Unavailable by ICMP ping'
                    """)
    abiertos = pd.DataFrame(session.execute(abiertos))
    """ print(abiertos.to_string()) """
    if not abiertos.empty:
        a_cerrar = abiertos[~abiertos['hostid'].isin(
            alertas['hostid'].values.tolist())]
        """ print(a_cerrar.to_string()) """
        ids = ','.join(['0']+[str(v)
                       for v in a_cerrar['cassia_arch_traffic_events_id'].values.tolist()])

        statement = text(f"""
        UPDATE cassia_arch_traffic_events
        set closed_at='{datetime.now(pytz.timezone(
            'America/Mexico_City'))}',
        updated_at='{datetime.now(pytz.timezone(
            'America/Mexico_City'))}',
        status='Cerrada automaticamente'
        where cassia_arch_traffic_events_id
        in ({ids})
        and message!='Unavailable by ICMP ping'
        """)

        session.execute(statement)
        session.commit()

    session.close()
