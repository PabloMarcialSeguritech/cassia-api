from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi import status
from utils.traits import success_response
from models.cassia_config import CassiaConfig
import numpy as np
settings = Settings()


def get_aps_layer(municipality_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_hostView('{municipality_id}','2','')")
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    session.close()
    response = {"aps": data.to_dict(
        orient="records")
    }
    return success_response(data=response)


def get_downs_layer(municipality_id, dispId, subtype_id):
    db_zabbix = DB_Zabbix()
    statement = text(
        f"call sp_hostDown('{municipality_id}','{dispId}','{subtype_id}')")
    session = db_zabbix.Session()
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")

    downs_origen = text(
        f"""call sp_diagnostic_problems('{municipality_id}','{dispId}')""")
    downs_origen = pd.DataFrame(
        session.execute(downs_origen)).replace(np.nan, "")
    if not downs_origen.empty:
        downs_origen['value'] = [1 for i in range(len(downs_origen))]
        downs_origen['description'] = [
            'ICMP ping' for i in range(len(downs_origen))]
        downs_origen['origen'] = [1 for i in range(len(downs_origen))]
        if not data.empty:
            data = data[~data['hostid'].isin(downs_origen['hostid'].to_list())]
    if not data.empty:
        data['origen'] = [0 for i in range(len(data))]
    if data.empty and not downs_origen.empty:
        data = downs_origen
    if not data.empty and not downs_origen.empty:
        data = pd.concat([downs_origen, data],
                         ignore_index=True).replace(np.nan, "")
    response = {"downs": data.to_dict(
        orient="records")
    }
    session.close()
    return success_response(data=response)


def get_carreteros(municipality_id):
    db_zabbix = DB_Zabbix()
    session_zabbix = db_zabbix.Session()
    if municipality_id != "0":

        statement = text("call sp_catCity()")
        municipios = session_zabbix.execute(statement)
        data = pd.DataFrame(municipios).replace(np.nan, "")
        try:
            municipio = data.loc[data["groupid"] == int(municipality_id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id is not a int value"
            )
        if len(municipio) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id not exist"
            )
        rfids = text(f"call sp_hostView('{municipality_id}','9','')")
        rfids = session_zabbix.execute(rfids)
        rfids = pd.DataFrame(rfids).replace(np.nan, "")

        session_zabbix.close()

    db_c5 = DB_C5()
    if municipality_id != "0":
        statement = text(f"""
SELECT  ISNULL(SUM(cl.lecturas),0)  as Lecturas, a.Longitud,a.Latitud FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
WHERE m.Nombre COLLATE Latin1_General_CI_AI LIKE '{municipio['name'].values[0]}' COLLATE Latin1_General_CI_AI
group by a.Latitud, a.Longitud 
order by a.Longitud,a.Latitud
""")
    else:
        statement = text("""
SELECT  ISNULL(SUM(cl.lecturas),0)  as Lecturas, a.Longitud,a.Latitud FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)

group by a.Latitud, a.Longitud 
order by a.Longitud,a.Latitud""")

    session = db_c5.Session()
    data = session.execute(statement)
    data = pd.DataFrame(data).replace(np.nan, "")
    session.close()
    return success_response(data=data.to_dict(orient="records"))


def get_carreteros2(municipality_id):
    with DB_Zabbix().Session() as session:
        rfid_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == "lpr_id").first()
        rfid_id = "9" if not rfid_id else rfid_id.value
        if municipality_id != "0":
            statement = text("call sp_catCity()")
            municipios = session.execute(statement)
            data = pd.DataFrame(municipios).replace(np.nan, "")
            try:
                municipio = data.loc[data["groupid"] == int(municipality_id)]
            except:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id is not a int value"
                )
            if len(municipio) < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id not exist"
                )
            rfids = text(
                f"call sp_hostView('{municipality_id}','{rfid_id}','')")
            rfids = pd.DataFrame(session.execute(rfids)).replace(np.nan, "")
        else:
            rfids = text(f"call sp_hostView('0','{rfid_id}','')")
            rfids = pd.DataFrame(session.execute(rfids)).replace(np.nan, "")
        if not rfids.empty:
            rfids = rfids[['latitude', 'longitude']]
            rfids = rfids[rfids['latitude'] != '--']
            rfids = rfids[rfids['longitude'] != '--']
            rfids = rfids.drop_duplicates(subset=['latitude', 'longitude'])
        data = rfids
        if municipality_id != "0":
            statement = text(f"""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude FROM cassia_arch_traffic c 
where c.`date` between DATE_ADD(now(),INTERVAL -5 MINUTE) and NOW()  
and c.municipality  LIKE '{municipio['name'].values[0]}'
group by c.latitude, c.longitude 
""")
        else:
            statement = text("""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude 
FROM cassia_arch_traffic c where c.`date` 
between DATE_ADD(now(),INTERVAL -5 MINUTE) and NOW()  
group by c.latitude, c.longitude
""")
        lecturas = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
        if not lecturas.empty:
            data = pd.merge(data, lecturas, how='left', left_on=['latitude', 'longitude'],
                            right_on=['latitude', 'longitude']).replace(np.nan, 0)
        else:
            data['Lecturas'] = [0 for i in range(len(data))]
        statement = text(f"""
SELECT max(c.severity) as max_severity, c.longitude ,c.latitude FROM cassia_arch_traffic_events c 
WHERE c.closed_at is NULL 
AND tech_id='{rfid_id}'
group by c.latitude, c.longitude 
""")
        alerts = pd.DataFrame(session.execute(
            statement)).replace(np.nan, "")
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)
    else:
        data['max_severity'] = [0 for al in range(len(data))]
    return success_response(data=data.to_dict(orient="records"))


def get_lpr(municipality_id):
    with DB_Zabbix().Session() as session:
        lpr_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == "lpr_id").first()
        lpr_id = "1" if not lpr_id else lpr_id.value
        if municipality_id != "0":
            statement = text("call sp_catCity()")
            municipios = session.execute(statement)
            data = pd.DataFrame(municipios).replace(np.nan, "")
            try:
                municipio = data.loc[data["groupid"] == int(municipality_id)]
            except:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id is not a int value"
                )
            if len(municipio) < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id not exist"
                )
            lprs = text(f"call sp_hostView('{municipality_id}','{lpr_id}','')")
            lprs = pd.DataFrame(session.execute(lprs)).replace(np.nan, "")
        else:
            lprs = text(f"call sp_hostView('0','{lpr_id}','')")
            lprs = pd.DataFrame(session.execute(lprs)).replace(np.nan, "")
        if not lprs.empty:
            lprs = lprs[['latitude', 'longitude']]
            lprs = lprs[lprs['latitude'] != '--']
            lprs = lprs[lprs['longitude'] != '--']
            lprs = lprs.drop_duplicates(subset=['latitude', 'longitude'])
        data = lprs
        if municipality_id != "0":
            statement = text(f"""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude FROM cassia_arch_traffic_lpr c 
where c.`date` between DATE_ADD(now(),INTERVAL -10 MINUTE) and NOW()  
and c.municipality  LIKE '{municipio['name'].values[0]}'
group by c.latitude, c.longitude 
""")
        else:
            statement = text("""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude 
FROM cassia_arch_traffic_lpr c where c.`date` 
between DATE_ADD(now(),INTERVAL -10 MINUTE) and NOW()  
group by c.latitude, c.longitude
""")
        lecturas = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
        if not lecturas.empty:
            data = pd.merge(data, lecturas, how='left', left_on=['latitude', 'longitude'],
                            right_on=['latitude', 'longitude']).replace(np.nan, 0)
        else:
            data['Lecturas'] = [0 for i in range(len(data))]
        statement = text(f"""
SELECT max(c.severity) as max_severity, c.longitude ,c.latitude FROM cassia_arch_traffic_events c 
where c.closed_at is NULL 
AND tech_id='{lpr_id}'
group by c.latitude, c.longitude 
""")
        alerts = pd.DataFrame(session.execute(
            statement)).replace(np.nan, "")
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)

    else:
        data['max_severity'] = [0 for al in range(len(data))]
    return success_response(data=data.to_dict(orient="records"))


async def get_switches_connectivity(municipality_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    statement = text(f"call sp_switchConnectivity('{municipality_id}')")
    switches = session.execute(statement)
    data = pd.DataFrame(switches).replace(np.nan, "")
    session.close()

    return success_response(data=data.to_dict(orient="records"))
