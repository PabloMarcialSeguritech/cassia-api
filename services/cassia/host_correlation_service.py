from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from models.host_correlation import HostCorrelation
import numpy as np
from utils.traits import success_response
from fastapi.responses import FileResponse
import tempfile
settings = Settings()


def get_correlations():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"""
    SELECT hc.correlarionid,hc.hostidP ,(SELECT name from hosts where hostid=hc.hostidP) as hostidP_name,hc.hostidC ,(SELECT name from hosts where hostid=hc.hostidC) as hostidC_name,
                     hc.session_id ,hc.created_at ,hc.updated_at
    FROM host_correlation hc
    WHERE hc.deleted_at is NULL
    """)
    correlations = pd.DataFrame(session.execute(statement))
    session.close()
    return success_response(data=correlations.to_dict(orient="records"))


async def get_hosts_without_relations():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_hostView('0', '11','' )")
    suscriptores = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    if len(suscriptores) > 1:
        hostids = tuple(suscriptores['hostid'].values.tolist())
    else:
        if len(suscriptores) == 1:
            hostids = f"({suscriptores['hostid'][0]})"
        else:
            hostids = "()"
    statement = text(f"""
    SELECT h.hostid,h.name from hosts h 
where h.hostid not in(SELECT hostidP from host_correlation)
and h.hostid not in (SELECT hostidC from host_correlation)
and h.hostid in {hostids}
    """)
    not_correlations = pd.DataFrame(session.execute(statement))
    statement = text(f"call sp_hostView('0', '2','' )")
    aps = session.execute(statement)
    aps = pd.DataFrame(aps).replace(np.nan, "")
    aps = aps[['hostid', 'Host']].copy()
    session.close()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
        xlsx_filename = temp_file.name
        with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:
            not_correlations.to_excel(
                writer, sheet_name="HOST SIN RELACIONAR", index=False)
            aps.to_excel(writer, sheet_name="APS", index=False)

    return FileResponse(xlsx_filename, headers={"Content-Disposition": "attachment; filename=datos.xlsx"}, media_type="application/vnd.ms-excel", filename="datos.xlsx")