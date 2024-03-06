from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from models.host_correlation import HostCorrelation
import numpy as np
from utils.traits import success_response
from fastapi.responses import FileResponse
from fastapi.exceptions import HTTPException
from fastapi import status
import math
import tempfile
import os
import ntpath
from fastapi import status
settings = Settings()


def get_correlations(page, page_size):
    skip = (page-1)*page_size
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"""
    SELECT hc.correlarionid,hc.hostidP ,(SELECT name from hosts where hostid=hc.hostidP) as hostidP_name,hc.hostidC ,(SELECT name from hosts where hostid=hc.hostidC) as hostidC_name,
                     hc.session_id ,hc.created_at ,hc.updated_at
    FROM host_correlation hc
    WHERE hc.deleted_at is NULL
    LIMIT :page_size OFFSET :skip
    """)
    correlations = pd.DataFrame(session.execute(
        statement, {'page_size': page_size, 'skip': skip}))
    statement = text("""SELECT count(hc.correlarionid)
    FROM host_correlation hc
    WHERE hc.deleted_at is NULL""")
    total = pd.DataFrame(session.execute(statement)).replace(np.nan, 0)
    total_registros = 0
    if not total.empty:
        total_registros = int(total.iloc[0, 0])
    total_paginas = total_registros/page_size
    total_paginas = math.ceil(total_paginas)
    registros_pagina = len(correlations)
    data = {'actual_page': page,
            'total_pages': total_paginas,
            'page_size': registros_pagina,
            'total_data': total_registros,
            'data': correlations.to_dict(orient="records")}

    session.close()
    return success_response(data=data)


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


async def download_file():
    path = os.path.join(
        os.getcwd(), 'static\\templates\\arrastres\ejemplo.xlsx')
    print(path)
    if os.path.exists(path):
        return FileResponse(path,  headers={"Content-Disposition": "attachment; filename=_Alta Relaciones - Ej.  v1.0.xlsx"}, media_type="application/vnd.ms-excel", filename="_Alta Relaciones - Ej.  v1.0.xlsx")

    return success_response(
        message="File not found",
        success=False,
        status_code=status.HTTP_404_NOT_FOUND
    )


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


async def process_file(current_user_session, file):
    current_session = current_user_session.session_id.hex

    content_type = file.content_type
    if content_type not in ["text/csv", "application/vnd.ms-excel"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type")
    data = pd.read_csv(file.file)
    if not data.empty:
        with DB_Zabbix().Session() as session:
            data['resultado'] = ''
            ids = data.iloc[:, 0].to_list()+data.iloc[:, 1].to_list()
            ids = list(set(ids))
            existen = text(f"Select hostid from hosts where hostid in :ids")
            existen = pd.DataFrame(session.execute(existen, {'ids': ids}))
            no_existen = pd.DataFrame(columns=['hostid'], data=ids)
            if not existen.empty:
                no_existen = no_existen[~no_existen['hostid'].isin(
                    existen['hostid'].to_list())]
            for i in data.index:
                padre = data.iloc[i, 0]
                hijo = data.iloc[i, 1]
                existe_padre = existe(
                    str(padre), existen['hostid'].astype('str').to_list())
                existe_hijo = existe(
                    str(hijo), existen['hostid'].astype('str').to_list())
                if not existe_padre and not existe_hijo:
                    print(f"no existe {padre} {hijo}")
                    data['resultado'][i] = 'No existe el host padre ni el hijo'
                    continue
                if not existe_padre:
                    print(f"no existe {padre} ")
                    data['resultado'][i] = 'No existe el host padre'
                    continue
                if not existe_hijo:
                    print(f"no existe {hijo}")
                    data['resultado'][i] = 'No existe el host hijo'
                    continue
                existe_relacion = text(
                    "SELECT correlarionid from host_correlation where hostidP=:padre and hostidC=:hijo")
                existe_relacion = pd.DataFrame(session.execute(
                    existe_relacion, {'padre': padre, 'hijo': hijo}))
                if not existe_relacion.empty:
                    data['resultado'][i] = 'La relacion ya existe'
                    continue
                host_correlation = HostCorrelation(
                    hostidP=padre,
                    hostidC=hijo,
                    session_id=current_session
                )
                session.add(host_correlation)
                session.commit()
                data['resultado'][i] = 'Guardado'
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
        xlsx_filename = temp_file.name
        with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:
            data.to_excel(writer, sheet_name="Resultados", index=False)
            return FileResponse(xlsx_filename, headers={"Content-Disposition": "attachment; filename=resultados.xlsx"}, media_type="application/vnd.ms-excel", filename="resultados.xlsx")
    """ return success_response(data={"filename": file.filename}) """


def existe(valor, lista):
    return valor in lista
