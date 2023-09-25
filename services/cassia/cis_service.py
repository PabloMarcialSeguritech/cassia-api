from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from schemas import cassia_ci_schema
import numpy as np
from utils.traits import success_response
from fastapi import HTTPException, status
from models.cassia_ci import CassiaCI
from models.cassia_ci_document import CassiaCIDocument
from datetime import datetime
from fastapi.responses import FileResponse
import os
import ntpath
import shutil
settings = Settings()


async def get_cis():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""
    select cc.ci_id,cc.ip,h.name,cc.device_description ,cc.justification ,
cc.`result` ,cc.responsible_name ,cc.auth_name ,cc.`date`,cc.status from cassia_ci cc 
join hosts h on h.hostid =cc.host_id 
    WHERE deleted_at is NULL
    """)
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def get_host_by_ip(ip: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""
    SELECT hostid ,name FROM hosts h 
    where hostid in (select DISTINCT hostid from interface i 
    where ip = '{ip}')    
    """)
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def create_ci(ci_data: cassia_ci_schema.CiBase, files, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"select hostid from hosts where hostid={ci_data.host_id}")
    host = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if len(host) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="host_id not exists")
    date = ci_data.date

    cassia_ci = CassiaCI(
        host_id=ci_data.host_id,
        ip=ci_data.ip,
        date=ci_data.date,
        responsible_name=ci_data.responsible_name,
        auth_name=ci_data.auth_name,
        session_id=current_session.session_id.hex,
        device_description=ci_data.device_description,
        justification=ci_data.justification,
        previous_state=ci_data.previous_state,
        new_state=ci_data.new_state,
        impact=ci_data.impact,
        observations=ci_data.observations,
        result=ci_data.result,
        status=ci_data.status,
    )
    session.add(cassia_ci)
    session.commit()
    session.refresh(cassia_ci)
    if files:
        if len(files):
            for file in files:
                upload_dir = os.path.join(
                    os.getcwd(), f"uploads/cis/{cassia_ci.ci_id}")
                # Create the upload directory if it doesn't exist
                if not os.path.exists(upload_dir):
                    os.makedirs(upload_dir)
                # get the destination path
                file_dest = os.path.join(upload_dir, file.filename)
                print(file_dest)

                # copy the file contents
                with open(file_dest, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                ci_document = CassiaCIDocument(
                    ci_id=cassia_ci.ci_id,
                    path=file_dest,
                    filename=file.filename
                )
                session.add(ci_document)
                session.commit()
                session.refresh(ci_document)

    session.close()
    return success_response(data=cassia_ci)


async def update_ci(ci_id: str, ci_data: cassia_ci_schema.CiBase, files, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    ci = session.query(CassiaCI).filter(
        CassiaCI.ci_id == ci_id, CassiaCI.deleted_at == None).first()
    if not ci:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="ci_id not exists")
    print(ci_data)
    return success_response(data=ci)

    query = text(f"select hostid from hosts where hostid={ci_data.host_id}")
    host = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if len(host) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="host_id not exists")
    date = ci_data.date

    cassia_ci = CassiaCI(
        host_id=ci_data.host_id,
        ip=ci_data.ip,
        date=ci_data.date,
        responsible_name=ci_data.responsible_name,
        auth_name=ci_data.auth_name,
        session_id=current_session.session_id.hex,
        device_description=ci_data.device_description,
        justification=ci_data.justification,
        previous_state=ci_data.previous_state,
        new_state=ci_data.new_state,
        impact=ci_data.impact,
        observations=ci_data.observations,
        result=ci_data.result,
        status=ci_data.status,
    )
    session.add(cassia_ci)
    session.commit()
    session.refresh(cassia_ci)
    if files:
        if len(files):
            for file in files:
                upload_dir = os.path.join(
                    os.getcwd(), f"uploads/cis/{cassia_ci.ci_id}")
                # Create the upload directory if it doesn't exist
                if not os.path.exists(upload_dir):
                    os.makedirs(upload_dir)
                # get the destination path
                file_dest = os.path.join(upload_dir, file.filename)
                print(file_dest)

                # copy the file contents
                with open(file_dest, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                ci_document = CassiaCIDocument(
                    ci_id=cassia_ci.ci_id,
                    path=file_dest,
                    filename=file.filename
                )
                session.add(ci_document)
                session.commit()
                session.refresh(ci_document)

    session.close()
    return success_response(data=cassia_ci)


async def get_ci_by_id(ci_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci = session.query(CassiaCI).filter(
        CassiaCI.ci_id == ci_id, CassiaCI.deleted_at == None).first()
    if not ci:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ci_id not exists")
    files = session.execute(
        text(f"select * from cassia_ci_documents where ci_id={ci_id}"))
    ci_dict = ci.__dict__
    files = pd.DataFrame(files).replace(np.nan, "").to_dict(orient="records")

    files = {"files": files}
    response = ci_dict | files
    session.close()
    return success_response(data=response)


async def get_ci_document_by_id(ci_document_id: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci_document = session.query(CassiaCIDocument).filter(
        CassiaCIDocument.doc_id == ci_document_id).first()

    if not ci_document:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The document not exists",
        )
    session.close()
    if os.path.exists(ci_document.path):
        filename = ci_document.filename
        return FileResponse(path=ci_document.path, filename=filename)

    return success_response(
        message="File not found",
        success=False,
        status_code=status.HTTP_404_NOT_FOUND
    )


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


async def change_status(ci_id: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci = session.query(CassiaCI).filter(CassiaCI.ci_id == ci_id,
                                        CassiaCI.deleted_at == None).first()
    if not ci:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ci_id not exists")
    status_ci = "Activo" if ci.status == "Inactivo" else "Inactivo"
    ci.status = status_ci
    session.commit()
    session.refresh(ci)
    session.close()
    return success_response(message=f"Status changed to {status}", data=ci)


async def delete_ci(ci_id: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci = session.query(CassiaCI).filter(CassiaCI.ci_id == ci_id,
                                        CassiaCI.deleted_at == None).first()
    if not ci:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="ci_id not exists")
    ci.deleted_at = datetime.now()
    session.commit()
    session.refresh(ci)
    session.close()
    return success_response(message=f"Ci deleted successfully", )
