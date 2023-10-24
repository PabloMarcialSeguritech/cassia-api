from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from schemas import cassia_ci_schema
from schemas import cassia_ci_element_schema
import numpy as np
from utils.traits import success_response
from fastapi import HTTPException, status
from models.cassia_ci_element import CassiaCIElement
from models.cassia_ci_relations import CassiaCIRelation

from models.cassia_ci_document import CassiaCIDocument
from datetime import datetime
from fastapi.responses import FileResponse
import os
import ntpath
import shutil
settings = Settings()
abreviatura_estado = settings.abreviatura_estado


async def get_ci_elements():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""
    select cce.element_id,cce.ip,h.name,cce.technology ,cce.device_name,
cce.description,his.hardware_brand,his.hardware_model,his.software_version,
cce.location, cce.criticality, cce.status, cce.status_conf from cassia_ci_element cce 
join hosts h on h.hostid =cce.host_id 
join (
SELECT cch.element_id, cch.hardware_brand,cch.hardware_model,cch.software_version from cassia_ci_history cch
where cch.status="Cerrada"
order by closed_at  limit 1
) his on cce.element_id=his.element_id
WHERE deleted_at is NULL
    """)
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def create_ci_element(ci_element_data: cassia_ci_element_schema.CiElementBase, current_session):

    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(
        f"select hostid from hosts where hostid={ci_element_data.host_id}")
    host = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if len(host) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="host_id not exists")
    cassia_ci_element = CassiaCIElement(
        ip=ci_element_data.ip,
        host_id=ci_element_data.host_id,
        technology=ci_element_data.technology,
        device_name=ci_element_data.device_name,
        description=ci_element_data.description,
        location=ci_element_data.location,
        criticality=ci_element_data.criticality,
        status=ci_element_data.status,
        status_conf='Creado',
        session_id=current_session.session_id.hex
    )

    session.add(cassia_ci_element)
    session.commit()
    session.refresh(cassia_ci_element)
    session.close()
    return success_response(data=cassia_ci_element)


async def get_ci_element_relations(element_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    cassia_ci_element = text(f"""
    select * from cassia_ci_element where element_id='{element_id}'
    and deleted_at is NULL""")
    cassia_ci_element = pd.DataFrame(
        session.execute(cassia_ci_element)).replace(np.nan, "")
    if cassia_ci_element.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="CI Element not exists")
    query = text(f"""
    select cr.cassia_ci_relation_id,cr.folio,cr.depends_on_ci,cr.cassia_ci_element_id
from cassia_ci_relations cr WHERE cr.depends_on_ci='{element_id}'""")
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def create_ci_element_relation(element_id, affected_ci_element_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Element id not exist")
    affected_element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == affected_ci_element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not affected_element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Affected Element id not exist")

    relation = session.query(CassiaCIRelation).filter(
        CassiaCIRelation.depends_on_ci == element.element_id,
        CassiaCIRelation.cassia_ci_element_id == affected_element.element_id,
    ).first()

    if relation:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Relation already exist")

    cassia_ci_element_relation = CassiaCIRelation(
        cassia_ci_element_id=affected_element.element_id,
        depends_on_ci=element.element_id,
        folio=f"CI-{abreviatura_estado}-" +
        str(affected_element.element_id).zfill(5)
    )

    session.add(cassia_ci_element_relation)
    session.commit()
    session.refresh(cassia_ci_element_relation)
    session.close()

    session.close()
    return success_response(data=cassia_ci_element_relation)
""" para abajo antiguo """


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


def check_pdf(file_types):
    for type_file in file_types:
        if type_file != "application/pdf":
            return False
    return True


async def create_ci(ci_data: cassia_ci_schema.CiBase, files, current_session):
    if files:
        if len(files):
            type_files = [file.content_type for file in files]
            if not check_pdf(type_files):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Files must be only PDF")
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
                timestamp = str(datetime.now().timestamp()).replace(".", "")
                file_dest = os.path.join(
                    upload_dir, f"{timestamp}{file.filename}")

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
    if files:
        if len(files):
            type_files = [file.content_type for file in files]
            if not check_pdf(type_files):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Files must be only PDF")
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    ci = session.query(CassiaCI).filter(
        CassiaCI.ci_id == ci_id, CassiaCI.deleted_at == None).first()
    if not ci:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="ci_id not exists")

    query = text(f"select hostid from hosts where hostid={ci_data.host_id}")
    host = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if len(host) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="host_id not exists")
    if ci_data.doc_ids:
        cis_docs = session.query(CassiaCIDocument).filter(
            CassiaCIDocument.doc_id.not_in(ci_data.doc_ids.split(",")),
            CassiaCIDocument.ci_id == ci_id).all()

        for ci_doc in cis_docs:
            if os.path.exists(ci_doc.path):
                os.remove(ci_doc.path)
            session.delete(ci_doc)
            session.commit()

    ci.host_id = ci_data.host_id
    ci.ip = ci_data.ip
    ci.date = ci_data.date
    ci.responsible_name = ci_data.responsible_name
    ci.auth_name = ci_data.auth_name
    ci.session_id = current_session.session_id.hex
    ci.device_description = ci_data.device_description
    ci.justification = ci_data.justification
    ci.previous_state = ci_data.previous_state
    ci.new_state = ci_data.new_state
    ci.impact = ci_data.impact
    ci.observations = ci_data.observations
    ci.result = ci_data.result
    ci.status = ci_data.status
    ci.updated_at = datetime.now()

    session.commit()
    session.refresh(ci)

    if files:
        if len(files):
            for file in files:
                upload_dir = os.path.join(
                    os.getcwd(), f"uploads/cis/{ci.ci_id}")
                # Create the upload directory if it doesn't exist
                if not os.path.exists(upload_dir):
                    os.makedirs(upload_dir)
                # get the destination path

                timestamp = str(datetime.now().timestamp()).replace(".", "")
                file_dest = os.path.join(
                    upload_dir, f"{timestamp}{file.filename}")

                print(file_dest)

                # copy the file contents
                with open(file_dest, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                ci_document = CassiaCIDocument(
                    ci_id=ci.ci_id,
                    path=file_dest,
                    filename=file.filename
                )
                session.add(ci_document)
                session.commit()
                session.refresh(ci_document)

    session.close()
    return success_response(data=ci, message="CI updated successfully")


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
