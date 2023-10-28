from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
import numpy as np
from utils.traits import success_response
from fastapi import HTTPException, status
from models.cassia_ci_element import CassiaCIElement
from models.cassia_ci_relations import CassiaCIRelation
from models.cassia_ci_history import CassiaCIHistory
from models.cassia_ci_document import CassiaCIDocument
from datetime import datetime
from fastapi.responses import FileResponse
import os
import ntpath
import shutil
settings = Settings()
abreviatura_estado = settings.abreviatura_estado


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


async def get_ci_elements():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""
    select cce.element_id,cce.folio,cce.ip,h.name,cce.technology ,cce.device_name,
cce.description,his.hardware_brand,his.hardware_model,his.software_version,
cce.location, cce.criticality, cce.status, cce.status_conf from cassia_ci_element cce 
left join hosts h on h.hostid =cce.host_id 
left join (
SELECT cch.element_id, cch.hardware_brand,cch.hardware_model,cch.software_version from cassia_ci_history cch
where cch.status="Cerrada" and cch.deleted_at is NULL
order by closed_at desc limit 1
) his on cce.element_id=his.element_id
WHERE deleted_at is NULL
    """)
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")

    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def get_ci_element(element_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"""select cce.*, h.name from cassia_ci_element cce 
left join hosts h on h.hostid =cce.host_id 
where cce.element_id='{element_id}' and cce.deleted_at is NULL""")
    element = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    if element.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="CI Element not exists"
        )
    session.close()
    return success_response(data=element.to_dict(orient="records")[0])


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
        folio=ci_element_data.folio,
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


async def update_ci_element(element_id: str, ci_element_data: cassia_ci_element_schema.CiElementBase, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="CI Element not exists")
    query = text(
        f"select hostid from hosts where hostid={ci_element_data.host_id}")
    host = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if len(host) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="host_id not exists")
    element.ip = ci_element_data.ip
    element.technology = ci_element_data.technology
    element.folio = ci_element_data.folio
    element.description = ci_element_data.description
    element.criticality = ci_element_data.criticality
    element.session_id = current_session.session_id.hex
    element.device_name = ci_element_data.device_name
    element.host_id = ci_element_data.host_id
    element.location = ci_element_data.location
    element.status = ci_element_data.status
    element.updated_at = datetime.now()
    session.commit()
    session.refresh(element)
    session.close()

    return success_response(data=element, message="Elementento de Configuración actualizado correctamente")


async def delete_ci_element(element_id: str, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="CI Element not exists")

    element.session_id = current_session.session_id.hex
    element.updated_at = datetime.now()
    element.deleted_at = datetime.now()
    session.commit()
    session.refresh(element)
    session.close()

    return success_response(data=element, message="Elementento de Configuración eliminado correctamente")


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
from cassia_ci_relations cr WHERE cr.depends_on_ci='{element_id}'
and cr.cassia_ci_element_id in (SELECT c.element_id from 
cassia_ci_element c left join cassia_ci_relations ccr
on c.element_id= ccr.cassia_ci_element_id 
where c.deleted_at is NULL
)""")
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def get_ci_element_catalog():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    catalog = text(f"""
    select element_id,folio from cassia_ci_element where deleted_at is NULL""")
    catalog = pd.DataFrame(
        session.execute(catalog)).replace(np.nan, "")
    if catalog.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="CI Elements not exists")
    catalog['id'] = catalog['element_id']
    catalog['value'] = catalog['element_id']

    session.close()
    return success_response(data=catalog.to_dict(orient="records"))


async def create_ci_element_relation(element_id, affected_ci_element_id):
    if element_id == affected_ci_element_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El elemento no puede relacionarse consigo mismo")
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
        folio=affected_element.folio
    )

    session.add(cassia_ci_element_relation)
    session.commit()
    session.refresh(cassia_ci_element_relation)
    session.close()
    return success_response(data=cassia_ci_element_relation)


async def delete_ci_element_relation(cassia_ci_relation_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    relation = session.query(CassiaCIRelation).filter(
        CassiaCIRelation.cassia_ci_relation_id == cassia_ci_relation_id
    ).first()

    if not relation:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Relation not exist")
    session.delete(relation)
    session.commit()
    session.close()
    return success_response(message="Relacion eliminada correctamente")


async def get_ci_element_docs(element_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Element id not exist")

    docs = session.query(CassiaCIDocument).filter(
        CassiaCIDocument.element_id == element_id
    ).all()

    session.close()
    return success_response(data=docs)


async def upload_ci_element_docs(element_id, files):
    type_files = [file.content_type for file in files]
    if not check_pdf(type_files):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Files must be only PDF")
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Element id not exist")
    for file in files:
        upload_dir = os.path.join(
            os.getcwd(), f"uploads/cis/{element.element_id}")
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
            element_id=element.element_id,
            path=file_dest,
            filename=file.filename
        )
        session.add(ci_document)
        session.commit()
        session.refresh(ci_document)
    session.close()
    return success_response(message="Archivos subidos correctamente")


async def download_ci_element_doc(doc_id: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci_document = session.query(CassiaCIDocument).filter(
        CassiaCIDocument.doc_id == doc_id).first()

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


async def delete_ci_element_doc(doc_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci_document = session.query(CassiaCIDocument).filter(
        CassiaCIDocument.doc_id == doc_id).first()
    if not ci_document:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The document not exists",
        )
    if os.path.exists(ci_document.path):
        os.remove(ci_document.path)
    session.delete(ci_document)
    session.commit()
    return success_response(message='Documento eliminado correctamente')


async def get_ci_element_history(element_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    query = text(f"""
    select cce.element_id,cce.folio,cce.ip,h.name,cce.technology ,cce.device_name,
cce.description,his.hardware_brand,his.hardware_model,his.software_version,
cce.location, cce.criticality, cce.status, cce.status_conf from cassia_ci_element cce 
left join hosts h on h.hostid =cce.host_id 
left join (
SELECT cch.element_id, cch.hardware_brand,cch.hardware_model,cch.software_version from cassia_ci_history cch
where cch.status="Cerrada" and cch.deleted_at is NULL
order by closed_at desc limit 1
) his on cce.element_id=his.element_id
WHERE deleted_at is NULL
    """)
    ci_element = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if ci_element.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The CI Element not exists",
        )
    ci_element = ci_element.to_dict(orient='records')[0]
    history = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.element_id == element_id,
        CassiaCIHistory.deleted_at == None
    ).all()
    """ results['folio'] = results['element_id'].apply(
        lambda x: f"CI-{abreviatura_estado}-{str(x).zfill(5)}") """
    session.close()

    response = ci_element
    history = {'history': history}
    response.update(history)
    return success_response(data=response)


async def get_ci_element_history_detail(history_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    history = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.conf_id == history_id,
        CassiaCIHistory.deleted_at == None
    ).first()
    if not history:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The CI Element not exists",
        )
    session.close()

    return success_response(data=history)


async def create_ci_history_record(ci_element_history_data: cassia_ci_history_schema, current_session):

    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == ci_element_history_data.element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Element CI not exists")
    ci_element_history = CassiaCIHistory(
        element_id=ci_element_history_data.element_id,
        change_type=ci_element_history_data.change_type,
        description=ci_element_history_data.description,
        justification=ci_element_history_data.justification,
        hardware_no_serie=ci_element_history_data.hardware_no_serie,
        hardware_brand=ci_element_history_data.hardware_brand,
        hardware_model=ci_element_history_data.hardware_model,
        software_version=ci_element_history_data.software_version,
        responsible_name=ci_element_history_data.responsible_name,
        auth_name=ci_element_history_data.auth_name,
        created_at=ci_element_history_data.created_at,
        closed_at=ci_element_history_data.closed_at,
        session_id=current_session.session_id.hex,
        status=ci_element_history_data.status
    )
    match ci_element_history_data.status:
        case 'Iniciada':
            element.status_conf = 'Sin cerrar'

    session.add(ci_element_history)
    session.commit()
    session.refresh(ci_element_history)
    session.refresh(element)
    session.close()
    return success_response(data=ci_element_history)


async def update_ci_history_record(ci_element_history_id, ci_element_history_data: cassia_ci_history_schema, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == ci_element_history_data.element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if not element:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Element CI not exists")
    ci_element_history = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.conf_id == ci_element_history_id,
        CassiaCIHistory.deleted_at == None
    ).first()
    ci_element_history.element_id = ci_element_history_data.element_id,
    ci_element_history.change_type = ci_element_history_data.change_type,
    ci_element_history.description = ci_element_history_data.description,
    ci_element_history.justification = ci_element_history_data.justification,
    ci_element_history.hardware_no_serie = ci_element_history_data.hardware_no_serie,
    ci_element_history.hardware_brand = ci_element_history_data.hardware_brand,
    ci_element_history.hardware_model = ci_element_history_data.hardware_model,
    ci_element_history.software_version = ci_element_history_data.software_version,
    ci_element_history.responsible_name = ci_element_history_data.responsible_name,
    ci_element_history.auth_name = ci_element_history_data.auth_name,
    ci_element_history.created_at = ci_element_history_data.created_at,
    ci_element_history.closed_at = ci_element_history_data.closed_at,
    ci_element_history.session_id = current_session.session_id.hex,
    ci_element_history.status = ci_element_history_data.status

    ci_element_histories = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.element_id == ci_element_history_data.element_id,
        CassiaCIHistory.deleted_at == None,
        CassiaCIHistory.status == 'Iniciada'
    ).first()
    if ci_element_histories:
        element.status_conf = 'Sin cerrar'
    else:
        element.status_conf = 'Cerradas'
    session.commit()
    session.refresh(element)
    session.refresh(ci_element_history)
    session.close()

    return success_response(data=ci_element_history)


async def delete_ci_history_record(ci_element_history_id,  current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    ci_element_history = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.conf_id == ci_element_history_id,
        CassiaCIHistory.deleted_at == None
    ).first()
    if not ci_element_history:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Element CI History not exists")
    ci_element_history.deleted_at = datetime.now()
    ci_element_history.session_id = current_session.session_id.hex
    ci_element_history.updated_at = datetime.now()

    ci_element_histories = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.element_id == ci_element_history.element_id,
        CassiaCIHistory.deleted_at == None,
        CassiaCIHistory.status == 'Iniciada'
    ).first()
    element = session.query(CassiaCIElement).filter(
        CassiaCIElement.element_id == ci_element_history.element_id,
        CassiaCIElement.deleted_at == None
    ).first()
    if element:
        if ci_element_histories:
            element.status_conf = 'Sin cerrar'
        else:
            element.status_conf = 'Cerradas'
        session.commit()
        session.refresh(element)
    return success_response(message='El registro de historial de configuración eliminado correctamente.')


def check_pdf(file_types):
    for type_file in file_types:
        if type_file != "application/pdf":
            return False
    return True


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)
