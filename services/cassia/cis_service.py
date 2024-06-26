from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from schemas import cassia_ci_history_schema
from schemas import cassia_ci_element_schema
import numpy as np
from utils.traits import success_response
from utils.traits import failure_response
from fastapi import HTTPException, status, UploadFile, File
from models.cassia_ci_element import CassiaCIElement
from models.cassia_ci_relations import CassiaCIRelation
from models.cassia_ci_history import CassiaCIHistory
from models.cassia_ci_document import CassiaCIDocument
from models.cassia_mail import CassiaMail
from models.cassia_user_authorizer import UserAuthorizer
from models.user_model import User
from datetime import datetime
from fastapi.responses import FileResponse
import os
import ntpath
import shutil
import starlette
from infraestructure.cassia import cassia_ci_repository
settings = Settings()
abreviatura_estado = settings.abreviatura_estado


async def get_ci_elements_technologies():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""call sp_catCiTechnology()""")
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if not results.empty:
        results['id'] = results['tech_id']
        results['value'] = results['technology']
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
    hosts = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    query = text(f"""call sp_cassiaCILocation('{ip}')""")
    locations = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    results = pd.DataFrame()
    if not locations.empty:
        results = pd.merge(hosts, locations, how="left", on="hostid")
    else:
        if not hosts.empty:
            results = hosts
            results['location'] = ["Sin coordenadas" for i in len(hosts)]
    """ print(hosts.to_string())
    print(locations.to_string()) """
    """ response = dict()
    response.update(resume)
    response.update({'history': acks.to_dict(orient="records")})
    response.update({'tickets': tickets.to_dict(orient='records')}) """
    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def get_ci_elements():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""
    SELECT 
    cce.element_id,
    cce.folio,
    cce.ip,
    h.name,
    cce.technology,
    cce.device_name,
    cce.description,
    cce.referencia,
    his.hardware_brand,
    his.hardware_model,
    his.software_version,
    his.hardware_no_serie,
    cce.location,
    cce.criticality,
    cce.status,
    cce.status_conf,
    cct.tech_name 
FROM 
    cassia_ci_element cce
LEFT JOIN 
    hosts h ON h.hostid = cce.host_id
LEFT JOIN 
    cassia_ci_tech cct ON cct.tech_id = cce.tech_id
LEFT JOIN 
    (
        SELECT 
            cch.element_id,
            cch.hardware_brand,
            cch.hardware_model,
            cch.software_version,
            cch.hardware_no_serie 
        FROM 
            (
                SELECT 
                    element_id,
                    hardware_brand,
                    hardware_model,
                    software_version,
                    hardware_no_serie,
                    ROW_NUMBER() OVER (PARTITION BY element_id ORDER BY closed_at DESC) AS rn
                FROM 
                    cassia_ci_history
                WHERE 
                    status = "Cerrada" 
                    AND deleted_at IS NULL
            ) cch
        WHERE 
            rn = 1
    ) his ON cce.element_id = his.element_id
WHERE 
    cce.deleted_at IS NULL
    """)
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")

    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def get_ci_element(element_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"""select cce.*,cct.tech_name, h.name from cassia_ci_element cce
left join hosts h on h.hostid =cce.host_id
left join cassia_ci_tech cct on cct.tech_id=cce.tech_id
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
    query = text(
        f"select tech_id from cassia_ci_tech where tech_id={ci_element_data.tech_id}")
    tech = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if len(tech) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="tech_id not exists")
    cassia_ci_element = CassiaCIElement(
        ip=ci_element_data.ip,
        host_id=ci_element_data.host_id,
        folio="",
        device_name=ci_element_data.device_name,
        description=ci_element_data.description,
        location=ci_element_data.location,
        criticality=ci_element_data.criticality,
        status=ci_element_data.status,
        status_conf='Creado',
        session_id=current_session.session_id.hex,
        tech_id=ci_element_data.tech_id,
        referencia=ci_element_data.referencia
    )

    session.add(cassia_ci_element)
    session.commit()
    session.refresh(cassia_ci_element)
    folio = cassia_ci_element.element_id
    folio = str(folio).zfill(5)
    cassia_ci_element.folio = f"CI-{abreviatura_estado}-{folio}"
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
    query = text(
        f"select tech_id from cassia_ci_tech where tech_id={ci_element_data.tech_id}")
    tech = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if len(tech) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="tech_id not exists")
    element.ip = ci_element_data.ip

    element.description = ci_element_data.description
    element.criticality = ci_element_data.criticality
    element.session_id = current_session.session_id.hex
    element.device_name = ci_element_data.device_name
    element.host_id = ci_element_data.host_id
    element.location = ci_element_data.location
    element.status = ci_element_data.status
    element.updated_at = datetime.now()
    element.tech_id = ci_element_data.tech_id
    element.referencia = ci_element_data.referencia
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


async def get_ci_element_docs_(element_id):
    element = await cassia_ci_repository.get_ci_element(element_id)
    if element.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El elemento de configuracion(CI) no existe.")
    docs = await cassia_ci_repository.get_ci_element_docs(element_id)

    return success_response(data=docs.to_dict(orient='records'))


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


async def upload_ci_element_docs_(element_id, files, file_names):
    element = await cassia_ci_repository.get_ci_element(element_id)
    if element.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El elemento de configuracion(CI) no existe")
    for item in files:
        if type(item) == starlette.datastructures.UploadFile:
            if not check_pdf([item.content_type]):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail="Los archivos pueden ser solo tipo PDF")
    for i in range(len(files)):
        if type(files[i]) == starlette.datastructures.UploadFile:
            await cassia_ci_repository.save_document(element['element_id'][0], files[i], True, file_names[i])
        elif type(files[i]) == str:
            await cassia_ci_repository.save_document(element['element_id'][0], files[i], False, file_names[i])

    return success_response(message="Archivos guardados correctamente")


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


async def download_ci_element_doc_(doc_id: str):
    ci_document = await cassia_ci_repository.get_ci_element_doc_by_id(doc_id)
    if ci_document.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el documento",
        )
    if int(ci_document['is_link'][0]) == 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El documento es de tipo enlace",
        )
    doc_path = ci_document['path'][0]
    if os.path.exists(doc_path):
        filename = ci_document['filename'][0]
        return FileResponse(path=doc_path, filename=filename)

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


async def delete_ci_element_doc_(doc_id):
    ci_document = await cassia_ci_repository.get_ci_element_doc_by_id(doc_id)

    if ci_document.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El documento no existe",
        )
    result = await cassia_ci_repository.delete_document(ci_document)
    if result:
        return success_response(message='Documento eliminado correctamente')
    return success_response(message='Error al eliminar el documento')


async def get_ci_element_history(element_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    query = text(f"""
    select cce.element_id,cce.folio,cce.ip,h.name,cce.technology ,cce.device_name,
cce.description,his.hardware_brand,his.hardware_model,his.software_version,
cce.location, cce.criticality, cce.status, cce.status_conf,cce.referencia,cce.tech_id, cct.tech_name from cassia_ci_element cce
left join hosts h on h.hostid =cce.host_id
left join cassia_ci_tech cct on cct.tech_id=cce.tech_id
left join (
SELECT cch.element_id, cch.hardware_brand,cch.hardware_model,cch.software_version from cassia_ci_history cch
where cch.status="Cerrada" and cch.deleted_at is NULL
order by closed_at desc limit 1
) his on cce.element_id=his.element_id
WHERE deleted_at is NULL
and cce.element_id={element_id}
    """)
    ci_element = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    if ci_element.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The CI Element not exists",
        )
    ci_element = ci_element.to_dict(orient='records')[0]
    query = text(f"""CALL sp_ci_auth_comments('{element_id}')""")
    history = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    session.close()
    response = ci_element
    history = {'history': history.to_dict(orient='records')}
    response.update(history)
    return success_response(data=response)


async def get_ci_element_history_detail(history_id):
    with DB_Zabbix().Session() as session:
        query = text(f"""select cch.*,cm.comments as request_comments, cm.action_comments as authorization_comments from cassia_ci_history cch
left join (select cms.mail_id,cms.cassia_conf_id,cms.comments,cms.action_comments from cassia_mail cms where cms.cassia_conf_id={history_id} order by cms.mail_id desc limit 1)
cm on cm.cassia_conf_id = cch.conf_id
where cch.deleted_at is Null and
cch.conf_id={history_id}""")
        history = pd.DataFrame(session.execute(query)).replace(np.nan, "")
        if history.empty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The CI Element not exists",
            )
    return success_response(data=history.to_dict(orient='records')[0])


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
        session_id=current_session.session_id.hex,
        ticket=ci_element_history_data.ticket

    )

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
    if not ci_element_history:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="La configuracion no existe")
    match ci_element_history.status:
        case "No iniciado":
            ci_element_history.element_id = ci_element_history_data.element_id
            ci_element_history.change_type = ci_element_history_data.change_type
            ci_element_history.description = ci_element_history_data.description
            ci_element_history.justification = ci_element_history_data.justification
            ci_element_history.hardware_no_serie = ci_element_history_data.hardware_no_serie
            ci_element_history.hardware_brand = ci_element_history_data.hardware_brand
            ci_element_history.hardware_model = ci_element_history_data.hardware_model
            ci_element_history.software_version = ci_element_history_data.software_version
            ci_element_history.responsible_name = ci_element_history_data.responsible_name
            ci_element_history.auth_name = ci_element_history_data.auth_name
            ci_element_history.ticket = ci_element_history_data.ticket
            """ ci_element_history.created_at = ci_element_history_data.created_at """
            """ ci_element_history.closed_at = ci_element_history_data.closed_at """
            ci_element_history.session_id = current_session.session_id.hex
        case "Pendiente de autorizacion":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="No se puede actulizar un registro pendiente de autorización, para actualizar por favor cancele la solicitud")
        case "Iniciado":
            ci_element_histories = session.query(CassiaCIHistory).filter(
                CassiaCIHistory.element_id == ci_element_history_data.element_id,
                CassiaCIHistory.deleted_at == None,
                CassiaCIHistory.status == 'Iniciado',
                CassiaCIHistory.conf_id != ci_element_history_id
            ).first()
            if ci_element_history_data.status == "Cerrada":
                ci_element_history.closed_at = ci_element_history_data.closed_at
                print("si entra")
                print(ci_element_histories)
                if ci_element_histories:
                    print("si entra 1")
                    element.status_conf = 'Sin cerrar'
                else:
                    print("si entra 2")
                    print(element.status_conf)
                    element.status_conf = 'Cerradas'
                    print(element.status_conf)
            if ci_element_history_data.status == "Cancelada":
                ci_element_history.closed_at = ci_element_history_data.closed_at
                if ci_element_histories:
                    element.status_conf = 'Sin cerrar'
                else:
                    element.status_conf = 'Cerradas'
            print("aqui")
            print("actual", ci_element_history.status)
            print("nuevo", ci_element_history_data.status)

            ci_element_history.status = ci_element_history_data.status
            ci_element_history.session_id = current_session.session_id.hex

    session.commit()
    session.refresh(element)
    session.refresh(ci_element_history)
    session.close()
    print(element.status)
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
    if ci_element_history.status != "No iniciado":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="No puedes eliminar configuraciones autorizadas, canceladas o cerradas.")
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


async def get_ci_process():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text("SELECT process_id, name from cassia_ci_process")
    process_catalog = pd.DataFrame(
        session.execute(statement)).replace(np.nan, "")
    if not process_catalog.empty:
        process_catalog["id"] = process_catalog["process_id"]
        process_catalog["value"] = process_catalog["process_id"]
    session.close()
    return success_response(data=process_catalog.to_dict(orient="records"))


async def create_authorization_request(ci_element_history_id, ci_authorization, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci_element_history = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.conf_id == ci_element_history_id,
        CassiaCIHistory.deleted_at == None
    ).first()
    if not ci_element_history:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El elemento de configuracion no existe")
    if ci_element_history.status != "No iniciado":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Solo puedes crear solicitudes en los elementos de configuracion con estatus 'No iniciado'")

    ci_authorization_exist = session.query(CassiaMail).filter(
        CassiaMail.cassia_conf_id == ci_element_history_id,
        CassiaMail.autorizer_user_id == None,
        CassiaMail.action == None
    ).first()
    if ci_authorization_exist:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="La solicitud ya existe")
    ci_mail = CassiaMail(
        request_user_id=current_session.user_id,
        process_id=ci_authorization.process_id,
        comments=ci_authorization.comments,
        request_date=datetime.now(),
        cassia_conf_id=ci_element_history_id
    )
    ci_element_history.status = 'Pendiente de autorizacion'
    session.add(ci_mail)
    session.commit()
    session.refresh(ci_mail)
    session.close()
    return success_response(data=ci_mail, message="Solicitud creada correctamente")


async def cancel_authorization_request(ci_element_history_id, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    ci_element_history = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.conf_id == ci_element_history_id,
        CassiaCIHistory.deleted_at == None
    ).first()
    if not ci_element_history:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El elemento de configuracion no existe")
    if ci_element_history.status != "Pendiente de autorizacion":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Solo puedes cancelar solicitudes en los elementos de configuracion con estatus 'Pendiente de autorizacion'")

    ci_authorization = session.query(CassiaMail).filter(
        CassiaMail.cassia_conf_id == ci_element_history_id,
        CassiaMail.autorizer_user_id == None,
        CassiaMail.action == None
    ).first()
    if not ci_authorization:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="No hay solicitudes creadas.")
    ci_authorization.action = 0
    ci_authorization.action_comments = 'Cancelada por el solicitante'
    ci_element_history.status = "No iniciado"
    session.commit()
    session.refresh(ci_authorization)
    session.close()
    return success_response(data=ci_authorization, message="Solicitud cancelada correctamente")


async def get_authorization_requests(current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    authorizer = session.query(UserAuthorizer).filter(
        UserAuthorizer.user_id == current_session.user_id).first()
    if not authorizer:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="No tienes permiso para autorizar una solicitud")
    statement = text("""select h.host,cce.folio,cce.element_id as ci_id,cch.conf_id as configuration_id, cm.mail_id,re.name as user_request,cm.request_date,pr.name as process_name,cm.comments  from cassia_mail cm left join
cassia_users re on re.user_id =cm.request_user_id
left join cassia_ci_process pr on pr.process_id = cm.process_id
left join cassia_ci_history cch on cch.conf_id = cm.cassia_conf_id
left join cassia_ci_element cce  on cce.element_id =cch.element_id
left join hosts h on h.hostid = cce.host_id
                     where action IS NULL""")
    requests = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
    if not requests.empty:
        requests['request_date'] = pd.to_datetime(requests.request_date)
        requests['request_date'] = requests['request_date'].dt.strftime(
            '%d/%m/%Y %H:%M:%S')

    session.close()
    return success_response(data=requests.to_dict("records"))


async def authorize_request(cassia_mail_id, ci_authorization_data, current_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    authorizer = session.query(UserAuthorizer).filter(
        UserAuthorizer.user_id == current_session.user_id).first()
    if not authorizer:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="No tienes permiso para autorizar una solicitud")

    ci_authorization_get = session.query(CassiaMail).filter(
        CassiaMail.mail_id == cassia_mail_id,
    ).first()
    if not ci_authorization_get:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="La solicitud no existe")
    ci_element_history = session.query(CassiaCIHistory).filter(
        CassiaCIHistory.conf_id == ci_authorization_get.cassia_conf_id).first()
    if not ci_element_history:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El elemento de configuracion no existe")
    if ci_authorization_data.action:
        ci_element_history.status = "Iniciado"
        ci_element_history.created_at = datetime.now()
    else:
        ci_element_history.status = 'Rechazada'
        ci_element = session.query(CassiaCIElement).filter(
            CassiaCIElement.element_id == ci_element_history.element_id).first()

        ci_element_histories = session.query(CassiaCIHistory).filter(
            CassiaCIHistory.element_id == ci_element.element_id,
            CassiaCIHistory.deleted_at == None,
            CassiaCIHistory.status == 'Iniciado',
            CassiaCIHistory.conf_id != ci_element_history.conf_id
        ).first()
        if ci_element_histories:
            ci_element.status_conf = 'Sin cerrar'
        else:
            ci_element.status_conf = 'Cerradas'
    ci_authorization_get.action = ci_authorization_data.action
    ci_authorization_get.action_comments = ci_authorization_data.action_comments
    ci_authorization_get.autorizer_user_id = current_session.user_id
    user = session.query(User).filter(
        User.user_id == current_session.user_id).first()
    if user:
        ci_element_history.auth_name = user.name

    session.commit()
    session.refresh(ci_authorization_get)
    session.close()
    return success_response(data=ci_authorization_get, message="Solicitud actualizada correctamente")
