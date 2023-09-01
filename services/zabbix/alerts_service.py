from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_Prueba
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from models.exception_agency import ExceptionAgency
from models.exceptions import Exceptions as ExceptionModel
from models.problem_record import ProblemRecord
from models.problem_records_history import ProblemRecordHistory
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
import numpy as np
from datetime import datetime
from utils.traits import success_response
from fastapi import status, File, UploadFile
from fastapi.responses import FileResponse
import os
import ntpath
import shutil
settings = Settings()


def get_problems_filter(municipalityId, tech_host_type=0, subtype=""):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    if tech_host_type == "11":
        tech_host_type = "11,2"
    if subtype != "" and tech_host_type == "":
        tech_host_type = "0"
    statement = text(
        f"call sp_viewProblem('{municipalityId}','{tech_host_type}','{subtype}')")

    problems = session.execute(statement)
    data = pd.DataFrame(problems).replace(np.nan, "")

    """ statement = text(
        "SELECT problemid,estatus FROM problem_records where estatus!='Cerrado'")
    problem_records = db_zabbix.Session().execute(statement)
    problem_records = pd.DataFrame(problem_records).replace(np.nan, "")
    if len(problem_records) < 1:
        problemids = "(0)"
    else:
        if len(problem_records) == 1:
            problemids = f"({problem_records.iloc[0]['problemid']})"
        else:
            problemids = problem_records["problemid"].values.tolist()
            problemids = tuple(problemids)
    statement = text(
        f"call sp_verificationProblem('{municipalityId}','{tech_host_type}','{subtype}','{problemids}')")
    problems = db_zabbix.Session().execute(statement)
    # call sp_verificationProblem('0','','','(1,2,3,4)');
    db_zabbix.Session().close()
    db_zabbix.stop()

    # print(problems)
    data = pd.DataFrame(problems)
    data = data.replace(np.nan, "")
    data["estatus"] = ""
    for ind in data.index:
        record = problem_records.loc[problem_records['problemid']
                                     == data['eventid'][ind]]
        data['estatus'][ind] = record.iloc[0]['estatus']
 """
    session.close()
    return success_response(data=data.to_dict(orient="records"))


""" Exception Agencies """


def get_exception_agencies():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    try:
        rows = session.query(ExceptionAgency).filter(
            ExceptionAgency.deleted_at == None).all()
    finally:
        session.close()
        db_zabbix.stop()

    return success_response(data=rows)


def create_exception_agency(exception_agency: exception_agency_schema.ExceptionAgencyBase):
    db_zabbix = DB_Zabbix()
    exception_agency_new = ExceptionAgency(
        name=exception_agency.name,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )
    session = db_zabbix.Session()
    session.add(exception_agency_new)
    session.commit()
    session.refresh(exception_agency_new)
    session.close()
    db_zabbix.stop()
    return success_response(message="Registro guardado correctamente",
                            success=True,
                            data=exception_agency_schema.ExceptionAgency(
                                exception_agency_id=exception_agency_new.exception_agency_id,
                                name=exception_agency_new.name,
                                created_at=exception_agency_new.created_at,
                                updated_at=exception_agency_new.updated_at,
                                deleted_at=exception_agency_new.deleted_at
                            ),
                            status_code=status.HTTP_201_CREATED)


def update_exception_agency(exception_agency_id: int, exception_agency: exception_agency_schema.ExceptionAgencyBase):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(ExceptionAgency).filter(
        ExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    if exception_agency_search.name == exception_agency.name:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Update at least one field"
        )
    exception_agency_search.name = exception_agency.name
    exception_agency_search.updated_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Updated",
                            data=exception_agency_search)


def delete_exception_agency(exception_agency_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(ExceptionAgency).filter(
        ExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    exception_agency_search.deleted_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Deleted")


""" Exceptions """


def get_exceptions():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    try:
        rows = session.query(ExceptionModel).filter(
            ExceptionModel.deleted_at == None).all()
    finally:
        session.close()
        db_zabbix.stop()

    return success_response(data=rows)


def create_exception(exception: exception_schema.ExceptionsBase, current_user_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    problem_record = session.query(ProblemRecord).filter(
        ProblemRecord.problemid == exception.problemid and exception.deleted_at is None).first()
    if not problem_record:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Problem Record not exists",
        )
    if problem_record.estatus == "Excepcion":
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Exception already exists",
        )
    new_exception = ExceptionModel(
        problemrecord_id=problem_record.problemrecord_id,
        exception_agency_id=exception.exception_agency_id,
        description=exception.description,
        created_at=datetime.now(),
        user_id=current_user_id
    )
    session.add(new_exception)
    problem_record.estatus = "Excepcion"
    problem_record.closed_at = datetime.now()
    session.commit()
    session.refresh(problem_record)
    session.refresh(new_exception)
    session.close()
    db_zabbix.stop()
    respose = {
        "exception": new_exception,
        "problem_record": problem_record
    }
    return success_response(message="Excepcion creada correctamente",
                            data=respose,
                            status_code=status.HTTP_201_CREATED)


def update_exception(exception_agency_id: int, exception_agency: exception_agency_schema.ExceptionAgencyBase):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(ExceptionAgency).filter(
        ExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    if exception_agency_search.name == exception_agency.name:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Update at least one field"
        )
    exception_agency_search.name = exception_agency.name
    exception_agency_search.updated_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Updated",
                            data=exception_agency_search)


def delete_exception(exception_agency_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    exception_agency_search = session.query(ExceptionAgency).filter(
        ExceptionAgency.exception_agency_id == exception_agency_id).first()
    if not exception_agency_search:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    exception_agency_search.deleted_at = datetime.now()
    session.commit()
    session.refresh(exception_agency_search)
    session.close()
    db_zabbix.stop()
    return success_response(message="Exception Agency Deleted")


""" Change status """


def change_status(problemid: int, estatus: str, current_user_id: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    problem_record = session.query(ProblemRecord).filter(
        ProblemRecord.problemid == problemid).first()
    if not problem_record:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Problem Record not exists",
        )
    if problem_record.estatus == "Excepcion":
        excepcion = session.query(ExceptionModel).filter(
            ExceptionModel.problemrecord_id == problem_record.problemrecord_id).first()
        excepcion.deleted_at = datetime.now()
        session.commit()
        session.refresh(excepcion)
    match estatus:
        case "En curso":
            problem_record.estatus = "En curso"
            if problem_record.taken_at is None:
                problem_record.taken_at = datetime.now()
                problem_record.user_id = current_user_id
        case "Cerrado":
            problem_record.closed_at = datetime.now()
            if problem_record.taken_at is None:
                problem_record.taken_at = datetime.now()
                problem_record.user_id = current_user_id
            problem_record.estatus = "Cerrado"
        case "Soporte 2do Nivel":
            if problem_record.taken_at is None:
                problem_record.taken_at = datetime.now()
                problem_record.user_id = current_user_id
            problem_record.estatus = "Soporte 2do Nivel"

    session.commit()
    session.refresh(problem_record)
    session.close()
    return success_response(message="Estatus actualizado correctamente",
                            data=problem_record)


async def create_message(problemid: int, message: str | None, current_user_id: int, file: UploadFile | None):
    if message is None and file is None:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The request must contain a message or a File",
        )
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    problem = session.query(ProblemRecord).filter(
        ProblemRecord.problemid == problemid).first()
    if not problem:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Problem not exists",
        )
    file_dest = None
    if file:
        upload_dir = os.path.join(
            os.getcwd(), f"uploads/{problem.problemrecord_id}")
        # Create the upload directory if it doesn't exist
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)

            # get the destination path
        file_dest = os.path.join(upload_dir, file.filename)
        print(file_dest)

        # copy the file contents
        with open(file_dest, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

    prh = ProblemRecordHistory(
        problemrecord_id=problem.problemrecord_id,
        user_id=current_user_id,
        message=message,
        file_route=file_dest,
        created_at=datetime.now()
    )
    session.add(prh)
    session.commit()
    session.refresh(prh)
    session.close()
    return success_response(message="Mensaje guardado correctamente",
                            data=prh)


async def get_messages(problemid: int):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    problem = session.query(ProblemRecord).filter(
        ProblemRecord.problemid == problemid).first()
    if not problem:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Problem not exists",
        )
    history = session.query(ProblemRecordHistory).filter(
        ProblemRecordHistory.problemrecord_id == problem.problemrecord_id,
        ProblemRecordHistory.deleted_at == None
    ).all()
    session.close()
    db_zabbix.stop()
    return success_response(
        data=history)


async def download_file(message_id: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    message_file = session.query(ProblemRecordHistory).filter(
        ProblemRecordHistory.problemsHistory_id == message_id).first()

    if not message_file:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The message not exists",
        )

    if not message_file.file_route:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The file not exists",
        )

    if os.path.exists(message_file.file_route):
        filename = path_leaf(message_file.file_route)
        return FileResponse(path=message_file.file_route, filename=filename)
    session.close()
    return success_response(
        message="File not found",
        success=False,
        status_code=status.HTTP_404_NOT_FOUND
    )


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)
