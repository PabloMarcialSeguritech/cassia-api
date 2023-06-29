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
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
import numpy as np
from datetime import datetime
from utils.traits import success_response
from fastapi import status
settings = Settings()


def get_problems_filter(municipalityId, tech, hostType):
    db_zabbix = DB_Zabbix()
    statement = text(
        f"call sp_viewProblem('{municipalityId}','{tech}','{hostType}')")
    problems = db_zabbix.Session().execute(statement)

    db_zabbix.Session().close()
    db_zabbix.stop()

    # print(problems)
    data = pd.DataFrame(problems)
    data = data.replace(np.nan, "")
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
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    if exception_agency_search.name == exception_agency.name:
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
        ProblemRecord.problemrecord_id == exception.problemrecord_id).first()
    if not problem_record:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Problem Record not exists",
        )
    if problem_record.estatus == "Excepcion":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Exception already exists",
        )
    new_exception = ExceptionModel(
        problemrecord_id=exception.problemrecord_id,
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
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Exception Agency Not Found"
        )
    if exception_agency_search.name == exception_agency.name:
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Problem Record not exists",
        )
    match estatus:
        case "En curso":
            problem_record.estatus = "En curso"
            problem_record.taken_at = datetime.now()
            problem_record.user_id = current_user_id
        case "Cerrado":
            if problem_record.estatus == "Creado":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Problem Record must be first taken or pass to status 'En curso' to change the status to 'Cerrado'",
                )
            problem_record.closed_at = datetime.now()
            problem_record.estatus = "Cerrado"
        case "Soporte 2do Nivel":
            if problem_record.estatus != "En curso":
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Problem Record must be first taken or pass to status 'En curso' to change the status to 'Soporte 2do Nivel'",
                )
            problem_record.estatus = "Soporte 2do Nivel"
    session.commit()
    session.refresh(problem_record)
    return success_response(message="Estatus actualizado correctamente",
                            data=problem_record)
