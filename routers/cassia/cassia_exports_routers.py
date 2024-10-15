from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File
from services import auth_service2
from schemas import cassia_ci_history_schema
from typing import List, Optional
from services.cassia import cassia_group_types_service
from dependencies import get_db
from infraestructure.database import DB
import pandas as pd
from utils.traits import success_response
cassia_exports_router = APIRouter(prefix="/exports")


@cassia_exports_router.get(
    "/file_types",
    tags=["Group Types", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los tipos de archivo que acepta como parametro las funciones de export",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_file_types_export(db: DB = Depends(get_db)):
    data = {
        'type': ['csv', 'excel', 'json']
    }
    types = pd.DataFrame(data)
    return success_response(data=types.to_dict(orient='records'))
