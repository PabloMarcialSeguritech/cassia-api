from fastapi import APIRouter, UploadFile
from fastapi import Depends, status, Form, Body, File

from models.cassia_user_session import CassiaUserSession
from services import auth_service2
from schemas import cassia_host_groups_schema
from schemas import cassia_host_models_schema
from typing import List, Optional
from services.cassia import cassia_host_groups_service
from services.cassia import cassia_host_models_service
from dependencies import get_db
from infraestructure.database import DB

cassia_host_models_router = APIRouter(prefix="/host_models")


@cassia_host_models_router.get(
    "/by_brand/{brand_id}",
    tags=["Host Models"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los modelos de hosts disponibles en CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_models_by_brand(brand_id: int, db: DB = Depends(get_db)):
    return await cassia_host_models_service.get_host_models_by_brand(brand_id, db)


@cassia_host_models_router.get(
    "/",
    tags=["Host Models"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene los modelos de hosts disponibles en CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_host_models(db: DB = Depends(get_db)):
    return await cassia_host_models_service.get_host_models(db)


@cassia_host_models_router.post(
    "/",
    tags=["Host Models"],
    status_code=status.HTTP_200_OK,
    summary="Crea un modelo de host de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_host_model(model_data: cassia_host_models_schema.CassiaHostModelSchema,
                            current_user: CassiaUserSession = Depends(auth_service2.get_current_user_session), db: DB = Depends(get_db)):
    return await cassia_host_models_service.create_host_model(db, model_data, current_user)


@cassia_host_models_router.delete(
    "/{model_id}",
    tags=["Host Models"],
    status_code=status.HTTP_200_OK,
    summary="Eliminar un modelo de host de Cassia",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_host_model(model_id: int, current_user: CassiaUserSession = Depends(auth_service2.get_current_user_session), db: DB = Depends(get_db)):
    return await cassia_host_models_service.delete_host_model(model_id, current_user, db)


@cassia_host_models_router.post(
    "/export",
    tags=["Host Models", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Exporta los modelos de host de Cassia",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_models_data(export_data: cassia_host_models_schema.CassiaHostModelsExportSchema, db: DB = Depends(get_db)):
    return await cassia_host_models_service.export_models_data(export_data, db)


@cassia_host_models_router.post(
    "/import",
    tags=["Host Models", "CASSIA Imports"],
    status_code=status.HTTP_200_OK,
    summary="Importa los modelos de host de Cassia con un archivo proporcionado",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def import_models_data(file_import: UploadFile = File(...), db: DB = Depends(get_db)):
    return await cassia_host_models_service.import_models_data(file_import, db)


@cassia_host_models_router.put(
    '/{model_id}',
    tags=["Host Models"],
    status_code=status.HTTP_200_OK,
    summary="Actualiza el modelo de host de CASSIA (nombre y marca)",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def update_host_group(model_id: int, model_data: cassia_host_models_schema.CassiaHostModelSchema,
                            current_user: CassiaUserSession = Depends(
                                auth_service2.get_current_user_session),
                            db: DB = Depends(get_db)):
    return await cassia_host_models_service.update_host_model(model_id, model_data, current_user, db)
