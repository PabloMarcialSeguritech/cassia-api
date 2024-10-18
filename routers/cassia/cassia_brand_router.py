from fastapi import APIRouter, Depends, status, Body, UploadFile, File
from services.cassia import cassia_brand_service
from infraestructure.database import DB
from dependencies import get_db
from services import auth_service2
from schemas import cassia_brand_schema



cassia_brand_router = APIRouter(prefix="/brands")


@cassia_brand_router.get(
    "/",
    tags=["Cassia - Brands"],
    status_code=status.HTTP_200_OK,
    summary="Obtiene las Marcas",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def get_brands(db: DB = Depends(get_db)):
    return await cassia_brand_service.fetch_all_brands(db)

# Crear un nuevo brand (editable siempre será 0)
@cassia_brand_router.post(
    "/",
    tags=["Cassia - Brands"],
    status_code=status.HTTP_200_OK,
    summary="Crea la Marca",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_brand(brand_data: cassia_brand_schema.CassiaBrandSchema = Body(..., exclude={"brand_id"}), db: DB = Depends(get_db)):
    return await cassia_brand_service.create_new_brand(brand_data, db)

# Actualizar un brand existente (solo si editable = 1)
@cassia_brand_router.put(
    "/{brand_id}",
    tags=["Cassia - Brands"],
    status_code=status.HTTP_200_OK,
    summary="Actualiza la Marca",
    dependencies=[Depends(auth_service2.get_current_user_session)])
async def update_brand(brand_id: int, brand_data: cassia_brand_schema.CassiaBrandSchema, db: DB = Depends(get_db)):
    return await cassia_brand_service.modify_brand(db, brand_id, brand_data)

# Eliminar un brand (solo si editable = 1)
@cassia_brand_router.delete(
    "/{brand_id}",
    tags=["Cassia - Brands"],
    status_code=status.HTTP_200_OK,
    summary="Borra la Marca",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_brand(brand_id: int, db: DB = Depends(get_db)):
    return await cassia_brand_service.remove_brand(brand_id, db)

@cassia_brand_router.post(
    "/export",
    tags=["Cassia - Brands", "CASSIA Exports"],
    status_code=status.HTTP_200_OK,
    summary="Exporta los tipos de marcas de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_groups_data(export_data: cassia_brand_schema.CassiaBrandExportSchema, db: DB = Depends(get_db)):
    return await cassia_brand_service.export_brands_data(export_data, db)


@cassia_brand_router.post(
    "/import",
    tags=["Cassia - Brands", "CASSIA Imports"],
    status_code=status.HTTP_200_OK,
    summary="Importa las marcas de host de Cassia con un archivo proporcionado",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def import_groups_data(file_import: UploadFile = File(...), db: DB = Depends(get_db)):
    return await cassia_brand_service.import_brands_data(file_import, db)