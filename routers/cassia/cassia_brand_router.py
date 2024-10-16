from fastapi import APIRouter, Depends, status
from fastapi.responses import StreamingResponse
from services.cassia import cassia_brand_service
from infraestructure.database import DB
from dependencies import get_db  # Importa get_db desde dependencies.py
from services import auth_service2
from schemas import cassia_brand_schema



cassia_brand_router = APIRouter(prefix="/brand")

# Obtener todos los brands
@cassia_brand_router.get("/get_brands", tags=["CASSIA Brand: Crud"])
async def get_brands(db: DB = Depends(get_db)):
    return await cassia_brand_service.fetch_all_brands(db)

@cassia_brand_router.post(
    "/export",
    tags=["CASSIA Brand: Export"],
    status_code=status.HTTP_200_OK,
    summary="Exporta los tipos de marcas de CASSIA",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def export_groups_data(export_data: cassia_brand_schema.CassiaBrandExportSchema, db: DB = Depends(get_db)):
    return await cassia_brand_service.export_brands_data(export_data, db)