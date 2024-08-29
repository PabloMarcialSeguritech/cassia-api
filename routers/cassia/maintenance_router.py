from fastapi import APIRouter
import services.cassia.maintenance_service as maintenance_service
import schemas.cassia_maintenance_schema as cassia_maintenance_schema
from fastapi import Depends, status
from services import auth_service2
from fastapi import Body
from models.cassia_user_session import CassiaUserSession

maintenance_router = APIRouter(prefix="/maintenance")


@maintenance_router.get(
    '/maintenances',
    tags=["Cassia - Maintenance"],
    status_code=status.HTTP_200_OK,
    summary="Get all maintenances",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def get_agencies_async():
    return await maintenance_service.get_maintenances()


@maintenance_router.post(
    '/create_new',
    tags=["Cassia - Maintenance"],
    status_code=status.HTTP_200_OK,
    summary="Create a Maintenance",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_maintenance_async_new(maintenance: cassia_maintenance_schema.CassiaMaintenanceNew = Body(...),
                                       current_user_session: CassiaUserSession = Depends(
                                       auth_service2.get_current_user_session)):
    result = await maintenance_service.create_maintenance_async_new(maintenance=maintenance,
                                                                    current_user_session=current_user_session.session_id.hex)
    return result


@maintenance_router.post(
    '/create',
    tags=["Cassia - Maintenance"],
    status_code=status.HTTP_200_OK,
    summary="Create a Maintenance",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def create_maintenance_async(maintenance: cassia_maintenance_schema.CassiaMaintenance = Body(...),
                                   current_user_session: CassiaUserSession = Depends(
                                       auth_service2.get_current_user_session)):
    result = await maintenance_service.create_maintenance_async(maintenance=maintenance,
                                                                current_user_session=current_user_session.session_id.hex)
    return result


@maintenance_router.put(
    '/update',
    tags=["Cassia - Maintenance"],
    status_code=status.HTTP_200_OK,
    summary="Update a Maintenance",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def update_maintenance_async(maintenance: cassia_maintenance_schema.CassiaMaintenance = Body(...),
                                   current_user_session: CassiaUserSession = Depends(
                                       auth_service2.get_current_user_session)):
    response = await maintenance_service.update_maintenance_async(maintenance=maintenance,
                                                                  current_user_session=current_user_session.session_id.hex)
    return response


@maintenance_router.delete(
    '/delete/{maintenance_id}',
    tags=["Cassia - Maintenance"],
    status_code=status.HTTP_200_OK,
    summary="Delete a Maintenance",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def delete_maintenance_async(maintenance_id,
                                   current_user_session: CassiaUserSession =
                                   Depends(auth_service2.get_current_user_session)):
    response = await maintenance_service.delete_maintenance_async(maintenance_id=maintenance_id,
                                                                  current_user_session=current_user_session.session_id.hex)
    return response
