from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service
import services.cassia.roles_service as roles_service
from fastapi import Body
from models.user_model import User
from fastapi import File, UploadFile, Form
from typing import Optional
from schemas import user_schema, update_user_password, cassia_role_schema
roles_router = APIRouter()


@roles_router.get(
    '/roles/{role_id}',
    tags=["Cassia - Roles"],
    status_code=status.HTTP_200_OK,
    summary="Get a cassia role",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_role(role_id: int):
    return roles_service.get_role(role_id)


@roles_router.get(
    '/roles/permissions/get',
    tags=["Cassia - Roles"],
    status_code=status.HTTP_200_OK,
    summary="Get all cassia permissions",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_permissions():
    return roles_service.get_permissions()


@roles_router.post('/roles',
                   tags=["Cassia - Roles"],
                   status_code=status.HTTP_201_CREATED,
                   summary="Create a new role",
                   dependencies=[Depends(auth_service.get_current_user)])
async def create_role(role: cassia_role_schema.RoleRegister = Body(...)):
    """
    ## Create a new role in the app

    ### Args
    The app can recive next fields into a JSON
    - name: The name of the user
    - permissions: Id of permissions separated by comma. Example: 1,2,3

    ### Returns
    - Roles: Roles info
    """
    return await roles_service.create_role(role)


@roles_router.put('/roles/{role_id}',
                  tags=["Cassia - Roles"],
                  status_code=status.HTTP_201_CREATED,
                  summary="Update an user",
                  dependencies=[Depends(auth_service.get_current_user)])
async def update_role(role_id: int, role: cassia_role_schema.RoleRegister = Body(...)):
    """
    ## Create a new user in the app

    ### Args
    The app can recive next fields into a JSON
    - email: A valid email
    - name: The name of the user
    - roles: Id of roles separated by comma. Example: 1,2,3

    ### Returns
    - user: User info
    """
    return await roles_service.update_role(role_id, role)


@roles_router.delete(
    '/roles/{role_id}',
    tags=["Cassia - Roles"],
    status_code=status.HTTP_201_CREATED,
    summary="Delete a role",
    dependencies=[Depends(auth_service.get_current_user)])
async def delete_role(role_id: int):
    return await roles_service.delete_role(role_id)
