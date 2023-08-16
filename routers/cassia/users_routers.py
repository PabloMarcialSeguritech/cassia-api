from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service
import services.cassia.users_service as users_service
from fastapi import Body
from models.user_model import User
from fastapi import File, UploadFile, Form
from typing import Optional
from schemas import user_schema
users_router = APIRouter()


@users_router.get(
    '/',
    tags=["Cassia - Users"],
    status_code=status.HTTP_200_OK,
    summary="Get all cassia users",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_users():
    return users_service.get_users()


@users_router.get(
    '/roles',
    tags=["Cassia - Users"],
    status_code=status.HTTP_200_OK,
    summary="Get all cassia roles",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_users():
    return users_service.get_roles()


@users_router.get(
    '/{user_id}',
    tags=["Cassia - Users"],
    status_code=status.HTTP_200_OK,
    summary="Get user by id",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_users(user_id: int):
    return users_service.get_user(user_id)


@users_router.post('/',
                   tags=["Cassia - Users"],
                   status_code=status.HTTP_201_CREATED,
                   summary="Create a new user",
                   dependencies=[Depends(auth_service.get_current_user)])
async def create_user(user: user_schema.UserRegister = Body(...)):
    """
    ## Create a new user in the app

    ### Args
    The app can recive next fields into a JSON
    - email: A valid email
    - username: Unique username
    - password: Strong password for authentication

    ### Returns
    - user: User info
    """
    return await users_service.create_user(user)


@users_router.put('/{user_id}',
                  tags=["Cassia - Users"],
                  status_code=status.HTTP_201_CREATED,
                  summary="Update an user",
                  dependencies=[Depends(auth_service.get_current_user)])
async def create_user(user_id: int, user: user_schema.UserRegister = Body(...)):
    """
    ## Create a new user in the app

    ### Args
    The app can recive next fields into a JSON
    - email: A valid email
    - username: Unique username
    - password: Strong password for authentication

    ### Returns
    - user: User info
    """
    return await users_service.update_user(user_id, user)
