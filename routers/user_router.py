from utils.traits import success_response
from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Body
from typing import List, Annotated
from schemas import user_schema
from models.user_model import User as UserModel
from services import user_service
from schemas.token_schema import Token
from fastapi.security import OAuth2PasswordRequestForm
from services import auth_service
from schemas import update_user_password
import services.cassia.users_service as users_service
import services.cat_service as cat_service

auth_router = APIRouter(prefix="/api/v1")


""" @auth_router.post('/auth/sign-up',
                  tags=["Auth"],
                  status_code=status.HTTP_201_CREATED,
                  summary="Create a new user")
def create_user(user: user_schema.UserRegister = Body(...)): """
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
""" return user_service.create_user(user) """


@auth_router.post(
    "/auth/login",
    tags=["Auth"],
)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    ## Login for access token

    ### Args
    The app can receive next fields by form data
    - username: Your username or email
    - password: Your password

    ### Returns
    - access token and token type
    """
    access_token = auth_service.generate_token(
        form_data.username, form_data.password)
    print(access_token["roles"])
    response = {
        "access_token": access_token['access_token'],
        "refresh_token": access_token["refresh_token"],
        "token_type": "bearer",
        "roles": access_token["roles"],
        "permissions": access_token["permissions"]
    }
    return success_response(data=response)


@auth_router.post(
    "/auth/login/swagger",
    tags=["Auth"],
    response_model=Token
)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    ## Login for access token

    ### Args
    The app can receive next fields by form data
    - username: Your username or email
    - password: Your password

    ### Returns
    - access token and token type
    """
    access_token = auth_service.generate_token(
        form_data.username, form_data.password)

    return Token(access_token=access_token['access_token'], refresh_token=access_token["refresh_token"], token_type="bearer")


@auth_router.get(
    '/auth/profile',
    tags=["Auth"]
)
async def get_my_profile(current_user: Annotated[UserModel, Depends(auth_service.get_current_user)]):
    return success_response(data=current_user)


""" @auth_router.get('/users/',
                 tags=["Auth"],
                 status_code=status.HTTP_200_OK,
                 summary="Get all users",
                 dependencies=[Depends(auth_service.get_current_user)])
def get_users():
    """
# Get all users

# Returns
""" - users: User info """
"""
return user_service.get_users() """


@auth_router.put(
    '/auth/profile/update-password',
    tags=["Auth"],
    status_code=status.HTTP_201_CREATED,
    summary="Update User Password",
    dependencies=[Depends(auth_service.get_current_user)])
async def delete_user(data: update_user_password.UpdateUserPassword = Body(...), current_user: UserModel = Depends(auth_service.get_current_user)):
    return await users_service.update_password(data, current_user.user_id)

@auth_router.get(
    "/cat/roles",
    tags=["Catalogs"],
    status_code=status.HTTP_200_OK,
    summary="Get all roles and permissions",
    dependencies=[Depends(auth_service.get_current_user)]
)
def get_roles():
    return cat_service.get_roles()
