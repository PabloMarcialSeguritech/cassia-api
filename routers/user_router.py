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
auth_router = APIRouter(prefix="/api/v1")


@auth_router.post('/auth/sign-up',
                  tags=["Auth"],
                  status_code=status.HTTP_201_CREATED,
                  response_model=user_schema.User,
                  summary="Create a new user")
def create_user(user: user_schema.UserRegister = Body(...)):
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
    return user_service.create_user(user)


@auth_router.post(
    "/auth/login",
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
    return current_user


@auth_router.get('/users/',
                 tags=["Auth"],
                 status_code=status.HTTP_200_OK,
                 response_model=List[user_schema.UserBase],
                 summary="Get all users",
                 dependencies=[Depends(auth_service.get_current_user)])
def get_users():
    """
    ## Get all users

    ### Returns
    - users: User info
    """
    return user_service.get_users()
