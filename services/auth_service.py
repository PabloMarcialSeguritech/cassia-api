from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from jose import JWTError, jwt
from passlib.context import CryptContext

from models.user_model import User as UserModel
from schemas.token_schema import TokenData
from utils.settings import Settings

from utils.db import DB_Auth, DB_Zabbix
from sqlalchemy import or_, and_, text
from utils.traits import success_response

from models.user_has_roles import UserHasRole
from models.cassia_roles import CassiaRole
from models.cassia_permissions import CassiaPermission
from models.role_has_permissions import RoleHasPermission
import pandas as pd
import numpy as np
settings = Settings()

SECRET_KEY = settings.secret_key
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = settings.token_expire
REFRESH_TOKEN_EXPIRE_MINUTES = settings.refresh_token_expire

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/login/swagger",)


def verify_password(plain_password, password):
    return pwd_context.verify(plain_password, password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_user(username: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    user = session.query(UserModel).filter(
        or_(UserModel.mail == username)).first()
    session.close()
    return user


def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, user.password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    token = {
        "token": encoded_jwt,
        "expires": expire
    }
    return token


def create_refresh_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=REFRESH_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    token = {
        "token": encoded_jwt,
        "expires": expire
    }
    return token


def generate_token(username, password):
    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email/username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    """ if not user.verified_at:
        db_zabbix = DB_Zabbix()
        session = db_zabbix.Session()
        user_temp = session.query(UserModel).filter(
            UserModel.user_id == user.user_id).first()
        user_temp.verified_at = datetime.now()
        session.commit()
        session.refresh(user_temp)
        user = user_temp """
    roles = get_roles(user.user_id)
    """ print(roles) """
    access_token = create_access_token(
        data={"sub": user.mail}
    )
    refresh_token = create_refresh_token(
        data={"sub": user.mail}
    )
    print(datetime.utcnow())
    print()
    return {
        'access_token': access_token["token"],
        'access_token_expires': access_token["expires"],
        'refresh_token': refresh_token["token"],
        'refresh_token_expires': refresh_token["expires"],
        "roles": roles["roles"],
        "permissions": roles["permissions"],
        "verified_at": user.verified_at
    }


def get_roles(user_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select role_id from user_has_roles where user_id={user_id}")
    roles = session.execute(statement)
    roles = pd.DataFrame(roles).replace(np.nan, "")
    if not roles.empty:
        if len(roles["role_id"]) > 1:
            rol_ids = tuple(roles['role_id'].values.tolist())
        else:
            rol_ids = f"({roles['role_id'][0]})"
        statement = text(
            f"select rol_id,name from cassia_roles where rol_id in{rol_ids} and deleted_at IS NULL")
        roles = session.execute(statement)
        roles = pd.DataFrame(roles).replace(np.nan, "")

        if not roles.empty:

            statement = text(
                f"select permission_id,cassia_rol_id from role_has_permissions where cassia_rol_id in{rol_ids}")
            permissions = session.execute(statement)
            permissions = pd.DataFrame(permissions).replace(np.nan, "")
            """ print(permissions.head()) """
            if not permissions.empty:
                if len(permissions["permission_id"]) > 1:
                    permission_ids = tuple(
                        permissions['permission_id'].values.tolist())
                else:
                    permission_ids = f"({permissions['permission_id'][0]})"
                """ print(permission_ids) """
                statement = text(
                    f"select permission_id,module_name, name from cassia_permissions where permission_id in{permission_ids}")
                permissions = session.execute(statement)
                permissions = pd.DataFrame(permissions).replace(np.nan, "")
                session.close()
                return {"roles": roles.to_dict(orient="records"), "permissions": permissions.to_dict(orient="records")}
            else:
                session.close()
                return {"roles": roles.to_dict(orient="records"), "permissions": []}
        else:
            session.close()
            return {"roles": roles.to_dict(orient="records"), "permissions": []}
    else:
        session.close()
        return {"roles": [], "permissions": []}

# Get the current user decodign the JWT
# Act like a middleware to verify if the request has a token and is valid


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        expires: str = payload.get("exp")
        print(datetime.fromtimestamp(expires), datetime.now())
        if datetime.fromtimestamp(expires) < datetime.now():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token Expired",
                headers={"WWW-Authenticate": "Bearer"}
            )
        print(datetime.fromtimestamp(expires), datetime.now())
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)

    except JWTError:
        raise credentials_exception

    user = get_user(username=token_data.username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Could not find user",
        )
    return user
