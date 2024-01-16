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
from models.cassia_user_session import CassiaUserSession
from models.cassia_user_authorizer import UserAuthorizer
import pandas as pd
import numpy as np
import uuid
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


def get_session(token: str):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    user_session = session.query(CassiaUserSession).filter(
        CassiaUserSession.access_token == token
    ).first()
    session.close()
    if user_session.end_date != None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="La sesión ha expirado.",
        )
    return user_session


def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, user.password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    token = {
        "token": encoded_jwt,
    }
    return token


def generate_token(username, password):
    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nombre de usuario/correo electronico o contraseña incorrecta.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    roles = get_roles(user.user_id)
    session_uuid = uuid.uuid4()
    access_token = create_access_token(
        data={"sub": user.mail,
              "uuid": str(session_uuid)}
    )
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    session_id = CassiaUserSession(
        session_id=session_uuid,
        user_id=user.user_id,
        access_token=access_token["token"],
        start_date=datetime.now(),
        end_date=None,
        active=True
    )
    session.add(session_id)
    session.commit()
    session.refresh(session_id)
    authorizer = session.query(UserAuthorizer).filter(
        UserAuthorizer.user_id == user.user_id).first()
    session.close()

    return {
        'access_token': access_token["token"],
        "roles": roles["roles"],
        "permissions": roles["permissions"],
        "verified_at": user.verified_at,
        'authorizer': 1 if authorizer else 0
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


async def get_current_user_session(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="No se pudieron validar las credenciales.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)

    except JWTError:
        raise credentials_exception

    session_user = get_session(token=token)

    if session_user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No se puede encontrar el usuario.",
        )
    return session_user


async def logout(current_user_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    current_session = session.query(CassiaUserSession).filter(
        CassiaUserSession.session_id == current_user_session.session_id
    ).first()
    current_session.end_date = datetime.now()
    current_session.active = False
    session.commit()
    session.refresh(current_session)
    session.close()
    return success_response(message="Sesión cerrada correctamente.")


async def logout_all(current_user_session):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    current_sessions = session.query(CassiaUserSession).filter(
        CassiaUserSession.user_id == current_user_session.user_id,
        CassiaUserSession.end_date == None
    ).all()
    for current_session in current_sessions:
        current_session.end_date = datetime.now()
        current_session.active = False
        session.commit()
        session.refresh(current_session)
    session.close()
    return success_response(message="Sesión cerrada correctamente en todos los dispositivos.")
