from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_Prueba
from sqlalchemy import text, or_
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from models.user_model import User
from models.user_has_roles import UserHasRole
from models.cassia_roles import CassiaRole
from models.cassia_permissions import CassiaPermission
from models.role_has_permissions import RoleHasPermission
import schemas.exception_agency_schema as exception_agency_schema
import schemas.exceptions_schema as exception_schema
import schemas.cassia_role_schema as cassia_role_schema

import numpy as np
from utils.traits import success_response
from models.cassia_config import CassiaConfig
settings = Settings()


def get_configuration():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"SELECT * FROM cassia_config")
    configuration = session.execute(statement)
    configuration = pd.DataFrame(configuration).replace(np.nan, "")
    session.close()

    return success_response(data=configuration.to_dict(orient="records"))
