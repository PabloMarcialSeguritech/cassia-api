from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from utils.traits import success_response
import numpy as np
settings = Settings()


def get_aps_layer(municipality_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"call sp_hostView('{municipality_id}','2','')")
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    session.close()
    response = {"aps": data.to_dict(
        orient="records")
    }
    return success_response(data=response)


def get_downs_layer(municipality_id, dispId, subtype_id):
    db_zabbix = DB_Zabbix()
    statement = text(
        f"call sp_hostDown('{municipality_id}','{dispId}','{subtype_id}')")
    session = db_zabbix.Session()
    aps = session.execute(statement)
    data = pd.DataFrame(aps).replace(np.nan, "")
    response = {"downs": data.to_dict(
        orient="records")
    }
    session.close()
    return success_response(data=response)
