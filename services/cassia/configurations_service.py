from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text

import numpy as np
from utils.traits import success_response
settings = Settings()


def get_configuration():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"SELECT * FROM cassia_config")
    configuration = session.execute(statement)
    configuration = pd.DataFrame(configuration).replace(np.nan, "")
    session.close()

    return success_response(data=configuration.to_dict(orient="records"))
