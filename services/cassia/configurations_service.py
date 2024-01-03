from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
import socket
import numpy as np
from utils.traits import success_response
from models.cassia_state import CassiaState
from fastapi import HTTPException, status
settings = Settings()


async def get_configuration():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(f"SELECT * FROM cassia_config")
    configuration = session.execute(statement)
    configuration = pd.DataFrame(configuration).replace(np.nan, "")
    session.close()

    return success_response(data=configuration.to_dict(orient="records"))


async def get_estados():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"select state_id as id, state_name as name, url,url_front from cassia_state where deleted_at is null")
    estados = session.execute(statement)
    estados = pd.DataFrame(estados).replace(np.nan, "")
    session.close()
    return success_response(data=estados.to_dict(orient="records"))


async def ping_estado(id_estado):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    estado = session.query(CassiaState).filter(CassiaState.state_id == id_estado,
                                               CassiaState.deleted_at == None).first()
    if not estado:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="El estado con el id proporcionado no existe.")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    available = True
    try:
        sock.connect((estado.ip, int(estado.port)))
        print(f"El puerto esta abierto")
        result = f"El estado {estado.state_name} esta disponible."
    except socket.error:
        print("El puerto esta cerrado")
        result = f"El estado {estado.state_name} no esta disponible."
        available = False
    finally:
        sock.close()
    session.close()
    return success_response(message=result, data={'available': available})
