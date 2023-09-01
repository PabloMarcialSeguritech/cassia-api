from utils.settings import Settings
from utils.db import DB_Zabbix
from models.cassia_roles import CassiaRole as CassiaRolModel
from utils.traits import success_response
settings = Settings()

'''
    Servicio para la administracion de catalogos
'''


def get_roles():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    roles = session.query(CassiaRolModel).all()
    session.close()
    return success_response(data=roles)
