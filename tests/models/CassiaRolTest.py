import unittest
from utils.db import DB_Zabbix
from models.cassia_roles import CassiaRole as CassiaRolModel

'''
Clase de prueba para la entidad de Interface donde se obtiene la ip

'''


def obtener_roles():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    get_roles = session.query(CassiaRolModel).all()
    print("get_roles:", get_roles)
    return get_roles


class CassiaRolTest(unittest.TestCase):

    def test_obtener_roles(self):
        print("> Entrando a test_obtener_roles <")
        list = obtener_roles()
        for item in list:
            print("----------")
            for permission in item.permissions:
                print("permission:", permission.name)
