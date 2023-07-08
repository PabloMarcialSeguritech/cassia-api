import unittest
from utils.db import DB_Zabbix
from models.interface_model import Interface as InterfaceModel

'''
Clase de prueba para la entidad de Interface donde se obtiene la ip

'''


def obtener_interface():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    get_interface = session.query(InterfaceModel).first()
    print("get_interface:", get_interface)
    return get_interface


def obtener_interface_filtrada(hostid=None):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    get_interface = session.query(InterfaceModel).filter(InterfaceModel.hostid == hostid).first()
    print("get_interface:", get_interface)
    return get_interface


class InterfaceTest(unittest.TestCase):
    def test_obtener_interface(self):
        print("> Entrando a test_obtener_interface <")
        self.assertIsNotNone(obtener_interface().interfaceid, "Se encontró la entidad")

    def test_obtener_interface_filtrada_por_id(self):
        print("> Entrando a test_obtener_interface_filtrada_por_id <")
        valor_esperado = 10596
        interface = obtener_interface_filtrada(valor_esperado)
        self.assertEqual(interface.hostid, valor_esperado)
