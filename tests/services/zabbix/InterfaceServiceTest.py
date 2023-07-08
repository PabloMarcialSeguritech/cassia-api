import unittest
from services.zabbix import interface_service


class InterfaceServiceTest(unittest.TestCase):

    def test_create_ping(self):
        print('> Entrando a test_create_ping <')
        valor_esperado = 10596
        response = interface_service.create_ping(valor_esperado)
        json_obj = response.body
        print("obj_resp:", json_obj.decode())
        self.assertIn(str(valor_esperado), json_obj.decode(), "Hostid no se encuentra en la respuesta")


    def test_create_ping_host_not_found(self):
        print('> Entrando a test_create_ping_host_not_found <')
        valor_esperado = 'Host no encontrado'
        response = interface_service.create_ping(valor_esperado)
        json_obj = response.body
        print("obj_resp:", json_obj.decode())
        self.assertIn(valor_esperado, json_obj.decode(), "Host no encontrado no se encuentra en la respuesta")


