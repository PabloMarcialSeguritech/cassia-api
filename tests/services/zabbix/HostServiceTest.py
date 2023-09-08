import unittest
from services.zabbix import hosts_service
from utils.settings import Settings

settings = Settings()


class HostServiceTest(unittest.TestCase):

    def test_create_connection_ssh(self):
        print("> Entrando test_create_connection_ssh <")
        response = hosts_service.reboot(10596)
        valor_esperado = "false"
        json_obj = response.body
        print("obj_resp:", json_obj.decode())
        self.assertIn(valor_esperado, json_obj.decode(), "Host no encontrado no se encuentra en la respuesta")


