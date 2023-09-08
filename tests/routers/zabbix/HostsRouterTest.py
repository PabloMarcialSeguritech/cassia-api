from fastapi.testclient import TestClient
from api import app
import unittest

client = TestClient(app)


def user_authentication_headers(username: str, password: str):
    data = {"username": username, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "accept": "application/json"}
    r = client.post("/api/v1/auth/login", data=data, headers=headers)
    response = r.json()
    auth_token = response['data']['access_token']
    headers = {"Authorization": f"Bearer {auth_token}"}
    return headers


class HostsRouterTest(unittest.TestCase):

    def test_reboot_server(self):
        print('> Entrando a test_reboot_server <')
        headers = user_authentication_headers("juan.marcial@seguritech.com", "12345678")
        host_id = 10596
        valor_esperado = "true"
        response_reboot = client.post(
            f"/api/v1/zabbix/hosts/reboot/{host_id}",
            headers=headers
        )
        json_obj_reboot = response_reboot.json()
        self.assertIn(valor_esperado, json_obj_reboot['data']['reboot'], "Host no se pudo reiniciar en linea")
