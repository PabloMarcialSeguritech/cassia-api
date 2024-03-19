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


class AlertsRouterTest(unittest.TestCase):

    @unittest.skip("Omit test ack cassia")
    def test_acknowledge_cassia(self):
        print('> Entrando a test_acknowledge_cassia <')
        headers = user_authentication_headers("juan.marcial@seguritech.com", "12345678")
        eventid = 2
        texto_esperado = "correctamente"
        response_reboot = client.post(
            f"/api/v1/zabbix/problems/acknowledge_/{eventid}",
            headers=headers,
            data={
                "message": "Prueba CASSIA",
                "close": 0,
                "is_zabbix_event": 0
            }
        )
        json_obj = response_reboot.json()
        print("json_obj:", json_obj)
        self.assertIn(texto_esperado, json_obj['message'])

    @unittest.skip("Omit test ack zabbix")
    def test_acknowledge_zabbix(self):
        print('> Entrando a test_acknowledge_zabbix <')
        headers = user_authentication_headers("juan.marcial@seguritech.com", "12345678")
        eventid = 8998453
        texto_esperado = "correctamente"
        response_reboot = client.post(
            f"/api/v1/zabbix/problems/acknowledge_/{eventid}",
            headers=headers,
            data={
                "message": "Mensaje de prueba",
                "close": 0,
                "is_zabbix_event": 1
            }
        )
        json_obj = response_reboot.json()
        print("json_obj:", json_obj)
        self.assertIn(texto_esperado, json_obj['message'])
