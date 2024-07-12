from fastapi.testclient import TestClient
from api import app
import unittest
from datetime import datetime

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


class ExceptionsRouterTest(unittest.TestCase):

    def test_exception_update_cassia(self):
        print('> Entrando a test_exception_update_cassia <')
        headers = user_authentication_headers("juan.marcial@seguritech.com", "12345678")
        eventid = 2
        texto_esperado = "correctamente"
        response_reboot = client.post(
            f"/api/v1/cassia/exceptions/update",
            headers=headers,
            data={
                'exception_id': '1',
                'description': "Actualización descripción test",
                'exception_agency_id': 1,
                'hostid': 11214,
                'created_at': '2024-07-01 12:08:47',
                'closed_at': '2024-07-02 12:12:48'
            }
        )
        json_obj = response_reboot.json()
        print("json_obj:", json_obj)

