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


class CatRouterTest(unittest.TestCase):

    def test_get_roles_permissions(self):
        print('> Entrando a test_get_roles_permissions <')
        headers = user_authentication_headers("juan.marcial", "12345678")
        response = client.get(
            f"/api/v1/cat/roles",
            headers=headers
        )
        valor_esperado = 'success'
        json_obj = response.json()
        print("json_obj:", json_obj)
        self.assertIn(valor_esperado, json_obj['message'], "Host no se encuentra en linea")
