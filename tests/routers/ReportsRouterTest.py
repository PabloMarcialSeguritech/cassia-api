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


class ReportsRouterTest(unittest.TestCase):

    @unittest.skip("test_report_multiple")
    def test_report_multiple(self):
        print('> Entrando a test_report_multiple <')
        headers = user_authentication_headers("juan.marcial@seguritech.com", "12345678")
        municipality_id = 0
        tech_id = 11
        brand_id = 0
        model_id = 0
        init_date = '2024-03-01T00:00'
        end_date = '2024-03-26T12:20'
        response = client.get(
            f"/api/v1/cassia/reports/availability/multiple?municipality_id={municipality_id}&tech_id={tech_id}&brand_id={brand_id}&model_id={model_id}&init_date={init_date}&end_date={end_date}",
            headers=headers
        )
        json_obj = response.json()
        print("json_obj:", json_obj)

    def test_report_multiple_(self):
        print('> Entrando a test_report_multiple_ <')
        headers = user_authentication_headers("juan.marcial@seguritech.com", "12345678")
        municipality_id = 0
        tech_id = 11
        brand_id = 0
        model_id = 0
        init_date = '2024-03-01T00:00'
        end_date = '2024-03-26T12:20'
        response = client.get(
            f"/api/v1/cassia/reports/availability/multiple_?municipality_id={municipality_id}&tech_id={tech_id}&brand_id={brand_id}&model_id={model_id}&init_date={init_date}&end_date={end_date}",
            headers=headers
        )
        json_obj = response.json()
        print("json_obj:", json_obj)
