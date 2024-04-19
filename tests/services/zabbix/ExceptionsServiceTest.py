import unittest
import asyncio
from utils.settings import Settings
from fastapi.testclient import TestClient
from api import app
from services import auth_service2
from services.zabbix import alerts_service
from schemas.exceptions_schema import CassiaExceptionsBase
from routers.zabbix import alerts_router
import json

settings = Settings()
client = TestClient(app)


def user_authentication_headers(username: str, password: str):
    data = {"username": username, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "accept": "application/json"}
    r = client.post("/api/v1/auth/login", data=data, headers=headers)
    response = r.json()
    """ print(response) """
    auth_token = response['data']['access_token']
    headers = {"Authorization": f"Bearer {auth_token}"}
    return auth_token


class ExceptionsServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    @unittest.skip("Omit query acks")
    def test_get_agencies(self):
        print("> Entrando test_get_exceptions <")
        """ token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678') """

        async def async_test():
            """ current_session = await auth_service2.get_current_user_session(token) """

            """ eventid = 8421
            message = 'mensaje de prueba' """
            response = await alerts_service.get_exception_agencies()
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIn("success", response_dict['message'])

        self.loop.run_until_complete(async_test())

    def test_create_exception(self):
        print("> Entrando test_create_exception <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)

            exception = {'hostid': "19286",
                         'exception_agency_id': 2,
                         'created_at': '2024-04-01 10:01:47'}
            respone = await alerts_router
            response = await alerts_service.create_exception(exception, current_session)
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIn("correctamente", response_dict['message'])

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit query acks")
    def test_get_acks_cassia(self):
        print("> test_get_acks_cassia <")

        async def async_test():
            eventid = 82964
            is_zabbix_event = 1
            response = await alerts_service.get_acks(eventid, is_zabbix_event)

            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIsNotNone(
                response_dict['data'], "Se espera que no sea None")

        self.loop.run_until_complete(async_test())
