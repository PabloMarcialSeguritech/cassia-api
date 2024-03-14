import unittest
import asyncio
from utils.settings import Settings
from fastapi.testclient import TestClient
from api import app
from services import auth_service2
from services.zabbix import alerts_service
import json

settings = Settings()
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


class AlertsServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    def test_register_ack_cassia(self):
        print("> Entrando test_register_ack_cassia <")
        token = user_authentication_headers('', '')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            eventid = 8421
            message = 'mensaje de prueba'
            response = await alerts_service.register_ack_cassia(eventid, message, current_session, 1)
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIn("correctamente", response_dict['message'])

        self.loop.run_until_complete(async_test())

    def test_register_ack_cassia_abierto(self):
        print("> Entrando test_register_ack_cassia <")
        token = user_authentication_headers('', '')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            cassia_arch_traffic_events_id = 674
            message = 'mensaje de prueba'
            closed = 1
            response = await alerts_service.register_ack_cassia(cassia_arch_traffic_events_id,
                                                                message, current_session, closed)
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIn("correctamente", response_dict['message'])

        self.loop.run_until_complete(async_test())
