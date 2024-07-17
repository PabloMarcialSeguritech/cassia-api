import unittest
import asyncio
from utils.settings import Settings
from fastapi.testclient import TestClient
from api import app
from services import auth_service2
from services.cassia import maintenance_service
from datetime import datetime, timezone
import json
import schemas.cassia_maintenance_schema as maintenance_schema

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


class MaintenanceServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    @unittest.skip("Omit create maintenance")
    def test_create_maintenance(self):
        print("> Entrando test_create_exception <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)

            maintenance_data = {
                'description': "Mantenimiento descripción test",
                'exception_agency_id': 1,
                'hostid': 11214,
                'date_start': datetime.strptime('2024-07-26 12:08:47',
                                                '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc),
                'date_end': datetime.strptime('2024-07-27 12:12:48', '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            }
            maintenance = maintenance_schema.CassiaMaintenance(**maintenance_data)
            response = await maintenance_service.create_maintenance_async(maintenance, current_session.session_id.hex)
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIn("correctamente", response_dict['message'])

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit create maintenance minor date start")
    def test_create_maintenance_minor_date_start(self):
        print("> Entrando test_create_maintenance_minor_date_start <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)

            maintenance_data = {
                'description': "Mantenimiento descripción test",
                'exception_agency_id': 1,
                'hostid': 11214,
                'date_start': datetime.strptime('2024-07-04 12:08:47',
                                                '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc),
                'date_end': datetime.strptime('2024-07-02 12:12:48',
                                              '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            }
            maintenance = maintenance_schema.CassiaMaintenance(**maintenance_data)
            response = await  maintenance_service.create_maintenance_async(maintenance,
                                                                           current_session.session_id.hex)
            print("response:", response)
            # response_dict = json.loads(response.body)
            # print("response_dict:", response_dict)
            # self.assertIn("correctamente", response_dict['message'])

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit create maintenance minor date end")
    def test_create_maintenance_minor_date_end(self):
        print("> Entrando test_create_maintenance_minor_date_end <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)

            maintenance_data = {
                'description': "Mantenimiento descripción test",
                'hostid': 11214,
                'date_start': datetime.strptime('2024-07-04 12:08:47',
                                                '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc),
                'date_end': datetime.strptime('2024-07-03 12:12:48',
                                              '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            }
            maintenance = maintenance_schema.CassiaMaintenance(**maintenance_data)
            response = await maintenance_service.create_maintenance_async(maintenance,
                                                                          current_session.session_id.hex)
            print("response:", response)
            # response_dict = json.loads(response.body)
            # print("response_dict:", response_dict)
            # self.assertIn("correctamente", response_dict['message'])

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit create maintenance minor date end")
    def test_create_maintenance_minor_date_end_vs_date_start(self):
        print("> Entrando test_create_maintenance_minor_date_end_vs_date_start <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)

            maintenance_data = {
                'description': "Mantenimiento descripción test",
                'hostid': 11214,
                'date_start': datetime.strptime('2024-07-20 12:08:47',
                                                '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc),
                'date_end': datetime.strptime('2024-07-19 12:12:48',
                                              '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            }
            maintenance = maintenance_schema.CassiaMaintenance(**maintenance_data)
            response = await maintenance_service.create_maintenance_async(maintenance,
                                                                          current_session.session_id.hex)
            print("response:", response)
            # response_dict = json.loads(response.body)
            # print("response_dict:", response_dict)
            # self.assertIn("correctamente", response_dict['message'])

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit delete maintenance")
    def test_delete_maintenance(self):
        print("> Entrando test_delete_maintenance <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            maintenance_id = 2
            response = await maintenance_service.delete_maintenance_async(maintenance_id,
                                                                          current_session.session_id.hex)
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIn("Eliminado", response_dict['message'])

        self.loop.run_until_complete(async_test())


    def test_get_maintenances(self):
        print("> Entrando test_get_maintenances <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)

            response = await maintenance_service.get_maintenances()
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            # self.assertIn("Eliminado", response_dict['message'])

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit update maintenances")
    def test_update_maintenance(self):
        print("> Entrando test_update_maintenance <")
        token = user_authentication_headers(
            'juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)

            # Datos de la excepción a actualizar
            maintenance_data = {
                'description': "Mantenimiento descripción actualización test",
                'maintenance_id': 2,
                'hostid': 11214,
                'date_start': datetime.strptime('2024-07-21 12:08:47',
                                                '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc),
                'date_end': datetime.strptime('2024-07-22 12:12:48',
                                              '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            }
            maintenance = maintenance_schema.CassiaMaintenance(**maintenance_data)
            response = await maintenance_service.update_maintenance_async(maintenance, current_session.session_id.hex)
            response_dict = json.loads(response.body)
            print("response_dict:", response_dict)
            self.assertIsNotNone(
                response_dict['data'], "Se espera que no sea None")

        self.loop.run_until_complete(async_test())
