import unittest
import asyncio
from services.zabbix import hosts_service_
from services import auth_service2
from utils.settings import Settings
from fastapi.testclient import TestClient
from api import app


settings = Settings()
client = TestClient(app)


def user_authentication_token(username: str, password: str):
    data = {"username": username, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "accept": "application/json"}
    r = client.post("/api/v1/auth/login", data=data, headers=headers)
    response = r.json()
    print('response:::', response)
    auth_token = response['data']['access_token']
    return auth_token

class HostServiceTest2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    @unittest.skip("Omit test_get_action_type")
    def test_get_action_type(self):
        print("> Entrando test_get_action_type <")

        async def async_test():
            id_action_ping = 20
            tipo_accion_esperada = 0
            action_type = await hosts_service_.get_action_type(id_action_ping)
            print("obj_resp:", action_type)
            self.assertEquals(tipo_accion_esperada, action_type, "Se espera que la acción ping sea cero")

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_run_action_ping")
    def test_run_action_ping(self):
        print("> Entrando test_run_action_ping <")
        token = user_authentication_token('juan.marcial@seguritech.com', '12345678')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            response = await hosts_service_.run_action_('172.18.7.3', 20, current_session)
            print("obj_resp:", response.body)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_get_credentials_for_proxy_async")
    def test_get_credentials_for_proxy_async(self):
        print("> Entrando test_get_credentials_for_proxy_async <")

        async def async_test():
            ip_proxy, user_proxy, password_proxy = await hosts_service_.get_credentials_for_proxy_async('172.18.40.81')
            print("ip_proxy:", ip_proxy)
            print("user_proxy:", user_proxy)
            print("password_proxy:", password_proxy)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_proxy_validation")
    def test_proxy_validation(self):
        print("> Entrando test_proxy_validation <")

        async def async_test():
            has_proxy, credentials = await hosts_service_.proxy_validation('172.18.40.81')
            print("has_proxy:", has_proxy)
            print("credentials:", credentials)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_run_action_ping2")
    def test_run_action_ping2(self):
        print("> Entrando test_run_action_ping2 <")

        async def async_test():
            success, message = await hosts_service_.async_ping_by_local('172.18.7.3')
            print("success:", success)
            print("message:", message)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_async_ping")
    def test_async_ping(self):
        print("> Entrando test_async_ping <")

        async def async_test():
            try:
                ip = '172.28.102.106'
                ip_proxy, user_proxy, password_proxy = await hosts_service_.get_credentials_for_proxy_async(ip)
                print("ip_proxy:", ip_proxy)
                print("user_proxy:", user_proxy)
                print("password_proxy:", password_proxy)
                success, message = await hosts_service_.async_ping(ip, ip_proxy, user_proxy, password_proxy)
                print("success:", success)
                print("message:", message)
                print("saliendo de prueba")
            except Exception as e:
                print("Excepcion en test unitario:", str(e))
            print("saliendo de test unitario")

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_run_action_sin_credenciales")
    def test_run_action_sin_credenciales(self):
        print("> Entrando test_run_action_sin_credenciales <")
        token = user_authentication_token('juan.marcial@seguritech.com', '12345678')
        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            ip='172.28.102.106'
            id_action=20
            response = await hosts_service_.run_action_(ip, id_action, current_session)
            print("response:", response.body)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_run_action_con_credenciales")
    def test_run_action_con_credenciales(self):
        print("> Entrando test_run_action_con_credenciales <")
        token = user_authentication_token('juan.marcial@seguritech.com', '12345678')
        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            ip='172.19.16.24'
            id_action=20
            response = await hosts_service_.run_action_(ip, id_action, current_session)
            print("response:", response.body)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_run_action_con_credenciales_no_responde_por_proxy_pero_si_directo")
    def test_run_action_con_credenciales_no_responde_por_proxy_pero_si_directo(self):
        print("> Entrando test_run_action_con_credenciales_no_responde_por_proxy_pero_si_directo <")
        token = user_authentication_token('juan.marcial@seguritech.com', '12345678')
        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            ip='172.18.42.4'
            id_action=20
            response = await hosts_service_.run_action_(ip, id_action, current_session)
            print("response:", response.body)

        self.loop.run_until_complete(async_test())


    def test_run_action_down_ip(self):
        print("> Entrando test_run_action_down_ip <")
        token = user_authentication_token('juan.marcial@seguritech.com', '12345678')
        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            ip='172.17.0.46'
            id_action=20
            response = await hosts_service_.run_action_(ip, id_action, current_session)
            print("response:", response.body)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_run_action_down_ip2")
    def test_run_action_down_ip2(self):
        print("> Entrando test_run_action_down_ip2 <")
        token = user_authentication_token('juan.marcial@seguritech.com', '12345678')
        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            ip='172.18.42.4'
            id_action=20
            response = await hosts_service_.run_action_(ip, id_action, current_session)
            print("response:", response.body)

        self.loop.run_until_complete(async_test())




