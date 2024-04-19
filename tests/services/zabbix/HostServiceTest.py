import unittest
import asyncio
from services.zabbix import hosts_service
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


class HostServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    @unittest.skip("Omit action of info actions")
    def test_get_info_actions(self):
        print("> Entrando test_get_info_actions <")
        response = hosts_service.get_info_actions('172.18.200.17')
        json_obj = response.body
        print("obj_resp:", json_obj)

    @unittest.skip("Omit action of wrong ip")
    def test_get_info_actions_wrong_ip(self):
        print("> Entrando test_get_info_actions_wrong_ip <")
        response = hosts_service.get_info_actions('192.168.100.1')
        json_obj = response.body
        print("obj_resp:", json_obj)

    @unittest.skip("Omit encrypt")
    def test_ecncrypt(self):
        print("> Entrando test_ecncrypt <")
        usr = hosts_service.encrypt("", settings.ssh_key_gen)
        print("usr:", usr)
        passwd = hosts_service.encrypt("", settings.ssh_key_gen)
        print("pwd:", passwd)

    @unittest.skip("Omit reboot")
    def test_run_action_reboot(self):
        print("> Entrando test_run_action_reboot <")
        response = hosts_service.prepare_action('172.19.16.24', 2)
        json_obj = response.body
        print("obj_resp:", json_obj)

    @unittest.skip("Omit action ping")
    def test_run_action_ping(self):
        print("> Entrando test_run_action_ping <")
        token = user_authentication_token('', '')

        async def async_test():
            current_session = await auth_service2.get_current_user_session(token)
            response = await hosts_service.prepare_action('172.18.40.81', 20, current_session)
            print("obj_resp:", response.body)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit credentials not found")
    def test_run_action_credentials_not_found(self):
        print("> Entrando test_run_action_reboot <")
        response = hosts_service.prepare_action('172.19.16.25', 2)
        json_obj = response.body
        print("obj_resp:", json_obj)

    @unittest.skip("Omit query arcos")
    def test_get_host_arcos(self):
        print("> Entrando test_get_host_arcos <")

        async def async_test():
            response = await hosts_service.get_host_arcos(20157)
            print("obj_resp:", response.body)

        self.loop.run_until_complete(async_test())

    def test_decrypt(self):
        print("> Entrando test_decrypt <")
        usr = hosts_service.decrypt("gAAAAABlng4ilw3Qquxn01ObVFvaeHblTBtjnSvd583sJxwIUqT0JBPwdtoz4zJa3Ba0sXBsoeTkJQc5qcbXIxGfp7YaLcnObA==",settings.ssh_key_gen)
        print("usr:", usr)
        passwd = hosts_service.decrypt("gAAAAABlng4iVemaayFosx5KwZOBBhyk2VQkI8nsJdSxoPVCURYgYLyBEVlFHBixeUZ0RmnfJEqt7K_M2IjmEW2kk_kReHH34A==", settings.ssh_key_gen)
        print("pwd:", passwd)

