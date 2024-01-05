import unittest
from services.zabbix import hosts_service
from utils.settings import Settings

settings = Settings()


class HostServiceTest(unittest.TestCase):

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
        usr = hosts_service.encrypt("root", settings.ssh_key_gen)
        print("usr:", usr)
        passwd = hosts_service.encrypt("S3guR1t3ch#", settings.ssh_key_gen)
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
        response = hosts_service.prepare_action('172.18.42.11', -1)
        json_obj = response.body
        print("obj_resp:", json_obj)

    @unittest.skip("Omit credentials not found")
    def test_run_action_credentials_not_found(self):
        print("> Entrando test_run_action_reboot <")
        response = hosts_service.prepare_action('172.19.16.25', 2)
        json_obj = response.body
        print("obj_resp:", json_obj)
