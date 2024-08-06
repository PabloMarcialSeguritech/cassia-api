import unittest
import asyncio
from services.integration.reset_service_service_impl import ResetServiceImpl
import json

class ResetsServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    @unittest.skip("Omit test_resets_authenticate")
    def test_resets_authenticate(self):
        print("> Entrando test_resets_authenticate <")

        reset_service = ResetServiceImpl()

        async def async_test():
            response = await reset_service.authenticate()
            self.assertIsNotNone(response);

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_resets_get_devices")
    def test_resets_get_devices(self):
        print("> Entrando test_resets_get_devices <")

        reset_service = ResetServiceImpl()

        async def async_test():
            token = await reset_service.authenticate()
            response = await reset_service.get_devices(token)
            print("response_dict:", response)
            self.assertIsNotNone(response);

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_extract_device_info")
    def test_extract_device_info(self):
        print("> Entrando test_extract_device_info <")

        reset_service = ResetServiceImpl()

        async def async_test():
            token = await reset_service.authenticate()
            devices = await reset_service.get_devices(token)
            devices_filter = await reset_service.extract_device_info(devices)
            print(devices_filter)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_extract_device_info")
    def test_get_resets(self):
        print("> Entrando test_get_resets <")

        reset_service = ResetServiceImpl()

        async def async_test():
            resets = await reset_service.get_cassia_resets()

            print(resets)

        self.loop.run_until_complete(async_test())

    def test_compare_and_add_or_update_reset(self):
        print("> Entrando test_compare_and_add_or_update_reset <")

        reset_service = ResetServiceImpl()

        async def async_test():
            resets = await reset_service.merge_resets()

            print(resets)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_restart_reset")
    def test_restart_reset(self):
        print("> Entrando test_restart_reset <")

        reset_service = ResetServiceImpl()

        async def async_test():
            object_id = '64e6848929dd4b3df7a90f60'
            resets = await reset_service.restart_reset(object_id)
            response_dict = json.loads(resets.body)
            print(response_dict)

        self.loop.run_until_complete(async_test())
