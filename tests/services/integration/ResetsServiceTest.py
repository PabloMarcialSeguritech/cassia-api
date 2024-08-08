import unittest
import asyncio
from services.integration.reset_service_service_impl import ResetServiceImpl
import json
import pandas as pd

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

    @unittest.skip("Omit test_extract_device_info")
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
            hostid = '15098'
            resets = await reset_service.restart_reset(object_id, hostid)
            #response_dict = json.loads(resets.body)
            #print(response_dict)
            print(resets)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_restart_reset")
    def test_get_devices_related(self):
        print("> Entrando test_get_devices_related <")

        reset_service = ResetServiceImpl()

        async def async_test():

            devices = await reset_service.getDispositivosRelacionadosCapa1('15098')
            print(devices)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_get_devices_pmi")
    def test_get_devices_pmi(self):
        print("> Entrando test_get_devices_pmi <")

        reset_service = ResetServiceImpl()

        async def async_test():

            devices = await reset_service.get_devices_pmi('15098')
            print(devices)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_get_devices_pmi_status_connection")
    def test_get_devices_pmi_status_connection(self):
        print("> Entrando test_get_devices_pmi_status_connection <")

        reset_service = ResetServiceImpl()

        async def async_test():
            data = {
                'hostid': [15093, 15094, 15095, 15096, 15097, 15098, 21562],
                'host': [
                    'GUAN-CU23-CCTV-UNV-IPC2322EBR5-DUPZ-C-FIJA-FIJA',
                    'GUAN-CU23-CCTV-UNV-IPC2322EBR5-DUPZ-C-FIJA-FIJA',
                    'GUAN-CU23-CCTV-UNV-IPC2322EBR5-DUPZ-C-FIJA-FIJA',
                    'GUAN-CU23-CCTV-UNV-IPC7622ER-X44U-PTZ-PTZ-GTO',
                    'GUAN-CU23-CCTV-MOXA-EDS-408A-SWITCH-SWITCH-GTO',
                    'GUAN-CU23-CCTV-CAMBIUM_NETWORKS-FORCE300-16-SUSCRIPTOR',
                    'GUAN-RP_SAN MIGUEL-STD-CAMBIUM_NETWORK-EPMP_30'

                ],
                'alias': ['GTO-VVU-GUAN-023', 'GTO-VVU-GUAN-023', 'GTO-VVU-GUAN-023', 'GTO-VVU-GUAN-023',
                          'GTO-VVU-GUAN-023', 'GTO-VVU-GUAN-023', 'GTO-STD-GUAN-TOSE002'],
                'name': ['FIJA_1', 'FIJA_2', 'FIJA_3', 'PTZ', None, 'SUSCRIPTOR', 'AP4'],
                'device_id': [4, 4, 4, 8, 13, 11, 2],
                'ip': ['172.21.214.39', '172.21.214.40', '172.21.214.41', '172.21.214.38', '172.17.214.21',
                       '172.17.214.20', '172.17.114.5'],
                'b_interes': [0, 0, 0, 0, 1, 1, 1],
                'b_ubicacion': [1, 1, 1, 1, 1, 1, 1],
                'capa': [1, 1, 1, 1, 1, 1, 1]
            }
            devices_pmi_df = pd.DataFrame(data)
            devices_df = await reset_service.get_devices_pmi_status_connection(devices_pmi_df)
            print(devices_df)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_reset_pmi_5_min_arriba")
    def test_reset_pmi_5_min_arriba(self):
        print("> Entrando test_reset_pmi <")
        '''
            Prueba de 5 min y no response
            PMI Arriba
        '''

        reset_service = ResetServiceImpl()

        async def async_test():
            affiliation = 'GTO-VVU-GUAN-023'
            hostid = '15098'
            devices_df = await reset_service.reset_pmi(affiliation, hostid)
            print(devices_df)

        self.loop.run_until_complete(async_test())


    def test_reset_pmi_5_min_arriba(self):
        print("> Entrando test_reset_pmi <")
        '''
            Prueba de 5 min y no response
            PMI Arriba
        '''

        reset_service = ResetServiceImpl()

        async def async_test():
            affiliation = 'GTO-VVU-GUAN-023'
            hostid = '15098'
            devices_df = await reset_service.reset_pmi(affiliation)
            print("devices_df:::::", devices_df)

        self.loop.run_until_complete(async_test())



