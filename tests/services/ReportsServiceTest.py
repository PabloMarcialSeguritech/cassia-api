import unittest
from services.cassia import reports_service_
from services.cassia import reports_service
import asyncio
from datetime import datetime
from utils.db import DB_Zabbix


class ReportsServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()


    def test_process_data_conectivity(self):
        print('> Entrando a test_process_data_conectivity <')

        async def async_test():
            municipality_id = ['0']
            tech_id = ['11']
            brand_id = ['']
            model_id = ['']
            init_date = '2024-03-01 00:00:00'
            end_date = '2024-03-26 22:16:00'
            init_date_str = '2024-03-01 00:00:00'
            end_date_str = '2024-03-26 22:16:00'
            init_date = datetime.strptime(init_date_str, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
            promps = list()

            response = await reports_service_.process_data_conectivity_(municipality_id, tech_id, brand_id, model_id,
                                                                        init_date, end_date, promps)
            print("conectividad::", response)

        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_process_data_alignment")
    def test_process_data_alignment(self):
        print('> Entrando a test_process_data_alignment <')

        async def async_test():
            municipality_id = ['0']
            tech_id = ['11']
            brand_id = ['']
            model_id = ['']
            init_date_str = '2024-03-01 00:00:00'
            end_date_str = '2024-03-26 22:16:00'
            init_date = datetime.strptime(init_date_str, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
            promps = list()
            metricas = list()
            response = await reports_service_.process_data_alignment_(municipality_id, tech_id, brand_id, model_id,
                                                                      init_date, end_date, metricas, promps)



        self.loop.run_until_complete(async_test())

    @unittest.skip("Omit test_process_data_alignment2")
    def test_process_data_alignment2(self):
        print('> Entrando a test_process_data_alignment2 <')

        async def async_test():
            db_zabbix = DB_Zabbix()
            session = db_zabbix.Session()
            municipality_id = ['0']
            tech_id = ['11']
            brand_id = ['']
            model_id = ['']
            init_date_str = '2024-03-01 00:00:00'
            end_date_str = '2024-03-26 22:16:00'
            init_date = datetime.strptime(init_date_str, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
            promps = list()
            metricas = list()
            response = await reports_service.process_data_alignment(municipality_id, tech_id, brand_id, model_id,
                                                                    init_date, end_date, session, metricas, promps)

        self.loop.run_until_complete(async_test())
