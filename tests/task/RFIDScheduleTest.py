import unittest
import asyncio
from tasks import rfid_schedule


class RFIDSchedule(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()

    def test_get_rfid_data(self):
        print('> Entrando a test_get_rfid_data <')
        async def async_test():
            await rfid_schedule.get_rfid_data()
        self.loop.run_until_complete(async_test())
