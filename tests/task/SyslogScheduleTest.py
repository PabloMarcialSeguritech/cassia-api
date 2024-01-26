import unittest
from tasks import syslog_schedule


class SyslogScheduleTest(unittest.TestCase):

    def test_update_syslog_data(self):
        print('> Entrando a test_update_syslog_data <')
        syslog_schedule.update_syslog_data()
