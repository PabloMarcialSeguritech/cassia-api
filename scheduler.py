from rocketry import Rocketry

from rocketry.conds import every, after_success
from utils.db import DB_Zabbix
from utils.db import DB_Prueba
from sqlalchemy import text
import pandas as pd
from models.problem_record import ProblemRecord
from datetime import datetime
from tasks.problems_schedule import problems_schedule
from tasks.rfid_schedule import rfid_schedule
from tasks.slack_notifications_schedule import slack_scheduler
from tasks.syslog_schedule import syslog_schedule
from tasks.diagnosta_schedule import diagnosta_schedule
from tasks.reports_notification_schedule import report_scheduler
from tasks.actions_automation_schedule import automation_actions_scheduler
# Creating the Rocketry app
app = Rocketry(config={"task_execution": "async"})

# Creating some tasks
app.include_grouper(problems_schedule)
app.include_grouper(rfid_schedule)
app.include_grouper(slack_scheduler)
app.include_grouper(syslog_schedule)
app.include_grouper(diagnosta_schedule)
app.include_grouper(report_scheduler)
app.include_grouper(automation_actions_scheduler)


if __name__ == "__main__":
    # If this script is run, only Rocketry is run
    app.run()
