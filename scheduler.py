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
# Creating the Rocketry app
app = Rocketry(config={"task_execution": "async"})

# Creating some tasks
app.include_grouper(problems_schedule)
app.include_grouper(rfid_schedule)


if __name__ == "__main__":
    # If this script is run, only Rocketry is run
    app.run()
