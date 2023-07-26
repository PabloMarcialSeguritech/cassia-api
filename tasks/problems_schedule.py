from rocketry import Rocketry, Grouper
from rocketry.conds import every, after_success
from utils.db import DB_Zabbix
from utils.db import DB_Prueba
from sqlalchemy import text
import pandas as pd
from models.problem_record import ProblemRecord
from datetime import datetime
# Creating the Rocketry app
problems_schedule = Grouper()

# Creating some tasks


#@problems_schedule.task(every("40 seconds"), execution="thread")
async def get_problems():
    print("Getting problems")
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    statement = text(
        f"call sp_viewProblem('0','','')")
    """ try: """
    problems = session.execute(statement)
    """ finally: """
    """ db_zabbix.Session().close() """
    # db_prueba = DB_Prueba.Session()
    for problem in problems:
        # record = db_prueba.query(ProblemRecord).filter(
        #    ProblemRecord.problemid == problem.eventid).first()
        record = session.query(ProblemRecord).filter(
            ProblemRecord.problemid == problem.eventid).first()

        if not record:
            print(problem.hostid)
            if problem.Estatus == "PROBLEM":
                problem_record = ProblemRecord(
                    hostid=int(problem.hostid),
                    problemid=int(problem.eventid),
                    created_at=datetime.strptime(
                        problem.Time, '%d/%m/%Y %H:%M:%S'),
                    estatus="Creado",
                    user_id=1
                )
            else:
                search = text(
                    f"SELECT eventid,r_clock FROM problem where eventid={problem.eventid}")
                """ try: """
                event = session.execute(search)
                """ finally: """
                """ db_zabbix.Session().close()
                db_zabbix.stop()
                 """
                data = pd.DataFrame(event)
                if len(data) > 0:
                    problem_record = ProblemRecord(
                        hostid=int(problem.hostid),
                        problemid=int(problem.eventid),
                        created_at=datetime.strptime(
                            problem.Time, '%d/%m/%Y %H:%M:%S'),
                        closed_at=datetime.fromtimestamp(data.iloc[0].r_clock),
                        estatus="Cerrado",
                        user_id=1
                    )
            """ db_prueba.add(problem_record)
            db_prueba.commit() """
            session.add(problem_record)
            session.commit()
    # db_prueba.close()
    session.close()
    db_zabbix.stop()


# @problems_schedule.task(every("50 seconds"), execution="main")
async def check_solved_problems():
    print("Updating problems")
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    # db_prueba = DB_Prueba.Session()
    statement = text(
        "SELECT problemid FROM problem_records WHERE estatus='Creado'")
    records = session.execute(statement)
    records = pd.DataFrame(records)
    records = records["problemid"].values.tolist()
    records = tuple(records)
    search = text(
        f"SELECT eventid,r_eventid,r_clock FROM problem where eventid in {records} and r_eventid IS NOT NULL")
    records = session.execute(search)
    data = pd.DataFrame(records)
    for ind in data.index:
        date = datetime.fromtimestamp(data['r_clock'][ind])
        update = text(
            f"UPDATE problem_records set estatus='Cerrado', closed_at='{date}' WHERE problemid={data['eventid'][ind]}")
        session.execute(update)
        session.commit()
        print(data['eventid'][ind])
    session.close()
    # db_zabbix.Session().close()
    db_zabbix.stop()
