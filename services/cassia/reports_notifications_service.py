from utils.settings import Settings
from utils.traits import success_response
from infraestructure.cassia import cassia_reports_notifications_repository
from fastapi import status
import time
settings = Settings()


async def get_report_names():
    report_names = await cassia_reports_notifications_repository.get_report_names()
    return success_response(data=report_names.to_dict(orient='records'))


async def get_user_reports():
    users = await cassia_reports_notifications_repository.get_users()
    report_names = await cassia_reports_notifications_repository.get_report_names()
    for ind in report_names.index:
        users[str(report_names['cassia_report_frequency_schedule_id'][ind])] = 0
    user_reports = await cassia_reports_notifications_repository.get_user_reports()
    for ind in user_reports.index:
        condicion = users['user_id'] == user_reports['user_id'][ind]
        users.loc[condicion,
                  str(user_reports['cassia_report_frequency_schedule_id'][ind])] = 1
    return success_response(data=users.to_dict(orient='records'))


async def save_user_reports(data):
    start = time.time()
    eliminar_asignaciones = await cassia_reports_notifications_repository.delete_user_reports_by_user_ids(data.user_ids)
    values = ""
    ind = 0
    print(data)
    for user_id, report_ids in zip(data.user_ids, data.cassia_report_frequency_schedule_ids):
        ind += 1
        print("AQUI")
        print(user_id)
        print(report_ids)
        if len(report_ids) > 0:
            if ind >= len(data.user_ids):
                values += ','.join(
                    [f"({user_id},{report_id})" for report_id in report_ids])
            else:
                values += ','.join(
                    [f"({user_id},{report_id})" for report_id in report_ids])+","
        print("Values")
        print(values)
    if len(values) > 0:
        if values[-1] == ',':
            values = values[0:-1]

        insertar = await cassia_reports_notifications_repository.insert_user_reports_values(values)
    end = time.time()
    tiempo = end-start
    return success_response(message="Registros actualizados correctamente")
