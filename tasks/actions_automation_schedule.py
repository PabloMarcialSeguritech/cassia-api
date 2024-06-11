from rocketry import Grouper
from rocketry.conds import every
from datetime import datetime
import pytz
import utils.settings as settings

from infraestructure.cassia import cassia_automation_actions
from infraestructure.zabbix import host_repository
import asyncio
import pandas as pd
import time
import json
# Creating the Rocketry app
automation_actions_scheduler = Grouper()
settings = settings.Settings()
automation_actions = settings.automation_actions


@automation_actions_scheduler.cond('automation_actions')
def is_automation_actions():
    return automation_actions


""" @automation_actions_scheduler.task(("every 1 minute  & automation_actions"), execution="thread") """


async def process_action_values():
    host_actions_to_process = await cassia_automation_actions.get_hosts_actions_to_process()
    procesos = []
    for ind in host_actions_to_process.index:
        procesos.append(verify_conditions(host_actions_to_process['interface_id'][ind],
                                          host_actions_to_process['hostid'][ind],
                                          host_actions_to_process['condition_id'][ind],
                                          host_actions_to_process['action_auto_id'][ind]))
    await asyncio.gather(*procesos)
    return


async def verify_conditions(interface_id, hostid, condition_id, action_auto_id):
    host_metrics = pd.DataFrame(await host_repository.get_host_health_detail(hostid))
    conditions = await cassia_automation_actions.get_auto_action_conditions(condition_id)
    actual_values = await cassia_automation_actions.get_auto_action_operational_values(interface_id, action_auto_id)
    for i in conditions.index:
        value = host_metrics[host_metrics['templateid']
                             == conditions['template_id'][i]]
        template_id = conditions['template_id'][i]

        """ if value.empty:
            # VERIFICAR ESTE CASO
            print("Error, metrica no encontrada") """
        if not value.empty:
            print(f"El valor de la metrica es {value}")
            valor = value['Metric'].values[0]
            print(valor)
            if valor > conditions['range_min'][i] and valor < conditions['range_max'][i]:
                print(
                    f"El valor cumple la condicion mayor que {conditions['range_min'][i]} y menor que {conditions['range_max'][i]}")
                if actual_values.empty:
                    now = datetime.now(pytz.timezone(
                        "America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
                    values = f"({interface_id},{action_auto_id},{conditions['delay'][i]},{conditions['template_id'][i]},{valor},'{now}',NULL,'EnProceso')"
                    await cassia_automation_actions.insert_auto_action_operational_values(values)
                    print(f"INSERTADOS VALORES {values}")
                else:
                    now_str = datetime.now(pytz.timezone(
                        "America/Mexico_City")).strftime('%Y-%m-%d %H:%M:%S')
                    now = datetime.strptime(now_str, "%Y-%m-%d %H:%M:%S")
                    last_value = actual_values[actual_values['templateid']
                                               == template_id]
                    fecha = pd.to_datetime(last_value['started_at']).values[0]
                    fecha_pd = pd.Timestamp(fecha)
                    fecha = fecha_pd.strftime('%Y-%m-%d %H:%M:%S')
                    fecha = datetime.strptime(fecha, '%Y-%m-%d %H:%M:%S')
                    diferencia = now-fecha
                    diferencia_minutos = diferencia.total_seconds() / 60
                    delay = last_value['delay'].values[0]

                    print(f"Fecha: {fecha}")
                    print(f"Diferencia: {diferencia_minutos}")
                    print(f"Delay: {delay}")
                    if float(delay) >= diferencia_minutos:
                        auto_operation_id = last_value['auto_operation_id'].values[0]
                        await cassia_automation_actions.update_auto_action_operational_values(auto_operation_id, valor, now_str)
                        print("ACTUALIZADO")

            else:
                print("El valor no cumple con la condicion")
                if not actual_values.empty:
                    ids = f"({','.join([str(id_auto) for id_auto in actual_values['auto_operation_id'].to_list()])})"
                    now_str = datetime.now(pytz.timezone(
                        "America/Mexico_City")).strftime('%Y-%m-%d %H:%M:%S')
                    await cassia_automation_actions.close_auto_action_operational_values(ids, now_str)
                    print("CANCELADOS")
                    break

    print(host_metrics)
    print(conditions)
    """ print(f"Hostid {hostid} - Condition: {condition_id}")
    for i in range(5):
        print(f"Iteracion {i}")
        time.sleep(1) """


@automation_actions_scheduler.task(("every 1 minute  & automation_actions"), execution="thread")
async def process_actions():
    procesos = []
    operation_values_to_verify = await cassia_automation_actions.get_auto_action_operational_values_to_process()
    if operation_values_to_verify.empty:
        return
    operation_values_to_verify['verified'] = 0

    for i in operation_values_to_verify.index:
        if operation_values_to_verify['verified'][i] == 1:
            continue
        condicion = ((operation_values_to_verify['interface_id'] == operation_values_to_verify['interface_id'][i]) &
                     (operation_values_to_verify['action_auto_id'] == operation_values_to_verify['action_auto_id'][i]))
        values = operation_values_to_verify[condicion]
        operation_values_to_verify.loc[condicion, 'verified'] = 1
        if (values['status'] == 'EnProceso').all():
            values['started_at'] = pd.to_datetime(values['started_at'])
            now = datetime.now()
            values['minutes'] = (now - values['started_at']
                                 ).dt.total_seconds() / 60
            if (values['minutes'] > values['delay'].astype(float)).all():
                print("Todos los delay cumplen")
                procesos.append(execute_actions(
                    operation_values_to_verify['interface_id'][i], operation_values_to_verify['ip'][i], operation_values_to_verify['action_auto_id'][i], values['auto_operation_id']))
    await asyncio.gather(*procesos)


async def execute_actions(interface_id, ip, action_id, operation_ids):
    ids = ",".join([str(op_id) for op_id in operation_ids.to_list()])
    print(ids)
    return
    print(
        f"Ejecutando Acción {action_id} en interface {interface_id}... con la ip {ip}")
    result = await cassia_automation_actions.prepare_action(ip, action_id)
    print(result)
    body = result.body
    json_string = body.decode('utf-8')
    response_dict = json.loads(json_string)
    success = response_dict['success']
    if success == False:
        await cassia_automation_actions.increase_retry_times(ids)
        pass
    else:
        await cassia_automation_actions.close_auto_action(ids)
        pass
    """ print(f"total_time: {response_dict['data']['total_time']}") """
