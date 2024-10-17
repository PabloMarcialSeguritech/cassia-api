from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from fastapi import status
from utils.traits import success_response
from utils.traits import success_response_with_alert
from models.cassia_config import CassiaConfig
from infraestructure.cassia import CassiaConfigRepository
from infraestructure.zabbix import layers_repository
from infraestructure.cassia import CassiaConfigRepository
from infraestructure.cassia import CassiaDiagnostaRepository
from infraestructure.zabbix import host_repository
from infraestructure.zabbix import AlertsRepository
import numpy as np
from datetime import datetime
import pytz
import time
import asyncio
settings = Settings()


async def get_aps_layer():
    with DB_Zabbix().Session() as session:
        statement = text(f"call sp_catTower();")
        aps = session.execute(statement)
        data = pd.DataFrame(aps).replace(np.nan, "")
        return success_response(data=data.to_dict(
            orient="records"))


async def get_aps_layer_async_backup(db):
    aps = await layers_repository.get_towers_pool(db)

    return success_response(data=aps)


async def get_aps_layer_async(db):
    aps = await layers_repository.get_towers_pool(db)
    is_licencias_df = await CassiaConfigRepository.get_config_value_by_name_pool('is_licencias', db)
    is_licencias = False
    response = aps.to_dict(orient='records')
    if not is_licencias_df.empty:
        is_licencias = is_licencias_df['value'].astype('int64')[0]
    print(is_licencias)
    print(type(is_licencias))
    if is_licencias:
        aps_licencias = aps[aps['is_own'] == 1]
        aps_siem = aps[aps['is_own'] == 0]
        response = {
            'aps_licencias': aps_licencias.to_dict(orient='records'),
            'aps_siem': aps_siem.to_dict(orient='records')
        }
    print(response)
    return success_response(data=response)


async def get_downs_layer(municipality_id, dispId, subtype_id):
    with DB_Zabbix().Session() as session:
        statement = text(
            f"call sp_hostDown('{municipality_id}','{dispId}','{subtype_id}')")

        downs = session.execute(statement)
        downs = pd.DataFrame(downs).replace(np.nan, "")
        print(len(downs))
        dependientes = text(f"""SELECT DISTINCT (hostid) from cassia_diagnostic_problems_2 cdp
where closed_at is null and depends_hostid is not null""")
        dependientes = pd.DataFrame(
            session.execute(dependientes)).replace(np.nan, '')
        if not downs.empty:
            downs['value'] = [1 for i in range(len(downs))]
            downs['description'] = [
                'ICMP ping' for i in range(len(downs))]
            downs['origen'] = [1 for i in range(len(downs))]
        statement = text(
            f"call sp_hostDown('0','','')")

        downs_totales = session.execute(statement)
        downs_totales = pd.DataFrame(downs_totales).replace(np.nan, "")
        dependientes_filtro = text(
            f"call sp_diagnostic_problemsD('{municipality_id}','{dispId}')")
        dependientes_filtro = pd.DataFrame(
            session.execute(dependientes_filtro)).replace(np.nan, '')
        "GLOBAL"
        glob = {'downs_totales': 0,
                'downs_dependientes': 0,
                'downs_origen': 0}
        filtro = {
            'downs_totales': 0,
            'downs_dependientes': 0,
            'downs_origen': 0
        }
        if not downs_totales.empty:
            downs_totales_count = len(downs_totales)
            dependientes_count = len(dependientes)
            origenes_count = downs_totales_count-dependientes_count
            glob = {
                'downs_totales': downs_totales_count,
                'downs_dependientes': dependientes_count,
                'downs_origen': origenes_count
            }
        if not downs.empty:
            downs_count = len(downs)
            dependientes_count = len(dependientes_filtro)
            origenes_count = downs_count-dependientes_count
            filtro = {
                'downs_totales': downs_count,
                'downs_dependientes': dependientes_count,
                'downs_origen': origenes_count
            }
        if not downs.empty and not dependientes.empty:
            """ downs = downs[~downs['hostid'].isin(
                dependientes['hostid'].to_list())] """
            print("F1")
            downs.loc[downs['hostid'].isin(
                dependientes['hostid'].to_list()), 'origen'] = 0
        print(len(downs))
        response = {"downs": downs.to_dict(
            orient="records"), 'global_count': glob, 'filter_count': filtro
        }
        print(response)
        return success_response(data=response)


async def get_downs_layer_async(municipality_id, dispId, subtype_id, db):
    tasks = {
        'downs_df': asyncio.create_task(layers_repository.get_host_downs_pool(municipality_id, dispId, subtype_id, db)),
        'dependientes_df': asyncio.create_task(layers_repository.get_host_downs_dependientes_pool(db)),
        'downs_origen_filtro_df': asyncio.create_task(CassiaDiagnostaRepository.get_downs_origen_pool(municipality_id, dispId, db)),
        'downs_origen_df': asyncio.create_task(CassiaDiagnostaRepository.get_downs_origen_pool(0, '', db)),
        'downs_totales_df': asyncio.create_task(layers_repository.get_host_downs_pool(0, '', '', db)),
        'dependientes_filtro_df': asyncio.create_task(layers_repository.get_host_downs_dependientes_filtro_pool(municipality_id, dispId, db)),
    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    downs = dfs['downs_df']
    dependientes = dfs['dependientes_df']
    if dependientes.empty:
        dependientes = pd.DataFrame(columns=['hostid'])
    downs_origen_filtro = dfs['downs_origen_filtro_df']
    downs_origen = dfs['downs_origen_df']
    """ downs_filtro = downs_origen """
    """ downs_filtro = await AlertsRepository.get_problems_filter(municipality_id, dispId, subtype_id, "6")
    downs_filtro = downs_filtro[downs_filtro['tipo'] == 1] """
    if not downs.empty:
        downs['value'] = [1 for i in range(len(downs))]
        downs['description'] = [
            'ICMP ping' for i in range(len(downs))]
        downs['origen'] = [0 for i in range(len(downs))]
    if 'hostid' in downs_origen.columns:
        if not downs.empty:
            downs_odd_no_analizados = downs[~downs['hostid'].isin(
                dependientes['hostid'].to_list())]

            downs_odd_analizados = downs[downs['hostid'].isin(
                downs_origen['hostid'].to_list())]
            downs_origen = pd.concat(
                [downs_odd_no_analizados, downs_odd_analizados])
            downs_origen = downs_origen.drop_duplicates()
            downs.loc[downs['hostid'].isin(
                downs_origen['hostid'].to_list()), 'origen'] = 1
            if not downs_origen_filtro.empty:
                downs_odd_no_analizados = downs[~downs['hostid'].isin(
                    dependientes['hostid'].to_list())]

                downs_odd_analizados = downs[downs['hostid'].isin(
                    downs_origen_filtro['hostid'].to_list())]
                downs_origen_filtro = pd.concat(
                    [downs_odd_no_analizados, downs_odd_analizados])
                downs_origen_filtro = downs_origen_filtro.drop_duplicates()
            """ downs[downs['hostid'].isin(downs_origen['hostid'].to_list())] = 1 """

    downs_totales = dfs['downs_totales_df']
    # AQUI
    dependientes_filtro = dfs['dependientes_filtro_df']
    glob = {'downs_totales': 0,
            'downs_dependientes': 0,
            'downs_origen': 0}
    filtro = {
        'downs_totales': 0,
        'downs_dependientes': 0,
        'downs_origen': 0
    }
    if not downs_totales.empty:
        downs_totales_count = len(downs_totales)
        dependientes_count = len(dependientes)
        origenes_count = len(downs_origen)

        glob = {
            'downs_totales': downs_totales_count,
            'downs_dependientes': dependientes_count,
            'downs_origen': origenes_count
        }
    if not downs.empty:
        downs_count = len(downs)
        dependientes_count = len(dependientes_filtro)
        origenes_count = len(downs_origen_filtro)
        filtro = {
            'downs_totales': downs_count,
            'downs_dependientes': dependientes_count,
            'downs_origen': origenes_count
        }
    if not downs.empty and not dependientes.empty:
        downs.loc[downs['hostid'].isin(
            dependientes['hostid'].to_list()), 'origen'] = 0

    response = {"downs": downs.to_dict(
        orient="records"), 'global_count': glob, 'filter_count': filtro,
    }
    return success_response(data=response)


async def get_downs_layer_async_backup(municipality_id, dispId, subtype_id):

    downs = await layers_repository.get_host_downs(municipality_id, dispId, subtype_id)
    print(downs)
    dependientes = await layers_repository.get_host_downs_dependientes()
    downs_origen_filtro = await CassiaDiagnostaRepository.get_downs_origen(municipality_id, dispId)
    downs_origen = await CassiaDiagnostaRepository.get_downs_origen(0, '')
    """ downs_filtro = downs_origen """
    """ downs_filtro = await AlertsRepository.get_problems_filter(municipality_id, dispId, subtype_id, "6")
    downs_filtro = downs_filtro[downs_filtro['tipo'] == 1] """
    if not downs.empty:
        downs['value'] = [1 for i in range(len(downs))]
        downs['description'] = [
            'ICMP ping' for i in range(len(downs))]
        downs['origen'] = [0 for i in range(len(downs))]
    if 'hostid' in downs_origen.columns:
        if not downs.empty:
            downs_odd_no_analizados = downs[~downs['hostid'].isin(
                dependientes['hostid'].to_list())]

            downs_odd_analizados = downs[downs['hostid'].isin(
                downs_origen['hostid'].to_list())]
            downs_origen = pd.concat(
                [downs_odd_no_analizados, downs_odd_analizados])
            downs_origen = downs_origen.drop_duplicates()
            downs.loc[downs['hostid'].isin(
                downs_origen['hostid'].to_list()), 'origen'] = 1
            if not downs_origen_filtro.empty:
                downs_odd_no_analizados = downs[~downs['hostid'].isin(
                    dependientes['hostid'].to_list())]

                downs_odd_analizados = downs[downs['hostid'].isin(
                    downs_origen_filtro['hostid'].to_list())]
                downs_origen_filtro = pd.concat(
                    [downs_odd_no_analizados, downs_odd_analizados])
                downs_origen_filtro = downs_origen_filtro.drop_duplicates()
            """ downs[downs['hostid'].isin(downs_origen['hostid'].to_list())] = 1 """

    downs_totales = await layers_repository.get_host_downs(0, '', '')
    # AQUI
    dependientes_filtro = await layers_repository.get_host_downs_dependientes_filtro(municipality_id, dispId)
    glob = {'downs_totales': 0,
            'downs_dependientes': 0,
            'downs_origen': 0}
    filtro = {
        'downs_totales': 0,
        'downs_dependientes': 0,
        'downs_origen': 0
    }
    if not downs_totales.empty:
        downs_totales_count = len(downs_totales)
        dependientes_count = len(dependientes)
        origenes_count = len(downs_origen)

        glob = {
            'downs_totales': downs_totales_count,
            'downs_dependientes': dependientes_count,
            'downs_origen': origenes_count
        }
    if not downs.empty:
        downs_count = len(downs)
        dependientes_count = len(dependientes_filtro)
        origenes_count = len(downs_origen_filtro)
        filtro = {
            'downs_totales': downs_count,
            'downs_dependientes': dependientes_count,
            'downs_origen': origenes_count
        }
    if not downs.empty and not dependientes.empty:
        downs.loc[downs['hostid'].isin(
            dependientes['hostid'].to_list()), 'origen'] = 0

    response = {"downs": downs.to_dict(
        orient="records"), 'global_count': glob, 'filter_count': filtro,
    }
    return success_response(data=response)


async def get_downs_layer_async_backup(municipality_id, dispId, subtype_id):
    downs = await layers_repository.get_host_downs(municipality_id, dispId, subtype_id)
    dependientes = await layers_repository.get_host_downs_dependientes()
    downs_filtro = await AlertsRepository.get_problems_filter(municipality_id, dispId, subtype_id, "6")
    if not downs.empty:
        downs['value'] = [1 for i in range(len(downs))]
        downs['description'] = [
            'ICMP ping' for i in range(len(downs))]
        downs['origen'] = [0 for i in range(len(downs))]
    if not downs_filtro.empty:
        downs_origen = downs_filtro
        if not downs.empty:
            downs.loc[downs['hostid'].isin(
                downs_origen['hostid'].to_list()), 'origen'] = 1
            """ downs[downs['hostid'].isin(downs_origen['hostid'].to_list())] = 1 """
    downs_totales = await layers_repository.get_host_downs(0, '', '')
    dependientes_filtro = await layers_repository.get_host_downs_dependientes_filtro(municipality_id, dispId)
    glob = {'downs_totales': 0,
            'downs_dependientes': 0,
            'downs_origen': 0}
    filtro = {
        'downs_totales': 0,
        'downs_dependientes': 0,
        'downs_origen': 0
    }
    if not downs_totales.empty:
        downs_totales_count = len(downs_totales)
        dependientes_count = len(dependientes)
        origenes_count = downs_totales_count-dependientes_count
        glob = {
            'downs_totales': downs_totales_count,
            'downs_dependientes': dependientes_count,
            'downs_origen': origenes_count
        }
    if not downs.empty:
        downs_count = len(downs)
        dependientes_count = len(dependientes_filtro)
        origenes_count = downs_count-dependientes_count
        filtro = {
            'downs_totales': downs_count,
            'downs_dependientes': dependientes_count,
            'downs_origen': origenes_count
        }
    if not downs.empty and not dependientes.empty:
        downs.loc[downs['hostid'].isin(
            dependientes['hostid'].to_list()), 'origen'] = 0

    response = {"downs": downs.to_dict(
        orient="records"), 'global_count': glob, 'filter_count': filtro,
    }
    return success_response(data=response)


async def get_downs_origin_layer(municipality_id, dispId, subtype_id):
    with DB_Zabbix().Session() as session:
        statement = text(
            f"call sp_hostDown('{municipality_id}','{dispId}','{subtype_id}')")

        downs = session.execute(statement)
        downs = pd.DataFrame(downs).replace(np.nan, "")

        statement = text(
            f"call sp_hostDown('0','','')")

        downs_globales = session.execute(statement)
        downs_globales = pd.DataFrame(downs_globales).replace(np.nan, "")
        print(len(downs))
        dependientes = text(f"""SELECT DISTINCT (hostid) from cassia_diagnostic_problems_2 cdp
where closed_at is null and depends_hostid is not null""")
        dependientes = pd.DataFrame(
            session.execute(dependientes)).replace(np.nan, '')
        statement = text(
            f"call sp_hostDown('0','','')")
        downs_filtro = await downs_count(municipality_id, dispId, subtype_id, session)
        if not downs.empty:
            downs_totales = len(downs)
            origenes = len(downs_filtro)
        filtro = {'downs_totales': downs_totales,
                  'downs_dependientes': 0,
                  'downs_origen': origenes}
        downs_globales_problems = await downs_count(0, dispId, '', session)
        if not downs_globales.empty:
            print(len(downs_globales))
            downs_totales = len(downs_globales)
            origenes = len(downs_globales_problems)
            print(origenes)
            """ data4['Downs_origen'] = origenes """
        glob = {
            'downs_totales': downs_totales,
            'downs_dependientes': 0,
            'downs_origen': origenes
        }

        if not downs.empty:
            downs['value'] = [1 for i in range(len(downs))]
            downs['description'] = [
                'ICMP ping' for i in range(len(downs))]
            downs['origen'] = [1 for i in range(len(downs))]
        print(len(downs))
        response = {"downs": downs.to_dict(
            orient="records"), 'global_count': glob, 'filter_count': filtro
        }
        return success_response(data=response)


async def get_downs_origin_layer_async(municipality_id, dispId, subtype_id, lenght, db):
    init = time.time()
    tasks = {'downs': layers_repository.get_host_downs_pool(
        municipality_id, dispId, subtype_id, db),
        'downs_excepcion': layers_repository.get_host_down_excepciones_pool(db),
        'downs_globales': layers_repository.get_host_downs_pool('0', '', '', db),
        'dependientes': layers_repository.get_host_downs_dependientes_pool(db),
        'max_count': CassiaConfigRepository.get_config_value_by_name_pool('max_odd_count', db)}
    results = await asyncio.gather(*tasks.values())
    dfs = {
        'downs_df': results[0],
        'downs_excepcion_df': results[1],
        'downs_globales_df': results[2],
        'dependientes_df': results[3],
        'max_count_df': results[4],
    }
    end = time.time()
    print(end-init)
    init = time.time()
    downs = dfs['downs_df']
    print("********DOWNS********")
    print(downs)
    downs_excepcion = dfs['downs_excepcion_df']
    print("********DOWNS EXCEPCION********")
    print(downs_excepcion)
    downs = downs[~downs['hostid'].isin(downs_excepcion['hostid'].to_list())]
    print("***********DOWNS SIN EXCEPCION***********")
    print(downs)
    count_downs = len(downs)

    downs_globales = dfs['downs_globales_df']
    downs_globales = downs_globales[~downs_globales['hostid'].isin(
        downs_excepcion['hostid'].to_list())]
    count_downs_globales = len(downs_globales)

    dependientes = dfs['dependientes_df']
    print("***********DEPENDIENTES***********")
    print(dependientes)
    if dependientes.empty:
        dependientes = pd.DataFrame(columns=['hostid'])
    origenes = downs[~downs['hostid'].isin(dependientes['hostid'].to_list())]
    print("***********ORIGENES***********")
    print(origenes)
    count_origenes = len(origenes)
    origenes_globales = downs_globales[~downs_globales['hostid'].isin(
        dependientes['hostid'].to_list())]
    count_origenes_globales = len(origenes_globales)

    filtro = {'downs_totales': count_downs,
              'downs_dependientes': 0,
              'downs_origen': count_origenes}
    glob = {
        'downs_totales': count_downs_globales,
        'downs_dependientes': 0,
        'downs_origen': count_origenes_globales
    }
    if not origenes.empty:
        origenes['value'] = [1 for i in range(len(origenes))]
        origenes['description'] = [
            'ICMP ping' for i in range(len(origenes))]
        origenes['origen'] = [1 for i in range(len(origenes))]

    max_count = dfs['max_count_df']
    print(f"RESULT MAX COUNT QUERY: {max_count}")
    if not max_count.empty:
        max_count = max_count['value'].astype(int)[0]

    else:
        max_count = None
    print(f"RESULT MAX COUNT LOGIC: {max_count}")
    odd_len = max_count if max_count is not None else 500
    if max_count != lenght:
        if max_count is not None:
            odd_len = lenght
    print(f"ODD LEN: {odd_len}")
    print(f"ODD COUNT: {len(origenes)}")

    if not origenes.empty:
        if len(origenes) > odd_len:
            hostids = ",".join(
                [str(hostid) for hostid in origenes['hostid'].to_list()])
            origenes_dates = await CassiaDiagnostaRepository.get_downs_dates_pool(hostids, odd_len, db)
            origenes = pd.merge(origenes_dates, origenes,
                                how='left', on='hostid')

    response = {"downs": origenes.to_dict(
        orient="records"), 'global_count': glob, 'filter_count': filtro
    }

    return success_response(data=response)


async def get_downs_origin_layer_async_backupppppp(municipality_id, dispId, subtype_id):
    print("get_downs_origin_layer_async function")
    downs = await layers_repository.get_host_downs(municipality_id, dispId, subtype_id)
    print("1.1")
    downs_aaa = downs.copy()
    downs_globales = await layers_repository.get_host_downs('0', '', '')
    print("1.2")
    dependientes = await layers_repository.get_host_downs_dependientes()
    print("1.3")
    downs_filtro = await AlertsRepository.get_problems_filter(municipality_id, dispId, subtype_id, "6")
    print("2")
    if not downs_filtro.empty:
        downs_filtro = downs_filtro[downs_filtro['Estatus'] == 'PROBLEM']
        downs_filtro = downs_filtro[downs_filtro['recovered_at'] != None]
        if not downs_filtro.empty:
            downs_filtro = downs_filtro[downs_filtro['tipo'] == 1]

    if not downs.empty:
        print("22")
        downs_totales = len(downs)
        origenes = len(downs_filtro)
    if 'hostid' in downs_filtro.columns:
        downs_origen = downs_filtro
        if not downs.empty:
            print("23")
            downs_odd_no_analizados = pd.DataFrame(columns=downs.columns)
            downs_odd_analizados = pd.DataFrame(columns=downs.columns)
            if not dependientes.empty:
                downs_odd_no_analizados = downs[~downs['hostid'].isin(
                    dependientes['hostid'].to_list())]
            if not downs_origen.empty:
                downs_odd_analizados = downs[downs['hostid'].isin(
                    downs_origen['hostid'].to_list())]
                downs = pd.concat(
                    [downs_odd_no_analizados, downs_odd_analizados])
                downs = downs.drop_duplicates()
            origenes = len(downs)
    filtro = {'downs_totales': downs_totales,
              'downs_dependientes': 0,
              'downs_origen': origenes}
    # AQUI
    print("3")
    downs_globales_problems = await AlertsRepository.get_problems_filter(0, dispId, '', "6")
    if not downs_globales_problems.empty:
        print("33")
        downs_globales_problems = downs_globales_problems[
            downs_globales_problems['Estatus'] == 'PROBLEM']
        """ downs_globales_problems = downs_globales_problems[downs_globales_problems['recovered_at'] != None] """
        if not downs_globales_problems.empty:
            downs_globales_problems = downs_globales_problems[downs_globales_problems['tipo'] == 1]
    if 'hostid' in downs_globales.columns:
        print("34")
        downs_totales = len(downs_globales)
        origenes_globales = len(downs_globales_problems)
        if not downs_globales.empty:
            downs_odd_globales_no_analizados = pd.DataFrame(
                columns=downs.columns)
            downs_odd_globales_analizados = pd.DataFrame(columns=downs.columns)
            if not dependientes.empty:
                downs_odd_globales_no_analizados = downs_globales[~downs_globales['hostid'].isin(
                    dependientes['hostid'].to_list())]
            if not downs_globales_problems.empty:
                downs_odd_globales_analizados = downs_globales[downs_globales['hostid'].isin(
                    downs_globales_problems['hostid'].to_list())]
                downs_globales = pd.concat(
                    [downs_odd_globales_no_analizados, downs_odd_globales_analizados])
                downs_globales = downs_globales.drop_duplicates()
            origenes_globales = len(downs_globales)
    glob = {
        'downs_totales': downs_totales,
        'downs_dependientes': 0,
        'downs_origen': origenes_globales
    }
    if not downs.empty:
        downs['value'] = [1 for i in range(len(downs))]
        downs['description'] = [
            'ICMP ping' for i in range(len(downs))]
        downs['origen'] = [1 for i in range(len(downs))]

    response = {"downs": downs.to_dict(
        orient="records"), 'global_count': glob, 'filter_count': filtro
    }
    print("4")
    print(response)
    return success_response(data=response)


async def downs_count(municipalityId, dispId, subtype, session):
    severities = "6"
    if subtype == "0":
        subtype = ""

    rfid_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "rfid_id").first()
    rfid_id = "9"
    if rfid_config:
        rfid_id = rfid_config.value
    lpr_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "lpr_id").first()
    lpr_id = "1"
    if lpr_config:
        lpr_id = lpr_config.value
    if subtype == "376276" or subtype == "375090":
        subtype = '376276,375090'
    """ if tech_host_type == "11":
        tech_host_type = "11,2" """
    if subtype != "" and tech_host_type == "":
        tech_host_type = "0"
    switch_config = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_id").first()
    switch_id = "12"

    if switch_config:
        switch_id = switch_config.value

    metric_switch_val = "Interface Bridge-Aggregation_: Bits"
    metric_switch = session.query(CassiaConfig).filter(
        CassiaConfig.name == "switch_throughtput").first()
    if metric_switch:
        metric_switch_val = metric_switch.value
    if subtype == metric_switch_val:
        subtype = ""
    statement = text(
        f"call sp_viewProblem('{municipalityId}','{dispId}','{subtype}','')")

    problems = session.execute(statement)
    data = pd.DataFrame(problems).replace(np.nan, "")
    ping_loss_message = session.query(CassiaConfig).filter(
        CassiaConfig.name == "ping_loss_message").first()
    ping_loss_message = ping_loss_message.value if ping_loss_message else "Unavailable by ICMP ping"
    if not data.empty:
        data['tipo'] = [0 for i in range(len(data))]
        data.loc[data['Problem'] ==
                 ping_loss_message, 'tipo'] = 1
        data['local'] = [0 for i in range(len(data))]
        data['dependents'] = [0 for i in range(len(data))]
        data['alert_type'] = ["" for i in range(len(data))]

    downs_origen = text(
        f"""call sp_diagnostic_problems1('{municipalityId}','{dispId}')""")
    downs_origen = pd.DataFrame(session.execute(downs_origen))
    if not downs_origen.empty:
        """ data['tipo'] = [0 for i in range(len(data))]
        data.loc[data['hostid'].astype(int).isin(
            downs_origen['hostid'].tolist()), 'tipo'] = 1
        data['local'] = [0 for i in range(len(data))]
        data.loc[data['hostid'].astype(int).isin(
            downs_origen['hostid'].tolist()), 'local'] = 0
        data['dependents'] = [0 for i in range(len(data))] """
        data_problems = text(
            """select cate.*,cdp.dependents,IFNULL(cea.message,'') as Ack_message from cassia_arch_traffic_events_2 cate
left join (select eventid,MAX(cea.acknowledgeid) acknowledgeid
from cassia_event_acknowledges cea group by eventid ) as ceaa
on  cate.cassia_arch_traffic_events_id=ceaa.eventid
left join cassia_event_acknowledges cea on cea.acknowledgeid  =ceaa.acknowledgeid
left join cassia_diagnostic_problems_2 cdp on cdp.local_eventid=cate.cassia_arch_traffic_events_id
where cate.closed_at is NULL and cate.hostid in :hostids""")
        """select cate.*,cdp.dependents  from cassia_arch_traffic_events cate
left join cassia_diagnostic_problems cdp on cdp.eventid=cate.cassia_arch_traffic_events_id
where cate.closed_at is NULL and cate.hostid in :hostids """
        """ print(data_problems) """
        data_problems = pd.DataFrame(session.execute(
            data_problems, {'hostids': downs_origen['hostid'].tolist()})).replace(np.nan, 0)

        if not data_problems.empty:
            """ data_problems['TimeRecovery'] = [
                '' for i in range(len(data_problems))] """
            data_problems['r_eventid'] = [
                '' for i in range(len(data_problems))]
            data_problems['Ack'] = [0 for i in range(len(data_problems))]
            """ data_problems['Ack_message'] = [
                '' for i in range(len(data_problems))] """
            data_problems['manual_close'] = [
                1 for i in range(len(data_problems))]
            data_problems['dependents'] = [
                0 for i in range(len(data_problems))]
            data_problems['local'] = [
                1 for i in range(len(data_problems))]
            data_problems['tipo'] = [
                1 for i in range(len(data_problems))]
            data_problems.drop(columns={'updated_at', 'tech_id'}, inplace=True)
            data_problems['created_at'] = pd.to_datetime(
                data_problems['created_at'])
            data_problems["created_at"] = data_problems['created_at'].dt.strftime(
                '%d/%m/%Y %H:%M:%S')
            data_problems.rename(columns={
                'created_at': 'Time',
                'closed_at': 'TimeRecovery',
                'hostname': 'Host',
                'message': 'Problem',
                'status': 'Estatus',
                'cassia_arch_traffic_events_id': 'eventid',
            }, inplace=True)

            if severities != "":
                severities = severities.split(',')
                severities = [int(severity) for severity in severities]
            else:
                severities = [1, 2, 3, 4, 5, 6]
            if 6 in severities:
                downs = data_problems[data_problems['Problem']
                                      == ping_loss_message]
            data_problems = data_problems[data_problems['severity'].isin(
                severities)]
            if 6 in severities:
                data_problems = pd.concat([data_problems, downs],
                                          ignore_index=True).replace(np.nan, "")

            data = pd.concat([data_problems, data],
                             ignore_index=True).replace(np.nan, "")
    dependientes_filtro = text(
        f"call sp_diagnostic_problemsD('{municipalityId}','{dispId}')")
    dependientes_filtro = pd.DataFrame(
        session.execute(dependientes_filtro)).replace(np.nan, '')
    """ host = dependientes_filtro[dependientes_filtro['hostid'] == 16143]
        print(host.to_string())
        print(dependientes_filtro) """
    if not dependientes_filtro.empty:
        indexes = data[data['Problem'] == ping_loss_message]
        indexes = indexes[indexes['hostid'].isin(
            dependientes_filtro['hostid'].to_list())]
        data.loc[data.index.isin(indexes.index.to_list()), 'tipo'] = 0

    sincronizados_totales = text("""select * from cassia_diagnostic_problems_2 cdp
where cdp.closed_at is NULL""")

    sincronizados_totales = pd.DataFrame(
        session.execute(sincronizados_totales)).replace(np.nan, 0)
    if not sincronizados_totales.empty:
        if not data.empty:
            for ind in data.index:
                if data['Problem'][ind] == ping_loss_message:
                    dependientes = sincronizados_totales[sincronizados_totales['hostid_origen']
                                                         == data['hostid'][ind]]
                    """ print(dependientes) """
                    dependientes['depends_hostid'] = dependientes['depends_hostid'].astype(
                        'int')
                    dependientes = dependientes[dependientes['depends_hostid'] != 0]
                    dependientes = dependientes.drop_duplicates(
                        subset=['depends_hostid'])
                    data.loc[data.index == ind,
                             'dependents'] = len(dependientes)

    if not data.empty:
        now = datetime.now(pytz.timezone('America/Mexico_City'))
        data['fecha'] = pd.to_datetime(data['Time'], format='%d/%m/%Y %H:%M:%S').dt.tz_localize(
            pytz.timezone('America/Mexico_City'))
        data['diferencia'] = now-data['fecha']
        data['dias'] = data['diferencia'].dt.days
        data['horas'] = data['diferencia'].dt.components.hours
        data['minutos'] = data['diferencia'].dt.components.minutes
        """ print(data['diferencia']) """
        data.loc[data['alert_type'].isin(
            ['rfid', 'lpr']), 'Problem'] = data.loc[data['alert_type'].isin(['rfid', 'lpr']), ['dias', 'horas', 'minutos']].apply(lambda x:
                                                                                                                                  f"Este host no ha tenido lecturas por más de {x['dias']} dias {x['horas']} hrs {x['minutos']} min" if x['dias'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['horas']} hrs {x['minutos']} min" if x['horas'] > 0
                                                                                                                                  else f"Este host no ha tenido lecturas por más de {x['minutos']} min", axis=1)
        data = data.drop(columns=['diferencia'])
        data['diferencia'] = data.apply(
            lambda row: f"{row['dias']} dias {row['horas']} hrs {row['minutos']} min", axis=1)
        data.drop_duplicates(
            subset=['hostid', 'Problem'], inplace=True)
        """ print(data.to_string()) """

        """ data['Problem'] = data.apply(lambda x: x['diferencia'] if x['alert_type'] in [
                                     'rfid', 'lpr'] else x['Problem']) """
    """ print(data) """
    if not data.empty:
        data = data[data['Estatus'] == 'PROBLEM']
        if not data.empty:
            data = data[data['tipo'] == 1]
    """ print(data.head()) """
    return data


def get_carreteros(municipality_id):
    db_zabbix = DB_Zabbix()
    session_zabbix = db_zabbix.Session()
    if municipality_id != "0":

        statement = text("call sp_catCity()")
        municipios = session_zabbix.execute(statement)
        data = pd.DataFrame(municipios).replace(np.nan, "")
        try:
            municipio = data.loc[data["groupid"] == int(municipality_id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id is not a int value"
            )
        if len(municipio) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id not exist"
            )
        rfids = text(f"call sp_hostView('{municipality_id}','9','')")
        rfids = session_zabbix.execute(rfids)
        rfids = pd.DataFrame(rfids).replace(np.nan, "")

        session_zabbix.close()

    db_c5 = DB_C5()
    if municipality_id != "0":
        statement = text(f"""
SELECT  ISNULL(SUM(cl.lecturas),0)  as Lecturas, a.Longitud,a.Latitud FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)
WHERE m.Nombre COLLATE Latin1_General_CI_AI LIKE '{municipio['name'].values[0]}' COLLATE Latin1_General_CI_AI
group by a.Latitud, a.Longitud
order by a.Longitud,a.Latitud
""")
    else:
        statement = text("""
SELECT  ISNULL(SUM(cl.lecturas),0)  as Lecturas, a.Longitud,a.Latitud FROM RFID r
INNER JOIN ArcoRFID ar  ON (R.IdRFID = ar.IdRFID )
INNER JOIN Arco a ON (ar.IdArco =a.IdArco )
INNER JOIN ArcoMunicipio am ON (a.IdArco =am.IdArco)
INNER JOIN Municipio m ON (am.IdMunicipio =M.IdMunicipio)
LEFT JOIN Antena a2  On (r.IdRFID=a2.IdRFID)
LEFT JOIN (select lr.IdRFID,lr.IdAntena,
COUNT(lr.IdRFID) lecturas FROM LecturaRFID lr
where lr.Fecha between dateadd(minute,-5,getdate()) and getdate()
group by lr.IdRFID,lr.IdAntena) cl ON (r.IdRFID=cl.Idrfid AND a2.IdAntena=cl.idAntena)

group by a.Latitud, a.Longitud
order by a.Longitud,a.Latitud""")

    session = db_c5.Session()
    data = session.execute(statement)
    data = pd.DataFrame(data).replace(np.nan, "")
    session.close()
    return success_response(data=data.to_dict(orient="records"))


def get_carreteros2(municipality_id):
    with DB_Zabbix().Session() as session:
        rfid_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == "rfid_id").first()
        rfid_id = "9" if not rfid_id else rfid_id.value
        if municipality_id != "0":
            statement = text("call sp_catCity()")
            municipios = session.execute(statement)
            data = pd.DataFrame(municipios).replace(np.nan, "")
            try:
                municipio = data.loc[data["groupid"] == int(municipality_id)]
            except:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id is not a int value"
                )
            if len(municipio) < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id not exist"
                )
            rfids = text(
                f"call sp_hostView('{municipality_id}','{rfid_id}','')")
            rfids = pd.DataFrame(session.execute(rfids)).replace(np.nan, "")
        else:
            rfids = text(f"call sp_hostView('0','{rfid_id}','')")
            rfids = pd.DataFrame(session.execute(rfids)).replace(np.nan, "")
        if not rfids.empty:
            rfids = rfids[['latitude', 'longitude']]
            rfids = rfids[rfids['latitude'] != '--']
            rfids = rfids[rfids['longitude'] != '--']
            rfids = rfids.drop_duplicates(subset=['latitude', 'longitude'])
        data = rfids
        if municipality_id != "0":
            statement = text(f"""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude FROM cassia_arch_traffic c
where c.`date` between DATE_ADD(now(),INTERVAL -5 MINUTE) and NOW()
and c.municipality  LIKE '{municipio['name'].values[0]}'
group by c.latitude, c.longitude
""")
        else:
            statement = text("""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude
FROM cassia_arch_traffic c where c.`date`
between DATE_ADD(now(),INTERVAL -5 MINUTE) and NOW()
group by c.latitude, c.longitude
""")
        lecturas = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
        activos = text("""
SELECT DISTINCT c.longitude ,c.latitude
FROM cassia_arch_traffic c
group by c.latitude, c.longitude
""")
        activos = pd.DataFrame(session.execute(activos)).replace(np.nan, "")
        if not lecturas.empty:
            data = pd.merge(data, lecturas, how='right', left_on=['latitude', 'longitude'],
                            right_on=['latitude', 'longitude']).replace(np.nan, 0)
            data['activo'] = 0
            if not activos.empty:
                data.loc[data.set_index(['latitude', 'longitude']).index.isin(
                    activos.set_index(['latitude', 'longitude']).index), 'activo'] = 1
            print(data.to_string())

        else:
            data['Lecturas'] = [0 for i in range(len(data))]
        statement = text(f"""
SELECT max(c.severity) as max_severity, c.longitude ,c.latitude FROM cassia_arch_traffic_events c
WHERE c.closed_at is NULL
AND tech_id='{rfid_id}'
group by c.latitude, c.longitude
""")
        alerts = pd.DataFrame(session.execute(
            statement)).replace(np.nan, "")
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)
    else:
        data['max_severity'] = [0 for al in range(len(data))]
    no_ceros = data[data['Lecturas'] != 0]

    if no_ceros.empty:
        return success_response_with_alert(data=data.to_dict(orient="records"), alert="Error al conectar a la base de datos de arcos carreteros, favor de contactar a soporte.")
    return success_response_with_alert(data=data.to_dict(orient="records"))


async def get_carreteros2_async(municipality_id, db):
    tasks = {
        'rfid_id': asyncio.create_task(CassiaConfigRepository.get_config_value_by_name_pool('rfid_id', db)),
        'activos': asyncio.create_task(layers_repository.get_rfid_host_active_pool(db)),
    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    rfid_id = dfs['rfid_id']
    if not rfid_id.empty:
        rfid_id = rfid_id['value'][0]
    else:
        rfid_id = "9"

    if municipality_id != "0":
        municipios = await CassiaConfigRepository.get_city_catalog_pool(db)
        try:
            municipio = municipios.loc[municipios["groupid"] == int(
                municipality_id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id is not a int value"
            )
        if len(municipio) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id not exist"
            )
        rfids = await host_repository.get_host_view_pool(municipality_id, rfid_id, '', db)
        rfids = pd.DataFrame(rfids).replace(np.nan, "")
    else:
        rfids = await host_repository.get_host_view_pool('0', rfid_id, '', db)
        rfids = pd.DataFrame(rfids).replace(np.nan, "")
    if not rfids.empty:
        rfids = rfids[['latitude', 'longitude']]
        rfids = rfids[rfids['latitude'] != '--']
        rfids = rfids[rfids['longitude'] != '--']
        rfids = rfids.drop_duplicates(subset=['latitude', 'longitude'])
    data = rfids

    if municipality_id != "0":
        lecturas = await layers_repository.get_rfid_readings_by_municipality_pool(municipio['name'].values[0], db)
    else:
        lecturas = await layers_repository.get_rfid_readings_global_pool(db)
    activos = dfs['activos']
    if not lecturas.empty:
        data = pd.merge(data, lecturas, how='right', left_on=['latitude', 'longitude'],
                        right_on=['latitude', 'longitude']).replace(np.nan, 0)
        data['activo'] = 0
        if not activos.empty:
            data.loc[data.set_index(['latitude', 'longitude']).index.isin(
                activos.set_index(['latitude', 'longitude']).index), 'activo'] = 1
    else:
        data['Lecturas'] = [0 for i in range(len(data))]
    alerts = await layers_repository.get_max_serverities_by_tech_pool(rfid_id, db)
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)
    else:
        data['max_severity'] = [0 for al in range(len(data))]
    no_ceros = data[data['Lecturas'] != 0]
    print(len(data))
    if no_ceros.empty:
        return success_response_with_alert(data=data.to_dict(orient="records"), alert="Error al conectar a la base de datos de arcos carreteros, favor de contactar a soporte.")
    return success_response_with_alert(data=data.to_dict(orient="records"))


async def get_carreteros2_async_backup(municipality_id):
    rfid_id = await CassiaConfigRepository.get_config_value_by_name('rfid_id')
    if not rfid_id.empty:
        rfid_id = rfid_id['value'][0]
    else:
        rfid_id = "9"

    if municipality_id != "0":
        municipios = await CassiaConfigRepository.get_city_catalog()
        try:
            municipio = municipios.loc[municipios["groupid"] == int(
                municipality_id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id is not a int value"
            )
        if len(municipio) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id not exist"
            )
        rfids = await host_repository.get_host_view(municipality_id, rfid_id, '')
        rfids = pd.DataFrame(rfids).replace(np.nan, "")
    else:
        rfids = await host_repository.get_host_view('0', rfid_id, '')
        rfids = pd.DataFrame(rfids).replace(np.nan, "")
    if not rfids.empty:
        rfids = rfids[['latitude', 'longitude']]
        rfids = rfids[rfids['latitude'] != '--']
        rfids = rfids[rfids['longitude'] != '--']
        rfids = rfids.drop_duplicates(subset=['latitude', 'longitude'])
    data = rfids

    if municipality_id != "0":
        lecturas = await layers_repository.get_rfid_readings_by_municipality(municipio['name'].values[0])
    else:
        lecturas = await layers_repository.get_rfid_readings_global()
    activos = await layers_repository.get_rfid_host_active()
    if not lecturas.empty:
        data = pd.merge(data, lecturas, how='right', left_on=['latitude', 'longitude'],
                        right_on=['latitude', 'longitude']).replace(np.nan, 0)
        data['activo'] = 0
        if not activos.empty:
            data.loc[data.set_index(['latitude', 'longitude']).index.isin(
                activos.set_index(['latitude', 'longitude']).index), 'activo'] = 1
    else:
        data['Lecturas'] = [0 for i in range(len(data))]
    alerts = await layers_repository.get_max_serverities_by_tech(rfid_id)
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)
    else:
        data['max_severity'] = [0 for al in range(len(data))]
    no_ceros = data[data['Lecturas'] != 0]
    print(len(data))
    if no_ceros.empty:
        return success_response_with_alert(data=data.to_dict(orient="records"), alert="Error al conectar a la base de datos de arcos carreteros, favor de contactar a soporte.")
    return success_response_with_alert(data=data.to_dict(orient="records"))


def get_lpr(municipality_id):
    with DB_Zabbix().Session() as session:
        lpr_id = session.query(CassiaConfig).filter(
            CassiaConfig.name == "lpr_id").first()
        lpr_id = "1" if not lpr_id else lpr_id.value
        if municipality_id != "0":
            statement = text("call sp_catCity()")
            municipios = session.execute(statement)
            data = pd.DataFrame(municipios).replace(np.nan, "")
            try:
                municipio = data.loc[data["groupid"] == int(municipality_id)]
            except:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id is not a int value"
                )
            if len(municipio) < 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Municipality id not exist"
                )
            lprs = text(f"call sp_hostView('{municipality_id}','{lpr_id}','')")
            lprs = pd.DataFrame(session.execute(lprs)).replace(np.nan, "")
        else:
            lprs = text(f"call sp_hostView('0','{lpr_id}','')")
            print(lprs)
            lprs = pd.DataFrame(session.execute(lprs)).replace(np.nan, "")
            print(lprs)
        if not lprs.empty:
            lprs = lprs[['latitude', 'longitude']]
            lprs = lprs[lprs['latitude'] != '--']
            lprs = lprs[lprs['longitude'] != '--']
            lprs = lprs.drop_duplicates(subset=['latitude', 'longitude'])
        data = lprs

        if municipality_id != "0":
            statement = text(f"""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude FROM cassia_arch_traffic_lpr c
where c.`date` between DATE_ADD(now(),INTERVAL -10 MINUTE) and NOW()
and c.municipality  LIKE '{municipio['name'].values[0]}'
group by c.latitude, c.longitude
""")
        else:
            statement = text("""
SELECT SUM(c.readings) as Lecturas, c.longitude ,c.latitude
FROM cassia_arch_traffic_lpr c where c.`date`
between DATE_ADD(now(),INTERVAL -10 MINUTE) and NOW()
group by c.latitude, c.longitude
""")
        lecturas = pd.DataFrame(session.execute(statement)).replace(np.nan, "")
        activos = text("""
SELECT DISTINCT c.longitude ,c.latitude
FROM cassia_arch_traffic_lpr c
group by c.latitude, c.longitude
""")
        activos = pd.DataFrame(session.execute(activos)).replace(np.nan, "")
        if not lecturas.empty:
            data = pd.merge(data, lecturas, how='left', left_on=['latitude', 'longitude'],
                            right_on=['latitude', 'longitude']).replace(np.nan, 0)
            data['activo'] = 0
            if not activos.empty:
                data.loc[data.set_index(['latitude', 'longitude']).index.isin(
                    activos.set_index(['latitude', 'longitude']).index), 'activo'] = 1

        else:
            data['Lecturas'] = [0 for i in range(len(data))]
        statement = text(f"""
SELECT max(c.severity) as max_severity, c.longitude ,c.latitude FROM cassia_arch_traffic_events c
where c.closed_at is NULL
AND tech_id='{lpr_id}'
group by c.latitude, c.longitude
""")
        alerts = pd.DataFrame(session.execute(
            statement)).replace(np.nan, "")
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)

    else:
        data['max_severity'] = [0 for al in range(len(data))]
    no_ceros = data[data['Lecturas'] != 0]

    if no_ceros.empty:
        return success_response_with_alert(data=data.to_dict(orient="records"), alert="Error al conectar a la base de datos de syslog, favor de contactar a soporte.")
    return success_response_with_alert(data=data.to_dict(orient="records"))


async def get_lpr_async(municipality_id, db):
    tasks = {
        'lpr_id': asyncio.create_task(CassiaConfigRepository.get_config_value_by_name_pool('lpr_id', db)),
        'activos': asyncio.create_task(layers_repository.get_lpr_host_active_pool(db)),
    }
    results = await asyncio.gather(*tasks.values())
    dfs = dict(zip(tasks.keys(), results))
    lpr_id = dfs['lpr_id']
    if not lpr_id.empty:
        lpr_id = lpr_id['value'][0]
    else:
        lpr_id = "1"
    if municipality_id != "0":
        municipios = await CassiaConfigRepository.get_city_catalog_pool(db)
        try:
            municipio = municipios.loc[municipios["groupid"] == int(
                municipality_id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id is not a int value"
            )
        if len(municipio) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id not exist"
            )
        lprs = await host_repository.get_host_view_pool(municipality_id, lpr_id, '', db)
        lprs = pd.DataFrame(lprs).replace(np.nan, "")
    else:
        lprs = await host_repository.get_host_view_pool('0', lpr_id, '', db)
        lprs = pd.DataFrame(lprs).replace(np.nan, "")
    if not lprs.empty:
        lprs = lprs[['latitude', 'longitude']]
        lprs = lprs[lprs['latitude'] != '--']
        lprs = lprs[lprs['longitude'] != '--']
        lprs = lprs.drop_duplicates(subset=['latitude', 'longitude'])
    data = lprs

    if municipality_id != "0":
        lecturas = await layers_repository.get_lpr_readings_by_municipality_pool(municipio['name'].values[0], db)
    else:
        lecturas = await layers_repository.get_lpr_readings_global_pool(db)
    activos = dfs['activos']
    if not lecturas.empty:
        data = pd.merge(data, lecturas, how='left', left_on=['latitude', 'longitude'],
                        right_on=['latitude', 'longitude']).replace(np.nan, 0)
        data['activo'] = 0
        if not activos.empty:
            data.loc[data.set_index(['latitude', 'longitude']).index.isin(
                activos.set_index(['latitude', 'longitude']).index), 'activo'] = 1
    else:
        data['Lecturas'] = [0 for i in range(len(data))]
    alerts = await layers_repository.get_max_serverities_by_tech_pool(lpr_id, db)
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)

    else:
        data['max_severity'] = [0 for al in range(len(data))]
    no_ceros = data[data['Lecturas'] != 0]

    if no_ceros.empty:
        return success_response_with_alert(data=data.to_dict(orient="records"), alert="Error al conectar a la base de datos de syslog, favor de contactar a soporte.")
    return success_response_with_alert(data=data.to_dict(orient="records"))


async def get_lpr_async_backup(municipality_id):
    lpr_id = await CassiaConfigRepository.get_config_value_by_name('lpr_id')
    if not lpr_id.empty:
        lpr_id = lpr_id['value'][0]
    else:
        lpr_id = "1"
    if municipality_id != "0":
        municipios = await CassiaConfigRepository.get_city_catalog()
        try:
            municipio = municipios.loc[municipios["groupid"] == int(
                municipality_id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id is not a int value"
            )
        if len(municipio) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id not exist"
            )
        lprs = await host_repository.get_host_view(municipality_id, lpr_id, '')
        lprs = pd.DataFrame(lprs).replace(np.nan, "")
    else:
        lprs = await host_repository.get_host_view('0', lpr_id, '')
        lprs = pd.DataFrame(lprs).replace(np.nan, "")
    if not lprs.empty:
        lprs = lprs[['latitude', 'longitude']]
        lprs = lprs[lprs['latitude'] != '--']
        lprs = lprs[lprs['longitude'] != '--']
        lprs = lprs.drop_duplicates(subset=['latitude', 'longitude'])
    data = lprs
    with DB_Zabbix().Session() as session:
        if municipality_id != "0":
            lecturas = await layers_repository.get_lpr_readings_by_municipality(municipio['name'].values[0])
        else:
            lecturas = await layers_repository.get_lpr_readings_global()
        activos = await layers_repository.get_lpr_host_active()
        if not lecturas.empty:
            data = pd.merge(data, lecturas, how='left', left_on=['latitude', 'longitude'],
                            right_on=['latitude', 'longitude']).replace(np.nan, 0)
            data['activo'] = 0
            if not activos.empty:
                data.loc[data.set_index(['latitude', 'longitude']).index.isin(
                    activos.set_index(['latitude', 'longitude']).index), 'activo'] = 1

        else:
            data['Lecturas'] = [0 for i in range(len(data))]

        alerts = await layers_repository.get_max_serverities_by_tech(lpr_id)
    if not alerts.empty and not data.empty:
        data = pd.merge(data, alerts, how="left", left_on=[
            'latitude', 'longitude'], right_on=['latitude', 'longitude']).replace(np.nan, 0)

    else:
        data['max_severity'] = [0 for al in range(len(data))]
    no_ceros = data[data['Lecturas'] != 0]

    if no_ceros.empty:
        return success_response_with_alert(data=data.to_dict(orient="records"), alert="Error al conectar a la base de datos de syslog, favor de contactar a soporte.")
    return success_response_with_alert(data=data.to_dict(orient="records"))


async def get_switches_connectivity(municipality_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    statement = text(f"call sp_switchConnectivity('{municipality_id}')")
    switches = session.execute(statement)
    data = pd.DataFrame(switches).replace(np.nan, "")
    session.close()

    return success_response(data=data.to_dict(orient="records"))


async def get_switches_connectivity_async(municipality_id):
    data = await layers_repository.get_switch_connectivity_layer(municipality_id)
    return success_response(data=data.to_dict(orient="records"))
