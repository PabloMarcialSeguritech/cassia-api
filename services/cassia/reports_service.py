from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
from utils.traits import success_response
from fastapi import status
from datetime import datetime
from functools import reduce
from fastapi.exceptions import HTTPException
import tempfile
import os
import ntpath
from fastapi.responses import FileResponse
settings = Settings()


async def get_graphic_data_multiple(municipality_id: list, tech_id: list, brand_id: list, model_id: list, init_date: datetime, end_date: datetime):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    brand_id = ['' if brand == '0' else brand for brand in brand_id]
    model_id = ['' if model == '0' else model for model in model_id]
    if not verify_lenghts([municipality_id, tech_id, brand_id, model_id]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    datas = list()
    dispositivos = list()
    dias = list()
    data_range = list()
    tiempo = list()
    first = list()
    last = list()
    for ind in range(len(municipality_id)):
        statement = text(f"""
        call sp_connectivity('{municipality_id[ind]}','{tech_id[ind]}','{brand_id[ind]}','{model_id[ind]}','{init_date}','{end_date}');
        """)
        data = pd.DataFrame(session.execute(statement))
        print(statement)

        data_procesed = process_data(data, end_date, init_date)
        datas.append(data_procesed['data'])
        dispositivos.append(data_procesed['number'])
        dias.append(data_procesed['dias'])
        data_range.append(data_procesed['data_range'])
        tiempo.append(data_procesed['tiempo'])
        first.append(data_procesed['first'])
        last.append(data_procesed['last'])
        print(datas[ind].to_string())
    merged_df = pd.DataFrame()
    promedios = list()
    if len(datas) > 1:
        merged_df = datas[0]
        ind = 1
        promedios.append(merged_df.loc[:, 'Disponibilidad'].mean())
        for df in datas[1:]:
            print(ind)
            merged_df = pd.merge(merged_df, df, on='Tiempo',
                                 how='inner', suffixes=[f'_{ind}', f'_{ind+1}'])
            promedios.append(df.loc[:, 'Disponibilidad'].mean())
            ind += 1
    elif len(datas) > 0 and len(datas) <= 1:
        merged_df = datas[0]
        promedios = [merged_df.loc[:, 'Disponibildad'].mean()]

    print(merged_df.to_string())
    session.close()
    metrics = [
        {'metric_name': "Conectividad",
         'availability_average': promedios,
         'days': dias,
         'device_count': dispositivos,
         'data_range': data_range,
         'time': tiempo,
         'first_data': first,
         'last_data': last,
         'dataset': merged_df.to_dict(orient="records")
         }
    ]

    response = {

        'general_funcionality_average': promedios,
        'metrics': metrics
    }

    return success_response(data=response)


def process_data(data, end_date, init_date):
    number = data['itemid'].nunique()
    data = data.groupby(['time']).sum(
    ).astype(float).apply(lambda x: round(x/number*100, 6)).reset_index()
    data = data[['time', 'num', 'Avg_min']]
    diff = end_date-init_date
    hours = diff.days*24 + diff.seconds//3600
    data_range = "horas"
    first = data['time'][0]
    last = data['time'][len(data)-1]

    if hours > 14400:
        data = data.groupby(
            [pd.to_datetime(data['time']).dt.floor('8640H').rename("date").dt.strftime('%Y')])[['num', 'Avg_min']].mean().round(6).reset_index()
        data = data[['date', 'num', 'Avg_min']]
        data.rename(columns={'date': 'time'}, inplace=True)
        data_range = "años"
    if hours > 3696 and hours <= 14400:
        data = data.groupby(
            [pd.to_datetime(data['time']).dt.floor('720H').rename("date").dt.strftime('%Y-%m')])[['num', 'Avg_min']].mean().round(6).reset_index()
        data = data[['date', 'num', 'Avg_min']]
        data.rename(columns={'date': 'time'}, inplace=True)
        data_range = "meses"
    if hours > 504 and hours <= 3696:
        data = data.groupby(
            [pd.to_datetime(data['time']).dt.floor('168H').rename("date").dt.strftime('%Y-%m-%d')])[['num', 'Avg_min']].mean().round(6).reset_index()
        data = data[['date', 'num', 'Avg_min']]
        data.rename(columns={'date': 'time'}, inplace=True)
        data_range = "semanas"
    if hours > 24 and hours <= 504:
        data = data.groupby(
            [pd.to_datetime(data['time']).dt.floor('24H').rename("date").dt.strftime('%Y-%m-%d')])[['num', 'Avg_min']].mean().round(6).reset_index()
        data = data[['date', 'num', 'Avg_min']]
        data.rename(columns={'date': 'time'}, inplace=True)
        data_range = "dias"
        print("aqui")
    tiempo = f"{len(data)} {data_range}"
    dias = round(hours / 24, 6)
    availability_avg = data.loc[:, 'Avg_min'].mean()
    data.rename(columns={'Avg_min': 'Disponibilidad',
                         'time': 'Tiempo'}, inplace=True)
    response = {
        'data': data,
        'number': number,
        'dias': dias,
        'data_range': data_range,
        'tiempo': tiempo,
        'first': first,
        'last': last
    }
    return response


async def get_graphic_data(municipality_id: str, tech_id: str, brand_id: str, model_id: str, init_date: datetime, end_date: datetime):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    brand_id = ['' if brand == 0 else brand for brand in brand_id]
    model_id = ['' if model == 0 else model for model in model_id]

    brand_id = '' if '0' else brand_id
    model_id = '' if '0' else brand_id

    print(municipality_id)

    statement = text(f"""
    call sp_connectivity('{municipality_id}','{tech_id}','{brand_id}','{model_id}','{init_date}','{end_date}');
    """)
    data = pd.DataFrame(session.execute(statement))
    print(statement)
    number = 0
    data_range = ''
    tiempo = ''
    first = ''
    last = ''
    dias = ''
    availability_avg = 0
    if len(data) > 0:
        number = data['itemid'].nunique()
        data = data.groupby(['time']).sum(
        ).astype(float).apply(lambda x: round(x/number*100, 6)).reset_index()
        data = data[['time', 'num', 'Avg_min']]
        diff = end_date-init_date
        hours = diff.days*24 + diff.seconds//3600
        data_range = "horas"
        first = data['time'][0]
        last = data['time'][len(data)-1]

        if hours > 14400:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('8640H').rename("date").dt.strftime('%Y')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "años"

        if hours > 3696 and hours <= 14400:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('720H').rename("date").dt.strftime('%Y-%m')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "meses"
        if hours > 504 and hours <= 3696:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('168H').rename("date").dt.strftime('%Y-%m-%d')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "semanas"
        if hours > 24 and hours <= 504:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('24H').rename("date").dt.strftime('%Y-%m-%d')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "dias"
            print("aqui")
        tiempo = f"{len(data)} {data_range}"
        dias = round(hours / 24, 6)
        availability_avg = data.loc[:, 'Avg_min'].mean()
        data.rename(columns={'Avg_min': 'Disponibilidad',
                    'time': 'Tiempo'}, inplace=True)
    session.close()
    metrics = [
        {'metric_name': "Conectividad",
         'availability_average': round(availability_avg, 6),
         'days': dias,
         'devices': number,
         'dataset': data.to_dict(orient="records")
         }
    ]
    response = {
        'device_count': number,
        'data_range': data_range,
        'time': tiempo,
        'first_data': first,
        'last_data': last,
        'days': dias,
        'availability_average': round(availability_avg, 6),
        'general_funcionality_average': round(availability_avg, 6),
        'metrics': metrics
    }
    return success_response(data=response)


def Average(lst):
    return reduce(lambda a, b: a + b, lst) / len(lst)


async def download_graphic_data_multiple(municipality_id: list, tech_id: list, brand_id: list, model_id: list, init_date: datetime, end_date: datetime):
    brand_id = ['' if brand == '0' else brand for brand in brand_id]
    model_id = ['' if model == '0' else model for model in model_id]
    if not verify_lenghts([municipality_id, tech_id, brand_id, model_id]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    datas = list()

    for ind in range(len(municipality_id)):
        statement = text(f"""
        call sp_connectivity('{municipality_id[ind]}','{tech_id[ind]}','{brand_id[ind]}','{model_id[ind]}','{init_date}','{end_date}');
        """)
        data = pd.DataFrame(session.execute(statement))
        datas.append(data)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
        xlsx_filename = temp_file.name
        with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:
            ind = 1
            for data in datas:
                data.to_excel(
                    writer, sheet_name=f"Consulta {ind}", index=False)
                ind += 1
    session.close()
    return FileResponse(xlsx_filename, headers={"Content-Disposition": "attachment; filename=datos.xlsx"}, media_type="application/vnd.ms-excel", filename="datos.xlsx")


def verify_lenghts(arrays: list):
    inicial = arrays[0]
    for arreglo in arrays[1:]:
        if len(inicial) != len(arreglo):
            return False
    return True
