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
    metrics = list()
    promedios = list()
    print(promedios)
    conectividad = process_data_conectivity(
        municipality_id, tech_id, brand_id, model_id, init_date, end_date, session, promedios)

    metrics.append(conectividad)
    process_data_alignment(municipality_id, tech_id, brand_id,
                           model_id, init_date, end_date, session, metrics, promedios)
    print(promedios)
    promedios = process_metrics(promedios)
    print(promedios)
    response = {
        'general_funcionality_average': promedios,
        'metrics': metrics
    }
    session.close()

    return success_response(data=response)


def process_data(data, end_date, init_date, metric_name):
    if not data.empty:
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
        if hours >= 7200 and hours <= 14400:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('720H').rename("date").dt.strftime('%Y-%m')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "meses"
        if hours > 3696 and hours < 7200:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('360H').rename("date").dt.strftime('%Y-%m-%d')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "quincenas"
        if hours > 1680 and hours <= 3696:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('168H').rename("date").dt.strftime('%Y-%m-%d')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "semanas"
        if hours > 240 and hours <= 1680:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('24H').rename("date").dt.strftime('%Y-%m-%d')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "dias"
        if hours > 120 and hours <= 240:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('12H').rename("date").dt.strftime('%Y-%m-%d %H:%M:%S')])[['num', 'Avg_min']].mean().round(6).reset_index()
            data = data[['date', 'num', 'Avg_min']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "medios dias"

        tiempo = f"{len(data)} {data_range}"
        dias = round(hours / 24, 6)
        availability_avg = data.loc[:, 'Avg_min'].mean()
        data.rename(columns={'Avg_min': metric_name,
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
    else:
        response = {
            'data': data,
            'number': 0,
            'dias': 0,
            'data_range': 0,
            'tiempo': 0,
            'first': 0,
            'last': 0
        }
    return response


def process_data_alignment2(data, end_date, init_date):
    if not data.empty:
        number = data['itemid'].nunique()
        data = data.groupby(['time']).sum(
        ).astype(float).apply(lambda x: round(x/number*100, 6)).reset_index()
        data = data[['time', 'value_avg', 'a_avg']]
        diff = end_date-init_date
        hours = diff.days*24 + diff.seconds//3600
        data_range = "horas"
        first = data['time'][0]
        last = data['time'][len(data)-1]

        if hours > 14400:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('8640H').rename("date").dt.strftime('%Y')])[['value_avg', 'a_avg']].mean().round(6).reset_index()
            data = data[['date', 'value_avg', 'a_avg']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "años"
        if hours > 3696 and hours <= 14400:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('720H').rename("date").dt.strftime('%Y-%m')])[['value_avg', 'a_avg']].mean().round(6).reset_index()
            data = data[['date', 'value_avg', 'a_avg']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "meses"
        if hours > 504 and hours <= 3696:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('168H').rename("date").dt.strftime('%Y-%m-%d')])[['value_avg', 'a_avg']].mean().round(6).reset_index()
            data = data[['date', 'value_avg', 'a_avg']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "semanas"
        if hours > 24 and hours <= 504:
            data = data.groupby(
                [pd.to_datetime(data['time']).dt.floor('24H').rename("date").dt.strftime('%Y-%m-%d')])[['value_avg', 'a_avg']].mean().round(6).reset_index()
            data = data[['date', 'value_avg', 'a_avg']]
            data.rename(columns={'date': 'time'}, inplace=True)
            data_range = "dias"

        tiempo = f"{len(data)} {data_range}"
        dias = round(hours / 24, 6)
        availability_avg = data.loc[:, 'a_avg'].mean()
        data.rename(columns={'a_avg': 'Alineacion',
                             'time': 'Tiempo',
                             'value_avg': 'num'}, inplace=True)
        response = {
            'data': data,
            'number': number,
            'dias': dias,
            'data_range': data_range,
            'tiempo': tiempo,
            'first': first,
            'last': last
        }
    else:
        response = {
            'data': data,
            'number': 0,
            'dias': 0,
            'data_range': 0,
            'tiempo': 0,
            'first': 0,
            'last': 0
        }
    return response


async def get_graphic_data(municipality_id: str, tech_id: str, brand_id: str, model_id: str, init_date: datetime, end_date: datetime):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    brand_id = ['' if brand == 0 else brand for brand in brand_id]
    model_id = ['' if model == 0 else model for model in model_id]

    brand_id = '' if '0' else brand_id
    model_id = '' if '0' else brand_id

    """ print(municipality_id) """

    statement = text(f"""
    call sp_connectivity('{municipality_id}','{tech_id}','{brand_id}','{model_id}','{init_date}','{end_date}');
    """)
    data = pd.DataFrame(session.execute(statement))
    """ print(statement) """
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
            """ print("aqui") """
        tiempo = f"{len(data)} {data_range}"
        dias = round(hours / 24, 6)
        availability_avg = data.loc[:, 'Avg_min'].mean()
        data.rename(columns={'Avg_min': metric_name,
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

    conectividad = list()
    processed_conectividades = list()
    catalog_municipios = text("call sp_catCity()")
    catalog_municipios = pd.DataFrame(
        session.execute(catalog_municipios)).replace(np.nan, "")
    catalog_techs = text("call sp_catDevice(0)")
    catalog_techs = pd.DataFrame(
        session.execute(catalog_techs)).replace(np.nan, "")
    catalog_brands = text("call sp_catBrand('')")
    catalog_brands = pd.DataFrame(
        session.execute(catalog_brands)).replace(np.nan, "")

    for ind in range(len(municipality_id)):
        statement = text(f"""
        call sp_connectivity('{municipality_id[ind]}','{tech_id[ind]}','{brand_id[ind]}','{model_id[ind]}','{init_date}','{end_date}');
        """)
        data = pd.DataFrame(session.execute(statement))
        data_insert = data
        processed_conectividad = data
        if not data.empty:
            data_insert = data[['itemid', 'time', 'Avg_min']]
            data_insert.rename(
                columns={'Avg_min': 'disponibildad', 'itemid': 'itemid(host)'}, inplace=True)
            processed_conectividad = process_data(data, init_date, end_date)
            processed_conectividad = processed_conectividad['data']
            model = 'Todos'
            if brand_id[ind] != '':
                catalog_models = text(f"call sp_catModel('{brand_id[ind]}')")
                catalog_models = pd.DataFrame(
                    session.execute(catalog_models)).replace(np.nan, "")
                model = get_model(
                    catalog_models, model_id[ind] if model_id[ind] != '' else 0)
            model = [
                model for i in range(len(processed_conectividad))]
            municipio = get_municipio(catalog_municipios, municipality_id[ind])
            municipio = [
                municipio for i in range(len(processed_conectividad))]
            tech = get_tech(catalog_techs, tech_id[ind])
            tech = [
                tech for i in range(len(processed_conectividad))]
            brand = get_brand(
                catalog_brands, brand_id[ind] if brand_id[ind] != '' else 0)
            brand = [
                brand for i in range(len(processed_conectividad))]

            processed_conectividad.insert(loc=0, column='modelo', value=model)
            processed_conectividad.insert(loc=0, column='marca', value=brand)
            processed_conectividad.insert(
                loc=0, column='tecnologia', value=tech)
            processed_conectividad.insert(
                loc=0, column='municipio', value=municipio)

        conectividad.append(data_insert)

        processed_conectividades.append(processed_conectividad)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
        xlsx_filename = temp_file.name
        with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:

            for ind in range(len(conectividad)):
                processed_conectividades[ind].to_excel(
                    writer, sheet_name=f"Consulta {ind+1} conectividad", index=False)
                conectividad[ind].to_excel(
                    writer, sheet_name=f"Consulta {ind+1} conectividad_data", index=False)

    session.close()
    return FileResponse(xlsx_filename, headers={"Content-Disposition": "attachment; filename=datos.xlsx"}, media_type="application/vnd.ms-excel", filename="datos.xlsx")


def verify_lenghts(arrays: list):
    inicial = arrays[0]
    for arreglo in arrays[1:]:
        if len(inicial) != len(arreglo):
            return False
    return True


def get_municipio(catalago, id):
    municipio = ""
    if int(id) == 0:
        municipio = "Todos"
    else:
        try:
            municipio = catalago.loc[catalago["groupid"] == int(id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Municipality id {id} is not a int value"
            )
        if len(municipio) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Municipality id not exist"
            )
        municipio = municipio['name'].values[0]
    return municipio


def get_tech(catalago, id):
    tech = ""
    if int(id) == 0:
        tech = "Todas"
    else:
        try:
            tech = catalago.loc[catalago["dispId"] == int(id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Tech id {id} is not a int value"
            )
        if len(tech) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tech id not exist"
            )
        tech = tech['name'].values[0]
    return tech


def get_brand(catalago, id):
    brand = ""
    if int(id) == 0:
        brand = "Todas"
    else:
        try:
            brand = catalago.loc[catalago["brand_id"] == int(id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Brand id {id} is not a int value"
            )
        if len(brand) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Brand id not exist"
            )
        brand = brand['name_brand'].values[0]
    return brand


def get_model(catalago, id):
    model = ""
    if int(id) == 0:
        model = "Todos"
    else:
        try:
            model = catalago.loc[catalago["model_id"] == int(id)]
        except:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Model id {id} is not a int value"
            )
        if len(model) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Model id not exist"
            )
        model = model['name_model'].values[0]
    return model


def procesar_vacio(vacio, mayor):

    vacio['Tiempo'] = [mayor['Tiempo'][ind] for ind in mayor.index]
    vacio['num'] = [0 for ind in range(len(mayor))]
    vacio['Disponibilidad'] = [0 for ind in range(len(mayor))]

    return vacio


def procesar_vacio_alineacion(vacio, mayor):
    vacio['Tiempo'] = [mayor['Tiempo'][ind] for ind in mayor.index]
    print(vacio, "Si es este")
    vacio['num'] = [0 for ind in range(len(mayor))]
    vacio['Alineacion'] = [0 for ind in range(len(mayor))]

    return vacio


def procesar_al():
    pass
    datas = list()
    dispositivos = list()
    dias = list()
    data_range = list()
    tiempo = list()
    first = list()
    last = list()
    vacios = list()
    no_vacios = list()
    for ind in range(len(municipality_id)):
        print(municipality_id)
        statement = text(f"""
        call sp_alignmentReport('{municipality_id[ind]}','{tech_id[ind]}','{brand_id[ind]}','{model_id[ind]}','{init_date}','{end_date}');
        """)
        data = pd.DataFrame(session.execute(statement))

        data_procesed = process_data_alignment(data, end_date, init_date)
        """ datas.append(data_procesed['data']) """
        datas.append({'index': ind+1, 'data': data_procesed['data']})
        dispositivos.append(data_procesed['number'])
        dias.append(data_procesed['dias'])
        data_range.append(data_procesed['data_range'])
        tiempo.append(data_procesed['tiempo'])
        first.append(data_procesed['first'])
        last.append(data_procesed['last'])

    merged_df = pd.DataFrame()
    promedios = list()
    vacios = list(filter(lambda x: len(x['data']) <= 0, datas))
    no_vacios = list(filter(lambda x: len(x['data']) > 0, datas))
    """ print(vacios)
    print(no_vacios) """
    if len(vacios) > 0 and len(no_vacios) == 0:
        for vacio in vacios:
            vacio['data']['Alineacion'] = [0, 0]
            vacio['data']['Tiempo'] = [init_date, end_date]
            vacio['data']['num'] = [0, 0]

        merged_df = vacios[0]['data']
        ind1 = vacios[0]['index']
        promedios.append(merged_df.loc[:, 'Alineacion'].mean())
        if len(vacios) <= 1:
            merged_df.rename(
                columns={'Alineacion': 'Alineacion_1', 'num': 'num_1'}, inplace=True)

        for df in vacios[1:]:
            ind2 = df['index']
            print(ind1, ind2)
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])
            promedios.append(df['data'].loc[:, 'Alineacion'].mean())
            ind1 = ind2
        metrics.append({'metric_name': "Alineacion",
                        'availability_average': [0 for vacios in range(len(vacios))],
                        'days': [0 for vacios in range(len(vacios))],
                        'device_count': [0 for vacios in range(len(vacios))],
                        'data_range': [0 for vacios in range(len(vacios))],
                        'time': [0 for vacios in range(len(vacios))],
                        'first_data': [0 for vacios in range(len(vacios))],
                        'last_data': [0 for vacios in range(len(vacios))],
                        'dataset': merged_df.to_dict(orient="records")
                        })

        """ response = {

            'general_funcionality_average': promedios,
            'metrics': metrics
        }
        session.close()
        return success_response(data=response) """
    if len(no_vacios) > 0 and len(vacios) > 0:
        """ print(no_vacios) """
        no_vacios = sorted(no_vacios, key=lambda x: len(x['data']))
        """ print(no_vacios) """

        mayor = no_vacios[0]['data']

        for vacio in vacios:
            vacio['data'] = procesar_vacio_alineacion(vacio['data'], mayor)
        """ print(no_vacios) """
        merged_df = no_vacios[0]['data']
        """ ind = 1 """
        ind1 = no_vacios[0]['index']
        promedios.append(
            {'index': ind1, 'data': merged_df.loc[:, 'Alineacion'].mean()})
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Alineacion': f'Alineacion_{ind2}'})
            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Alineacion'].mean()})
            ind1 = ind2
        for df in vacios:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Alineacion': f'Alineacion_{ind2}'})
            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Alineacion'].mean()})
            ind1 = ind2
        if len(vacios)+len(no_vacios) <= 1:
            merged_df.rename(
                columns={'Alineacion': 'Alineacion_1', 'num': 'num_1'}, inplace=True)
        promedios = sorted(promedios, key=lambda l: l['index'])

        promedios = [promedio['data'] for promedio in promedios]
        metrics.append({'metric_name': "Alineacion",
                        'availability_average': promedios,
                        'days': dias,
                        'device_count': dispositivos,
                        'data_range': data_range,
                        'time': tiempo,
                        'first_data': first,
                        'last_data': last,
                        'dataset': merged_df.to_dict(orient="records")
                        })

    if len(no_vacios) > 0 and len(vacios) == 0:
        no_vacios = sorted(no_vacios, key=lambda x: len(x['data']))

        mayor = no_vacios[0]['data']
        merged_df = no_vacios[0]['data']
        ind1 = no_vacios[0]['index']

        promedios.append(
            {'index': ind1, 'data': merged_df.loc[:, 'Alineacion'].mean()})
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='inner', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Alineacion': f'Alineacion_{ind2}'})

            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Alineacion'].mean()})
            ind1 = ind2
        if len(no_vacios) <= 1:
            merged_df.rename(
                columns={'Alineacion': 'Alineacion_1', 'num': 'num_1'}, inplace=True)
        promedios = sorted(promedios, key=lambda l: l['index'])

        promedios = [promedio['data'] for promedio in promedios]

        metrics.append({'metric_name': "Alineacion",
                        'availability_average': promedios,
                        'days': dias,
                        'device_count': dispositivos,
                        'data_range': data_range,
                        'time': tiempo,
                        'first_data': first,
                        'last_data': last,
                        'dataset': merged_df.to_dict(orient="records")
                        })


def process_data_conectivity(municipality_id, tech_id, brand_id, model_id, init_date, end_date, session, proms):
    datas = list()
    dispositivos = list()
    dias = list()
    data_range = list()
    tiempo = list()
    first = list()
    last = list()
    for ind in range(len(municipality_id)):
        print(municipality_id)
        statement = text(f"""
        call sp_connectivity('{municipality_id[ind]}','{tech_id[ind]}','{brand_id[ind]}','{model_id[ind]}','{init_date}','{end_date}');
        """)
        data = pd.DataFrame(session.execute(statement))

        data_procesed = process_data(
            data, end_date, init_date, "Disponibilidad")
        """ datas.append(data_procesed['data']) """
        datas.append({'index': ind+1, 'data': data_procesed['data']})
        dispositivos.append(data_procesed['number'])
        dias.append(data_procesed['dias'])
        data_range.append(data_procesed['data_range'])
        tiempo.append(data_procesed['tiempo'])
        first.append(data_procesed['first'])
        last.append(data_procesed['last'])

    merged_df = pd.DataFrame()
    promedios = list()
    vacios = list(filter(lambda x: len(x['data']) <= 0, datas))
    no_vacios = list(filter(lambda x: len(x['data']) > 0, datas))
    """ print(vacios)
    print(no_vacios) """
    if len(vacios) > 0 and len(no_vacios) == 0:
        for vacio in vacios:
            vacio['data']['Disponibilidad'] = [0, 0]
            vacio['data']['Tiempo'] = [init_date, end_date]
            vacio['data']['num'] = [0, 0]

        merged_df = vacios[0]['data']
        ind1 = vacios[0]['index']
        promedios.append(merged_df.loc[:, 'Disponibilidad'].mean())
        proms.append({'index': ind1, 'data': [
                     merged_df.loc[:, 'Disponibilidad'].mean()]})
        if len(vacios) <= 1:
            merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True)

        for df in vacios[1:]:
            ind2 = df['index']
            """ print(ind1, ind2) """
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])
            promedios.append(df['data'].loc[:, 'Disponibilidad'].mean())
            proms.append(
                {'index': df['index'], 'data': [df['data'].loc[:, 'Disponibilidad'].mean()]})
            ind1 = ind2

        metrics = {'metric_name': "Conectividad",
                   'availability_average': [0 for vacios in range(len(vacios))],
                   'days': [0 for vacios in range(len(vacios))],
                   'device_count': [0 for vacios in range(len(vacios))],
                   'data_range': [0 for vacios in range(len(vacios))],
                   'time': [0 for vacios in range(len(vacios))],
                   'first_data': [0 for vacios in range(len(vacios))],
                   'last_data': [0 for vacios in range(len(vacios))],
                   'dataset': merged_df.to_dict(orient="records")
                   }

    if len(no_vacios) > 0 and len(vacios) > 0:
        """ print(no_vacios) """
        no_vacios = sorted(
            no_vacios, key=lambda x: len(x['data']), reverse=True)
        """ print(no_vacios) """

        mayor = no_vacios[0]['data']

        for vacio in vacios:
            vacio['data'] = procesar_vacio(vacio['data'], mayor)
        """ print(no_vacios) """
        merged_df = no_vacios[0]['data']
        """ ind = 1 """
        ind1 = no_vacios[0]['index']
        promedios.append(
            {'index': ind1, 'data': merged_df.loc[:, 'Disponibilidad'].mean()})
        proms.append({'index': ind1, 'data:': [
                     merged_df.loc[:, 'Disponibilidad'].mean()]})
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}).replace(np.nan, 0)
            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Disponibilidad'].mean()})
            proms.append(
                {'index': df['index'], 'data': [df['data'].loc[:, 'Disponibilidad'].mean()]})
            ind1 = ind2
        for df in vacios:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='inner', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'})
            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Disponibilidad'].mean()})
            proms.append(
                {'index': df['index'], 'data': [df['data'].loc[:, 'Disponibilidad'].mean()]})
            ind1 = ind2
        if len(vacios)+len(no_vacios) <= 1:
            merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True)

        promedios = sorted(promedios, key=lambda l: l['index'])

        promedios = [promedio['data'] for promedio in promedios]
        metrics = {'metric_name': "Conectividad",
                   'availability_average': promedios,
                   'days': dias,
                   'device_count': dispositivos,
                   'data_range': data_range,
                   'time': tiempo,
                   'first_data': first,
                   'last_data': last,
                   'dataset': merged_df.to_dict(orient="records")
                   }

    if len(no_vacios) > 0 and len(vacios) == 0:
        """ print(no_vacios) """
        no_vacios = sorted(
            no_vacios, key=lambda x: len(x['data']), reverse=True)

        mayor = no_vacios[0]['data']
        """  print(no_vacios) """
        merged_df = no_vacios[0]['data']
        ind1 = no_vacios[0]['index']

        promedios.append(
            {'index': ind1, 'data': merged_df.loc[:, 'Disponibilidad'].mean()})
        proms.append({'index': ind1, 'data': [
                     merged_df.loc[:, 'Disponibilidad'].mean()]})
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}).replace(np.nan, 0)

            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Disponibilidad'].mean()})
            proms.append(
                {'index': df['index'], 'data': [df['data'].loc[:, 'Disponibilidad'].mean()]})
            ind1 = ind2
        if len(no_vacios) <= 1:
            merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True)
        promedios = sorted(promedios, key=lambda l: l['index'])

        promedios = [promedio['data'] for promedio in promedios]

        metrics = {'metric_name': "Conectividad",
                   'availability_average': promedios,
                   'days': dias,
                   'device_count': dispositivos,
                   'data_range': data_range,
                   'time': tiempo,
                   'first_data': first,
                   'last_data': last,
                   'dataset': merged_df.to_dict(orient="records")
                   }

    return metrics


def get_index(lst, key, value):
    return next((index for (index, d) in enumerate(lst) if d[key] == value), None)


def process_data_alignment(municipality_id, tech_id, brand_id, model_id, init_date, end_date, session, metricas, proms):
    alineacion_id = text(
        """select group_id from metric_group mg where nickname = 'Alineación'""")
    alineacion_id = pd.DataFrame(session.execute(alineacion_id))
    if alineacion_id.empty:
        return
    alineacion_id = alineacion_id['group_id'].values[0]

    datas = list()
    dispositivos = list()
    dias = list()
    data_range = list()
    tiempo = list()
    first = list()
    last = list()
    vacios = list()
    no_vacios = list()
    for ind in range(len(municipality_id)):
        pertenece = text(
            f"""select * from metrics_template mt where device_id ='{tech_id[ind]}' and group_id ='{alineacion_id}'""")
        pertenece = pd.DataFrame(session.execute(pertenece))
        if pertenece.empty:
            continue
        print("indice alineacion", ind)
        print(municipality_id)
        statement = text(f"""
        call sp_alignmentReport('{municipality_id[ind]}','{tech_id[ind]}','{brand_id[ind]}','{model_id[ind]}','{init_date}','{end_date}');
        """)
        data = pd.DataFrame(session.execute(statement))

        data_procesed = process_data(data, end_date, init_date, "Alineacion")
        """ datas.append(data_procesed['data']) """
        datas.append({'index': ind+1, 'data': data_procesed['data']})
        dispositivos.append(data_procesed['number'])
        dias.append(data_procesed['dias'])
        data_range.append(data_procesed['data_range'])
        tiempo.append(data_procesed['tiempo'])
        first.append(data_procesed['first'])
        last.append(data_procesed['last'])

    merged_df = pd.DataFrame()
    promedios = list()
    vacios = list(filter(lambda x: len(x['data']) <= 0, datas))
    no_vacios = list(filter(lambda x: len(x['data']) > 0, datas))
    """ print(vacios)
    print(no_vacios) """
    if len(vacios) > 0 and len(no_vacios) == 0:
        for vacio in vacios:
            vacio['data']['Alineacion'] = [0, 0]
            vacio['data']['Tiempo'] = [init_date, end_date]
            vacio['data']['num'] = [0, 0]

        merged_df = vacios[0]['data']
        ind1 = vacios[0]['index']
        promedios.append(merged_df.loc[:, 'Alineacion'].mean())
        proms_index = get_index(proms, 'index', ind1)
        proms[proms_index]['data'].append(
            merged_df.loc[:, 'Alineacion'].mean())
        if len(vacios) <= 1:
            merged_df.rename(
                columns={'Alineacion': 'Alineacion_1', 'num': 'num_1'}, inplace=True)

        for df in vacios[1:]:
            ind2 = df['index']
            print(ind1, ind2)
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])
            promedios.append(df['data'].loc[:, 'Alineacion'].mean())
            proms_index = get_index(proms, 'index', df['index'])
            proms[proms_index]['data'].append(
                df['data'].loc[:, 'Alineacion'].mean())
            ind1 = ind2
        metricas.append({'metric_name': "Alineacion",
                        'availability_average': [0 for vacios in range(len(vacios))],
                         'days': [0 for vacios in range(len(vacios))],
                         'device_count': [0 for vacios in range(len(vacios))],
                         'data_range': [0 for vacios in range(len(vacios))],
                         'time': [0 for vacios in range(len(vacios))],
                         'first_data': [0 for vacios in range(len(vacios))],
                         'last_data': [0 for vacios in range(len(vacios))],
                         'dataset': merged_df.to_dict(orient="records")
                         })

        """ response = {

            'general_funcionality_average': promedios,
            'metrics': metrics
        }
        session.close()
        return success_response(data=response) """
    if len(no_vacios) > 0 and len(vacios) > 0:
        """ print(no_vacios) """
        no_vacios = sorted(no_vacios, key=lambda x: len(x['data']))
        """ print(no_vacios) """

        mayor = no_vacios[0]['data']

        for vacio in vacios:
            vacio['data'] = procesar_vacio_alineacion(vacio['data'], mayor)
        """ print(no_vacios) """
        merged_df = no_vacios[0]['data']
        """ ind = 1 """
        ind1 = no_vacios[0]['index']
        promedios.append(
            {'index': ind1, 'data': merged_df.loc[:, 'Alineacion'].mean()})
        proms_index = get_index(proms, 'index', ind1)
        proms[proms_index]['data'].append(
            merged_df.loc[:, 'Alineacion'].mean())
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Alineacion': f'Alineacion_{ind2}'})
            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Alineacion'].mean()})
            proms_index = get_index(proms, 'index', df['index'])
            proms[proms_index]['data'].append(
                df['data'].loc[:, 'Alineacion'].mean())
            ind1 = ind2
        for df in vacios:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Alineacion': f'Alineacion_{ind2}'})
            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Alineacion'].mean()})
            proms_index = get_index(proms, 'index', df['index'])
            proms[proms_index]['data'].append(
                df['data'].loc[:, 'Alineacion'].mean())
            ind1 = ind2
        if len(vacios)+len(no_vacios) <= 1:
            merged_df.rename(
                columns={'Alineacion': 'Alineacion_1', 'num': 'num_1'}, inplace=True)
        promedios = sorted(promedios, key=lambda l: l['index'])

        promedios = [promedio['data'] for promedio in promedios]
        metricas.append({'metric_name': "Alineacion",
                        'availability_average': promedios,
                         'days': dias,
                         'device_count': dispositivos,
                         'data_range': data_range,
                         'time': tiempo,
                         'first_data': first,
                         'last_data': last,
                         'dataset': merged_df.to_dict(orient="records")
                         })

    if len(no_vacios) > 0 and len(vacios) == 0:
        no_vacios = sorted(no_vacios, key=lambda x: len(x['data']))

        mayor = no_vacios[0]['data']
        merged_df = no_vacios[0]['data']
        ind1 = no_vacios[0]['index']

        promedios.append(
            {'index': ind1, 'data': merged_df.loc[:, 'Alineacion'].mean()})
        proms_index = get_index(proms, 'index', ind1)
        proms[proms_index]['data'].append(
            merged_df.loc[:, 'Alineacion'].mean())
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='inner', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Alineacion': f'Alineacion_{ind2}'})

            promedios.append(
                {'index': ind2, 'data': df['data'].loc[:, 'Alineacion'].mean()})
            proms_index = get_index(proms, 'index', df['index'])
            proms[proms_index]['data'].append(
                df['data'].loc[:, 'Alineacion'].mean())
            ind1 = ind2
        if len(no_vacios) <= 1:
            merged_df.rename(
                columns={'Alineacion': 'Alineacion_1', 'num': 'num_1'}, inplace=True)
        promedios = sorted(promedios, key=lambda l: l['index'])

        promedios = [promedio['data'] for promedio in promedios]

        metricas.append({'metric_name': "Alineacion",
                        'availability_average': promedios,
                         'days': dias,
                         'device_count': dispositivos,
                         'data_range': data_range,
                         'time': tiempo,
                         'first_data': first,
                         'last_data': last,
                         'dataset': merged_df.to_dict(orient="records")
                         })


def process_metrics(data):
    promedios = list()
    for dat in data:
        datas = sorted(dat['data'])
        print(datas)
        if len(datas) > 1:
            promedios.append((datas[0]*datas[len(datas)-1])/100)
        else:
            promedios.append((datas[0]))
    return promedios
