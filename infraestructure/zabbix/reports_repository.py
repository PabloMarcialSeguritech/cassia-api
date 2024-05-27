import infraestructure.database_model as db
from fastapi.exceptions import HTTPException
from fastapi import status
import infraestructure.db_queries_model as db_queries_model
import pandas as pd
import numpy as np


async def get_connectivity_data_m(tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_connectivity_data_m,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_connectivity_data(municipality_id, tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipality_id}', f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_connectivity_data,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_alineacion_group_id():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(db_queries.query_statement_get_alineacion_group_id)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_aligment_report_m(tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_aligment_report_data_m,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_metrics_template(tech_id, alineacion_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()

    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(
            db_queries.builder_query_statement_get_metrics_template(tech_id, alineacion_id))
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_aligment_report(municipality_id, tech_id, brand_id, model_id, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{municipality_id}', f'{tech_id}', f'{brand_id}', f'{model_id}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_aligment_report_data,
                                                                     stored_procedure_params)
        return database_response

    except Exception as e:

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The lenght of the arrays must be the same"
        )
    finally:
        await db_connection.close_connection()


async def get_connectivity_by_device(hostid, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{hostid}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_connectivity_data_by_device,
                                                                     stored_procedure_params)
        data_df = pd.DataFrame(database_response)
        if data_df.empty:
            data_df = pd.DataFrame(
                columns=['templateid', 'itemid', 'time', 'num', 'Avg_min'])
        return data_df

    except Exception as e:
        print(f"Excepcion en get_connectivity_by_device {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_connectivity_by_device {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_device_id_alineacion():
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(db_queries.query_statement_get_device_alineacion)
        data_df = pd.DataFrame(database_response)
        return data_df
    except Exception as e:
        print(f"Excepcion en get_device_id_alineacion {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_device_id_alineacion {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_pertenencia_dispositivo_metric(hostid, metric_id):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_query(db_queries.builder_query_statement_get_pertenencia_host_metric(hostid, metric_id))
        data_df = pd.DataFrame(database_response)
        return data_df
    except Exception as e:
        print(f"Excepcion en get_pertenencia_dispositivo_metric {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_pertenencia_dispositivo_metric {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_alignment_by_device(hostid, init_date, end_date):
    db_connection = db.DB()
    db_queries = db_queries_model.DBQueries()
    stored_procedure_params = (
        f'{hostid}', f'{init_date}', f'{end_date}',)
    try:
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(db_queries.stored_name_get_aligment_report_data_by_device,
                                                                     stored_procedure_params)
        data_df = pd.DataFrame(database_response)
        return data_df

    except Exception as e:
        print(f"Excepcion en get_alignment_by_device {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_alignment_by_device {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_device_type_catalog():
    db_connection = db.DB()
    try:
        sp_name_catalog_devices = db_queries_model.DBQueries().stored_name_catalog_devices_types
        await db_connection.start_connection()

        database_response = await db_connection.run_stored_procedure(sp_name_catalog_devices,
                                                                     (0,))
        data_df = pd.DataFrame(database_response)
        return data_df

    except Exception as e:
        print(f"Excepcion en get_device_type_catalog {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_device_type_catalog {e}"
        )
    finally:
        await db_connection.close_connection()


async def get_device_brand_catalog():
    db_connection = db.DB()
    try:
        sp_name_catalog_devices_brand = db_queries_model.DBQueries(
        ).stored_name_catalog_devices_brands
        await db_connection.start_connection()
        database_response = await db_connection.run_stored_procedure(sp_name_catalog_devices_brand,
                                                                     (''))
        data_df = pd.DataFrame(database_response)
        return data_df

    except Exception as e:
        print(f"Excepcion en get_device_type_catalog {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion en get_device_type_catalog {e}"
        )
    finally:
        await db_connection.close_connection()


async def process_data_async(data, end_date, init_date, metric_name):
    if not data.empty:
        number = data['itemid'].nunique()
        data = data.groupby(['time']).agg(
            {'Avg_min': 'mean', 'num': 'mean'}).reset_index()
        data['Avg_min'] = data['Avg_min'].apply(lambda x: x*100)
        data = data[['time', 'num', 'Avg_min']]
        diff = end_date-init_date
        hours = diff.days*24 + diff.seconds//3600
        data_range = "horas"
        first = data['time'][0]
        last = data['time'][len(data)-1]

        if hours > 14400:
            data = await process_dates(data, init_date, end_date, '8640H', '%Y')
            data_range = "años"
        if hours >= 7200 and hours <= 14400:
            data = await process_dates(data, init_date, end_date, '720H', '%Y-%m-%d')
            data_range = "meses"
        if hours > 3696 and hours < 7200:
            data = await process_dates(data, init_date, end_date, '360H', '%Y-%m-%d')
            data_range = "quincenas"

        if hours > 1680 and hours <= 3696:
            data = await process_dates(data, init_date, end_date, '168H', '%Y-%m-%d')
            data_range = "semanas"

        if hours > 120 and hours <= 1680:
            data = await process_dates(data, init_date, end_date, '24H', '%Y-%m-%d')
            data_range = "dias"

        """ if hours > 120 and hours <= 240:
            data = await process_dates(data, init_date, end_date, '12H', '%Y-%m-%d %H:%M:%S')
            data_range = "medios dias" """
        if hours > 3 and hours <= 120:
            data = await process_dates(data, init_date, end_date, '1H', '%Y-%m-%d %H:%M:%S')
            data_range = "HORAS"
        if hours >= 1 and hours <= 3:
            data = await process_dates(data, init_date, end_date, '15min', '%Y-%m-%d %H:%M:%S')
            data_range = "minutos"

        if hours >= 0 and hours <= 1:
            data = await process_dates(data, init_date, end_date, '1min', '%Y-%m-%d %H:%M:%S')
            data_range = "minutos"

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
        data = pd.DataFrame(
            columns=['templateid', 'itemid', 'time', 'num', 'Avg_min'])
        data.rename(columns={'Avg_min': metric_name,
                             'time': 'Tiempo'}, inplace=True)
        data = data.replace(np.nan, 0)
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


async def process_data_async_alineacion(data, end_date, init_date, metric_name):
    if not data.empty:
        number = data['itemid'].nunique()
        data = data.groupby(['time']).agg(
            {'Avg_min': 'mean', 'num': 'mean'}).reset_index()
        data['Avg_min'] = data['Avg_min'].apply(lambda x: x*100)
        data = data[['time', 'num', 'Avg_min']]
        diff = end_date-init_date
        hours = diff.days*24 + diff.seconds//3600
        data_range = "horas"
        first = data['time'][0]
        last = data['time'][len(data)-1]

        if hours > 14400:
            data = await process_dates(data, init_date, end_date, '8640H', '%Y')
            data_range = "años"
        if hours >= 7200 and hours <= 14400:
            data = await process_dates(data, init_date, end_date, '720H', '%Y-%m-%d')
            data_range = "meses"
        if hours > 3696 and hours < 7200:
            data = await process_dates(data, init_date, end_date, '360H', '%Y-%m-%d')
            data_range = "quincenas"

        if hours > 1680 and hours <= 3696:
            data = await process_dates(data, init_date, end_date, '168H', '%Y-%m-%d')
            data_range = "semanas"

        if hours > 120 and hours <= 1680:
            data = await process_dates(data, init_date, end_date, '24H', '%Y-%m-%d')
            data_range = "dias"

        """ if hours > 120 and hours <= 240:
            data = await process_dates(data, init_date, end_date, '12H', '%Y-%m-%d %H:%M:%S')
            data_range = "medios dias" """
        if hours >= 0 and hours <= 120:
            data = await process_dates(data, init_date, end_date, '1H', '%Y-%m-%d %H:%M:%S')
            data_range = "HORAS"
        """ if hours >= 1 and hours <= 3:
            data = await process_dates(data, init_date, end_date, '15min', '%Y-%m-%d %H:%M:%S')
            data_range = "minutos"

        if hours >= 0 and hours <= 1:
            data = await process_dates(data, init_date, end_date, '1min', '%Y-%m-%d %H:%M:%S')
            data_range = "minutos" """

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
        data = pd.DataFrame(
            columns=['templateid', 'itemid', 'time', 'num', 'Avg_min'])
        data.rename(columns={'Avg_min': metric_name,
                             'time': 'Tiempo'}, inplace=True)
        data = data.replace(np.nan, 0)
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


def find_nearest_date(date, reference_dates):
    return min(reference_dates, key=lambda x: abs(x - date))


async def process_conectvidad(vacios, no_vacios, init_date, end_date, proms, general, promedios_general, municipios, hostids, dias, dispositivos, data_range, tiempo, first, last, promedios):

    if len(vacios) > 0 and len(no_vacios) == 0:
        for vacio in vacios:
            vacio['data'][f"{vacio['hostid']}"] = [0, 0]
            vacio['data']["templateid"] = [0, 0]
            vacio['data']["itemid"] = [0, 0]
            vacio['data']['Tiempo'] = [init_date, end_date]
            vacio['data']['num'] = [0, 0]

        merged_df = vacios[0]['data']
        ind1 = vacios[0]['index']

        promedios.append(
            {'index': ind1, 'data': merged_df.iloc[:, [4]].mean().values[0].round(2)})

        proms.append({'index': ind1, 'data': [
            merged_df.iloc[:, [4]].mean().values[0].round(2)]})
        """ if len(vacios) <= 1:
            merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True) """

        for df in vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])

            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [4]].mean().values[0].round(2)})
            proms.append(
                {'index': df['index'], 'data': [df['data'].iloc[:, [4]].mean().values[0].round(2)]})
            ind1 = ind2

        promedios = sorted(promedios, key=lambda l: l['index'])
        promedios = promedios[1:]
        indices = [promedio['index'] for promedio in promedios]
        promedios = [promedio['data'] for promedio in promedios]
        merged_df = merged_df.replace(np.nan, 0).drop(
            columns=['Avg_min', 'num_0'])
        metrics = {'metric_name': "Conectividad",
                   'indices': indices,
                   'availability_average': [0 for vacios in range(len(vacios))],
                   'days': [0 for vacios in range(len(vacios))],
                   'device_count': [0 for vacios in range(len(vacios))],
                   'data_range': [0 for vacios in range(len(vacios))],
                   'time': [0 for vacios in range(len(vacios))],
                   'first_data': [0 for vacios in range(len(vacios))],
                   'last_data': [0 for vacios in range(len(vacios))],
                   'dataset': merged_df.to_dict(orient="records"),
                   'dataset2': general.to_dict(orient='records'),
                   'availavility_average2': promedios_general,
                   'municipality': municipios,
                   'hostids': hostids}

    if len(no_vacios) > 0 and len(vacios) > 0:

        no_vacios = sorted(
            no_vacios, key=lambda x: len(x['data']), reverse=True)
        mayor = no_vacios[0]['data']

        for vacio in vacios:
            vacio['data'] = await procesar_vacio_device(
                vacio['data'], mayor, vacio['hostid'])
        merged_df = no_vacios[0]['data']
        print(merged_df)
        """ ind = 1 """
        ind1 = no_vacios[0]['index']
        promedios.append(
            {'index': ind1, 'data': merged_df.iloc[:, [2]].mean().values[0].round(2)})
        proms.append({'index': ind1, 'data': [
            merged_df.iloc[:, [2]].mean().values[0].round(2)]})
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).replace(np.nan, 0)
            """ merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}).replace(np.nan, 0)
             """
            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [2]].mean().values[0].round(2)})
            proms.append(
                {'index': df['index'], 'data': [df['data'].iloc[:, [2]].mean().values[0].round(2)]})
            ind1 = ind2
        for df in vacios:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])
            """ merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='inner', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}) """
            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [4]].mean().values[0].round(2)})
            proms.append(
                {'index': df['index'], 'data': [df['data'].iloc[:, [4]].mean().values[0].round(2)]})
            ind1 = ind2
        if len(vacios)+len(no_vacios) <= 1:
            """ merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True) """
        """ print("qaui") """
        promedios = sorted(promedios, key=lambda l: l['index'])
        promedios = promedios[1:]
        indices = [promedio['index'] for promedio in promedios]
        promedios = [promedio['data'] for promedio in promedios]
        merged_df = merged_df.replace(np.nan, 0).drop(
            columns=['Avg_min', 'num_0'])
        """ print(promedios)

        print(indices) """
        metrics = {'metric_name': "Conectividad",
                   'indices': indices,
                   'availability_average': promedios,
                   'days': dias,
                   'device_count': dispositivos,
                   'data_range': data_range,
                   'time': tiempo,
                   'first_data': first,
                   'last_data': last,
                   'dataset': merged_df.to_dict(orient="records"),
                   'dataset2': general.to_dict(orient='records'),
                   'availavility_average2': promedios_general,
                   'municipality': municipios,
                   'hostids': hostids,
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
            {'index': ind1, 'data': merged_df.iloc[:, [2]].mean().values[0].round(2)})
        proms.append({'index': ind1, 'data': [
            merged_df.iloc[:, [2]].mean().values[0].round(2)]})
        for df in no_vacios[1:]:
            ind2 = df['index']

            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).replace(np.nan, 0)
            """ merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}).replace(np.nan, 0) """

            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [2]].mean().values[0].round(2)})
            proms.append(
                {'index': df['index'], 'data': [df['data'].iloc[:, [2]].mean().values[0].round(2)]})
            ind1 = ind2
        if len(no_vacios) <= 1:
            """ merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True) """
        """ print(proms) """
        promedios = sorted(promedios, key=lambda l: l['index'])
        promedios = promedios[1:]
        indices = [promedio['index'] for promedio in promedios]
        promedios = [promedio['data'] for promedio in promedios]
        merged_df = merged_df.replace(np.nan, 0).drop(
            columns=['Avg_min', 'num_0'])
        metrics = {'metric_name': "Conectividad",
                   'indices': indices,
                   'availability_average': promedios,
                   'days': dias,
                   'device_count': dispositivos,
                   'data_range': data_range,
                   'time': tiempo,
                   'first_data': first,
                   'last_data': last,
                   'dataset': merged_df.to_dict(orient="records"),
                   'dataset2': general.to_dict(orient='records'),
                   'availavility_average2': promedios_general,
                   'municipality': municipios,
                   'hostids': hostids,
                   }
    return metrics


async def procesar_vacio_device(vacio, mayor, hostid):

    vacio['Tiempo'] = [mayor['Tiempo'][ind] for ind in mayor.index]
    vacio['num'] = [0 for ind in range(len(mayor))]
    vacio[f'Disponibilidad_{hostid}'] = [0 for ind in range(len(mayor))]
    vacio = vacio.replace(np.nan, 0)

    return vacio


async def process_alineacion(vacios, no_vacios, init_date, end_date, proms, general, promedios_general, municipios, hostids, dias, dispositivos, data_range, tiempo, first, last, metricas, promedios):
    if len(vacios) > 0 and len(no_vacios) == 0:

        for vacio in vacios:
            vacio['data'][f"{vacio['hostid']}"] = [0, 0]
            vacio['data']["templateid"] = [0, 0]
            vacio['data']["itemid"] = [0, 0]
            vacio['data']['Tiempo'] = [init_date, end_date]
            vacio['data']['num'] = [0, 0]

        merged_df = vacios[0]['data']

        ind1 = vacios[0]['index']
        promedios.append(
            {'index': ind1, 'data': merged_df.iloc[:, [4]].mean().values[0].round(2)})
        proms.append({'index': ind1, 'data': [
            merged_df.iloc[:, [4]].mean().values[0].round(2)]})
        """ if len(vacios) <= 1:
            merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True) """

        for df in vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])

            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [4]].mean().values[0].round(2)})
            proms.append(
                {'index': df['index'], 'data': [df['data'].iloc[:, [4]].mean().values[0].round(2)]})
            ind1 = ind2

        promedios = sorted(promedios, key=lambda l: l['index'])
        promedios = promedios[1:]
        indices = [promedio['index'] for promedio in promedios]
        promedios = [promedio['data'] for promedio in promedios]
        merged_df = merged_df.replace(np.nan, 0).drop(
            columns=['Avg_min', 'num_0'])

        metricas.append({'metric_name': "Alineacion",
                         'indices': indices,
                        'availability_average': [0 for vacios in range(len(vacios))],
                         'days': [0 for vacios in range(len(vacios))],
                         'device_count': [0 for vacios in range(len(vacios))],
                         'data_range': [0 for vacios in range(len(vacios))],
                         'time': [0 for vacios in range(len(vacios))],
                         'first_data': [0 for vacios in range(len(vacios))],
                         'last_data': [0 for vacios in range(len(vacios))],
                         'dataset': merged_df.to_dict(orient="records"),
                         'dataset2': general.to_dict(orient='records'),
                         'availavility_average2': promedios_general,
                         'municipality': municipios,
                         'hostids': hostids
                         })

    if len(no_vacios) > 0 and len(vacios) > 0:

        no_vacios = sorted(
            no_vacios, key=lambda x: len(x['data']), reverse=True)

        mayor = no_vacios[0]['data']

        for vacio in vacios:
            vacio['data'] = await procesar_vacio_alineacion_device(
                vacio['data'], mayor, vacio['hostid'])

        merged_df = no_vacios[0]['data']

        ind1 = no_vacios[0]['index']
        promedios.append(
            {'index': ind1, 'data': merged_df.iloc[:, [2]].mean().values[0].round(2)})
        proms_index = await get_index(proms, 'index', ind1)
        proms[proms_index]['data'].append(
            merged_df.iloc[:, [2]].mean().values[0].round(2))
        for df in no_vacios[1:]:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])
            """ merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}) """
            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [2]].mean().values[0].round(2)})
            proms_index = await get_index(proms, 'index', df['index'])
            proms[proms_index]['data'].append(
                df['data'].iloc[:, [2]].mean().values[0].round(2))
            ind1 = ind2
        for df in vacios:
            ind2 = df['index']
            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])
            """ merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}) """
            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [4]].mean().values[0].round(2)})
            proms_index = await get_index(proms, 'index', df['index'])
            proms[proms_index]['data'].append(
                df['data'].iloc[:, [4]].mean().values[0].round(2))
            ind1 = ind2
        if len(vacios)+len(no_vacios) <= 1:
            """ merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True) """
        promedios = sorted(promedios, key=lambda l: l['index'])
        promedios = promedios[1:]
        indices = [promedio['index'] for promedio in promedios]
        promedios = [promedio['data'] for promedio in promedios]
        merged_df = merged_df.replace(np.nan, 0).drop(
            columns=['Avg_min', 'num_0'])
        metricas.append({'metric_name': "Alineacion",
                         'indices': indices,
                        'availability_average': promedios,
                         'days': dias,
                         'device_count': dispositivos,
                         'data_range': data_range,
                         'time': tiempo,
                         'first_data': first,
                         'last_data': last,
                         'dataset': merged_df.to_dict(orient="records"),
                         'dataset2': general.to_dict(orient='records'),
                         'availavility_average2': promedios_general,
                         'municipality': municipios,
                         'hostids': hostids
                         })

    if len(no_vacios) > 0 and len(vacios) == 0:

        no_vacios = sorted(
            no_vacios, key=lambda x: len(x['data']), reverse=True)

        mayor = no_vacios[0]['data']

        merged_df = no_vacios[0]['data']
        ind1 = no_vacios[0]['index']

        promedios.append(
            {'index': ind1, 'data': merged_df.iloc[:, [2]].mean().values[0].round(2)})
        proms_index = await get_index(proms, 'index', ind1)

        proms[proms_index]['data'].append(
            merged_df.iloc[:, [2]].mean().values[0].round(2))
        for df in no_vacios[1:]:

            ind2 = df['index']

            merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='left', suffixes=[f'_{ind1}', f'_{ind2}'])
            """ merged_df = pd.merge(merged_df, df['data'], on='Tiempo',
                                 how='inner', suffixes=[f'_{ind1}', f'_{ind2}']).rename(columns={'Disponibilidad': f'Disponibilidad_{ind2}'}) """

            promedios.append(
                {'index': ind2, 'data': df['data'].iloc[:, [2]].mean().values[0].round(2)})
            proms_index = await get_index(proms, 'index', df['index'])
            proms[proms_index]['data'].append(
                df['data'].iloc[:, [2]].mean().values[0].round(2))
            ind1 = ind2
        if len(no_vacios) <= 1:
            """ merged_df.rename(
                columns={'Disponibilidad': 'Disponibilidad_1', 'num': 'num_1'}, inplace=True) """
        promedios = sorted(promedios, key=lambda l: l['index'])
        promedios = promedios[1:]
        indices = [promedio['index'] for promedio in promedios]
        promedios = [promedio['data'] for promedio in promedios]

        merged_df = merged_df.replace(np.nan, 0).drop(
            columns=['Avg_min', 'num_0'])
        metricas.append({'metric_name': "Alineacion",
                        'indices': indices,
                         'availability_average': promedios,
                         'days': dias,
                         'device_count': dispositivos,
                         'data_range': data_range,
                         'time': tiempo,
                         'first_data': first,
                         'last_data': last,
                         'dataset': merged_df.to_dict(orient="records"),
                         'dataset2': general.to_dict(orient='records'),
                         'availavility_average2': promedios_general,
                         'municipality': municipios,
                         'hostids': hostids
                         })


async def procesar_vacio_alineacion_device(vacio, mayor, hostid):
    vacio['Tiempo'] = [mayor['Tiempo'][ind] for ind in mayor.index]
    """ print(vacio, "Si es este") """
    vacio['num'] = [0 for ind in range(len(mayor))]
    vacio[f'Alineacion_{hostid}'] = [0 for ind in range(len(mayor))]
    vacio["templateid"] = [0 for ind in range(len(mayor))]
    vacio["itemid"] = [0 for ind in range(len(mayor))]
    vacio = vacio.replace(np.nan, 0)
    return vacio


async def get_index(lst, key, value):
    return next((index for (index, d) in enumerate(lst) if d[key] == value), None)


async def process_dates(data, init_date, end_date, freq, date_format):
    fechas = pd.date_range(start=init_date, end=end_date, freq=freq)
    mayor = pd.DataFrame({'Tiempo': fechas, 'num': 0, 'Avg_min': 0})
    mayor['Tiempo'] = pd.to_datetime(mayor['Tiempo'])
    data = data.groupby(
        [pd.to_datetime(data['time']).dt.floor(freq).rename("date").dt.strftime(date_format)])[['num', 'Avg_min']].mean().round(6).reset_index()
    data['date'] = pd.to_datetime(data['date'])
    data['date'] = data['date'].apply(
        find_nearest_date, args=(mayor['Tiempo'],))
    data['date'] = pd.to_datetime(data['date']).dt.strftime(date_format)
    data = data[['date', 'num', 'Avg_min']]
    data.rename(columns={'date': 'time'}, inplace=True)
    return data
