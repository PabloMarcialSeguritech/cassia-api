from services.integration.reset_service_facade import ResetServiceFacade
from utils.settings import Settings
import requests
import pandas as pd
from infraestructure.cassia import CassiaResetRepository
from utils.traits import success_response, failure_response
from fastapi import Depends
from typing import Optional
import asyncio
import subprocess
import asyncssh
import re
import nest_asyncio
import time
from fastapi.responses import JSONResponse
import json


class ResetServiceImpl(ResetServiceFacade):
    settings = None

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings if settings is not None else Settings()

    async def authenticate(self):
        headers = {'Content-Type': 'application/json'}
        body = {
            "username": self.settings.resets_username,
            "password": self.settings.resets_passwd,
            "role": self.settings.resets_role
        }
        try:
            response = requests.post(self.settings.resets_auth_url, json=body, headers=headers)
            if response.status_code == 200:
                print("Authentication successful")
                response_json = response.json()
                return response_json.get('session', {}).get('token')
            else:
                print(f"Failed to authenticate, status code: {response.status_code}, response: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request exception occurred: {e}")
            return None

    async def get_devices(self, token):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        try:
            response = requests.get(self.settings.resets_device_url, headers=headers)
            if response.status_code == 200:
                print("Successfully retrieved devices")
                devices_json = response.json()
                if 'device' in devices_json:
                    return pd.DataFrame(devices_json['device'])
                else:
                    print("No 'device' key in response JSON")
                    return pd.DataFrame()
            else:
                print(f"Failed to get devices, status code: {response.status_code}, response: {response.text}")
                return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            print(f"Request exception occurred: {e}")
            return pd.DataFrame()

    async def extract_device_info(self, devices_df):
        if devices_df.empty:
            print("No devices data available")
            return []

        # Definir una función para obtener el valor del primer elemento en 'info'
        def get_first_info_value(info):
            if isinstance(info, list) and len(info) > 0:
                return info[0].get('value', 'N/A')
            return 'N/A'

        # Aplicar la función a cada elemento de la columna 'info'
        if 'info' in devices_df.columns:
            devices_df['affiliation'] = devices_df['info'].apply(get_first_info_value).str.strip()

        devices_df['longitude'] = devices_df['gis'].apply(
            lambda x: x['coordinates'][0] if 'coordinates' in x and len(x['coordinates']) > 0 else 'N/A')
        devices_df['latitude'] = devices_df['gis'].apply(
            lambda x: x['coordinates'][1] if 'coordinates' in x and len(x['coordinates']) > 1 else 'N/A')

        return devices_df[[
            'id', 'objid', 'affiliation'
        ]].rename(columns={'id': 'imei', 'objid': 'object_id'})

    async def merge_resets(self):
        token = await self.authenticate()

        # Obtener los datos de los dispositivos
        devices_df = await self.extract_device_info(await self.get_devices(token))
        if devices_df.empty:
            print("No devices data available")
            return

        # Filtrar registros con 'affiliation' no vacío
        devices_df = devices_df[devices_df['affiliation'].str.strip() != '']
        if devices_df.empty:
            print("No valid devices data available")
            return

        # Obtener los datos de los resets
        resets_df = await self.get_cassia_resets()
        # Si resets_df está vacío, insertar todos los registros de devices_df
        if resets_df.empty:
            print("No resets data available. Insertando todos los registros de devices_df.")
            # Inserción masiva
            await CassiaResetRepository.create_cassia_resets_bulk(devices_df.to_dict('records'))
            print("Todos los registros de devices_df han sido insertados.")
            return
        # Listas para actualizaciones e inserciones
        updates = []
        inserts = []

        # Comparar y actualizar o insertar registros
        for _, device in devices_df.iterrows():
            affiliation = device['affiliation']
            if not affiliation:
                # Evitar dispositivos con IMEI {device['IMEI']} que no tienen affiliation
                continue

            matching_resets = resets_df[resets_df['affiliation'] == affiliation]

            if not matching_resets.empty:

                for _, reset in matching_resets.iterrows():
                    if device['affiliation'] == reset['affiliation']:
                        # Actualizar el registro
                        print(f"Updating reset for affiliation {affiliation}")
                        # Añadir a la lista de actualizaciones
                        updates.append(reset.to_dict())
                    else:
                        # Insertar un nuevo registro
                        print(f"Inserting new reset for affiliation {affiliation}")
                        # Añadir a la lista de inserciones
                        inserts.append(device.to_dict())
            else:
                # Insertar un nuevo registro
                print(f"Inserting new reset for affiliation {affiliation}")
                # Añadir a la lista de inserciones
                inserts.append(device.to_dict())

        # Realizar actualizaciones en bloque
        if updates:
            response = await CassiaResetRepository.update_cassia_resets_bulk(updates)
            print(response)

        # Realizar inserciones en bloque
        if inserts:
            await CassiaResetRepository.create_cassia_resets_bulk(inserts)

    async def get_cassia_resets(self):
        resets = await CassiaResetRepository.get_resets()
        return resets

    async def restart_reset(self, object_id, host_id) -> pd.DataFrame:

        devices_related_pmi_df = await self.getDispositivosRelacionadosCapa1(host_id)

        suscriptor_ip = devices_related_pmi_df.loc[devices_related_pmi_df['name'] == 'SUSCRIPTOR', 'ip']

        # Assuming you want to return the IP or do something with it
        if not suscriptor_ip.empty:
            suscriptor_ip = suscriptor_ip.iloc[0]  # Get the first IP if there are multiple matches
            # Print or log the IP if needed
            print(f"SUSCRIPTOR IP: {suscriptor_ip}")
            success_ping, ping_message = await self.async_ping_by_local(suscriptor_ip)

            if success_ping:
                token = self.authenticate()
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {token}'
                }
                try:

                    response = requests.post(
                        f'http://172.16.4.191:5978/api/secure/device/{object_id}/cmd/relayctl',
                        json={
                            "operation": "pulse",
                            "relay": 1,
                            "interval": 1,
                            "times": 9,
                            "period": 1,
                            "tag": "TODOS LOS DISPOSITIVOS durante 1 min",
                            "user": {}
                        },
                        headers=headers)
                    if response.status_code == 200:
                        print("Successfully restart device")
                        print("response::::", response)
                        reset_json = response.json()
                        print("response_json::::", reset_json)
                        return success_response(
                            data=reset_json
                        )
                    else:
                        print(f"Failed to post device, status code: {response.status_code}, response: {response.text}")
                        return pd.DataFrame()
                except requests.exceptions.RequestException as e:
                    print(f"Request exception occurred: {e}")
                    return pd.DataFrame()

        return None

    async def getDispositivosRelacionadosCapa1(self, hostid):
        devices_related_df = await CassiaResetRepository.get_devices_related_layer1(hostid)
        return devices_related_df

    async def reset_pmi(self, afiliacion):
        # Checar estatus de dispositivos en PMI
        devices_pmi_df = await self.get_devices_pmi(afiliacion)
        print("devices_pmi_df[deviceid = 4]::::::::::::::::::::::::", devices_pmi_df['device_id'].eq(4))

        devices_pmi_status_connection_df = await self.get_devices_pmi_status_connection(
            devices_pmi_df, is_initial_status=True)  # PMI ddispositivos estado inicial
        print("Estado inicial PMI::::", devices_pmi_status_connection_df)
        pmi_status: bool = False
        object_id = await self.get_object_id_by_affiliation(afiliacion)
        print("object_id:::::::", object_id)
        is_pmi_up = await self.is_pmi_up(devices_pmi_status_connection_df, is_initial_status=True)
        response = None
        if is_pmi_up:
            print("PMI arriba")
            # caso de reset cuando todos los disp de reset comienzan en up
            return await self.reset_pmi_case_all_up(object_id, devices_pmi_df)
        else:
            print("PMI abajo total")
            # caso de reset cuando todos los disp de reset comienzan en down
            return await self.reset_pmi_case_all_down(object_id, devices_pmi_df)

    async def get_devices_pmi(self, affiliation):
        devices_related_df = await CassiaResetRepository.get_devices_related_layer1(affiliation)
        return devices_related_df

    '''
        Extraer lo objs por separa y mandar hacer ping 
        devuelve un un atributo de columna que es su estatus
    '''

    async def get_devices_pmi_status_connection(self, devices_pmi_df, is_initial_status):
        # Lista para almacenar las tareas
        tasks = []
        for _, row in devices_pmi_df.iterrows():
            ip = row['ip']
            if pd.notna(ip):
                has_proxy, proxy_credentials = await self.proxy_validation(ip)
                if has_proxy:
                    tasks.append(self.async_ping(ip, proxy_credentials['ip_proxy'],
                                                 proxy_credentials['user_proxy'],
                                                 proxy_credentials['password_proxy']))
                else:
                    tasks.append(self.async_ping_by_local(ip))
            else:
                tasks.append(asyncio.sleep(0, result=(False, "No IP provided")))

        # Ejecutar todas las tareas concurrentemente y obtener los resultados
        results = await asyncio.gather(*tasks)

        # Asignar los resultados a las columnas del DataFrame
        if is_initial_status:
            devices_pmi_df['initial_status'] = [result[0] for result in results]
        else:
            devices_pmi_df['current_status'] = [result[0] for result in results]

        return devices_pmi_df

    async def async_ping(self, ip, ssh_host, ssh_user, ssh_pass):
        try:
            tasks = {
                asyncio.create_task(self.async_ping_by_local(ip)),
                asyncio.create_task(self.async_ping_by_proxy(
                    ip, ssh_host, ssh_user, ssh_pass))
            }
            while tasks:
                # Espera hasta que al menos una tarea se complete
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )

                # Verificar las tareas completadas por un resultado exitoso
                for task in done:
                    success, message = task.result()
                    if success == 1:
                        # Cancelar cualquier tarea restante
                        for t in pending:
                            t.cancel()
                        return success, message
                    # Si no se encontró una tarea exitosa, continuar con las tareas pendientes
                    tasks = pending

                # Si llegamos a este punto, ninguna tarea fue exitosa
                success = False
                message = "no ejecutado con exito"
                return success, message

        except Exception as e:
            print(f"Excepción ocurrida en la función async_ping: {e}")
            success = False
            message = f"No exitoso por excepción: {str(e)}"
            return success, message

    async def async_ping_by_local(self, ip):
        # -n para windows -c para ubuntu
        cmd = ['ping', '-c', '4', '-w', '5', ip]  # Envía 4 paquetes
        try:
            process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = await process.communicate()
            output = stdout.decode('utf-8', errors='ignore')
            if "100% packet loss" in output or "100% pérdida de paquetes" in output or "100% perdidos" in output:
                success_ping = False
                message = "no ejecutado con exito"
                return success_ping, message
            else:
                success_ping = True
                message = "success"
                return success_ping, message

        except Exception as e:
            print(f"Excepción ocurrida en la función async_ping_by_local: {e}")
            success_ping = False
            message = "no ejecutado con exito"
            return success_ping, message

    async def async_ping_by_proxy(self, ip, ssh_host, ssh_user, ssh_pass):
        # Si hace el ping retorna 1 sino retorna 0
        success_ping = False
        message = ""
        try:

            async with asyncssh.connect(ssh_host, username=ssh_user, password=ssh_pass, known_hosts=None,
                                        connect_timeout=6) as conn:
                result = await conn.run(f'ping -c 2 -W 5 {ip}', check=False)
                is_package_lost = await self.verify_output_ping(result.stdout)
                if is_package_lost:
                    success_ping = False
                    message = "no ejecutado con exito"
                else:
                    success_ping = True
                    message = "success"
                return success_ping, message

        except Exception as e:
            print(f"Excepción ocurrida en la función async_ping_by_proxy:{str(e)}")
            success_ping = False
            message = "no ejecutado con exito"
        return success_ping, message

    async def verify_output_ping(self, output):
        is_package_lost = 1
        text_coincidence = re.search(
            r'(\d+ packets transmitted, \d+ received, 100% packet loss, time \d+ms)',
            output)
        if not text_coincidence:
            is_package_lost = 0
        return is_package_lost

    async def proxy_validation(self, ip):
        has_proxy = 0
        credentials = None
        try:
            ip_proxy, user_proxy, password_proxy = await self.get_credentials_for_proxy_async(ip)
            if ip_proxy and user_proxy and password_proxy:
                has_proxy = 1
                credentials = {
                    'ip_proxy': ip_proxy,
                    'user_proxy': user_proxy,
                    'password_proxy': password_proxy
                }
            return has_proxy, credentials
        except (ValueError, Exception) as e:
            print(f"Error en proxy_validation: {e}")
            has_proxy = False
            credentials = {
                'ip_proxy': None,
                'user_proxy': None,
                'password_proxy': None
            }
        return has_proxy, credentials

    async def get_credentials_for_proxy_async(self, ip):
        ip_proxy, user_proxy, password_proxy = await CassiaResetRepository.get_credentials_for_proxy(ip)
        return ip_proxy, user_proxy, password_proxy

    async def is_pmi_up(self, devices_pmi_status_connection_df, is_initial_status):

        if is_initial_status:
            # Verificar si todos los dispositivos 'True' con initial status
            if devices_pmi_status_connection_df['initial_status'].eq(True).all():
                return True
            elif devices_pmi_status_connection_df['initial_status'].eq(False).all():
                return False
            else:
                return False
        else:
            # Verificar si todos los dispositivos 'True'
            if devices_pmi_status_connection_df['current_status'].eq(True).all():
                return True
            elif devices_pmi_status_connection_df['current_status'].eq(False).all():
                return False
            else:
                return False

    async def is_any_pmi_up(self, devices_pmi_status_connection_df):

        # Verificar si al menos uno de los dispositivos filtrados tiene el estado 'True'
        if devices_pmi_status_connection_df['current_status'].eq(True).any():
            return True # regresa a true si al menos un disp esta arriba
        else:
            return False # regresa false si todos los dispositivos estan abajo

    async def is_any_pmi_down(self, devices_pmi_status_connection_df):

        # Verificar si al menos uno de los dispositivos filtrados tiene el estado 'False'
        if devices_pmi_status_connection_df['current_status'].eq(False).any():
            return True # regresa a true si al menos un disp esta abajo
        else:
            return False # regresa false si todos los dispositivos estan arriba

    async def get_object_id_by_affiliation(self, affiliation):
        reset_df = await CassiaResetRepository.get_reset_by_affiliation(affiliation)

        if reset_df.empty:
            print("No se encontró ningún registro para la afiliación proporcionada.")
            return None  # O maneja esto de acuerdo a tus necesidades

        # Asegúrate de que la columna 'object_id' exista en el DataFrame
        if 'object_id' not in reset_df.columns:
            print("La columna 'object_id' no existe en el DataFrame.")
            return None  # O maneja esto de acuerdo a tus necesidades

        # Retorna el primer valor de la columna 'object_id'
        return reset_df['object_id'].iloc[0]

    async def send_request_api_reset(self, object_id):
        token = await self.authenticate()
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        print("headers:::::::::::::::::", headers)
        try:

            response = requests.post(
                f'http://st.gruposeguritech.com:5978/api/secure/device/{object_id}/cmd/relayctl',
                json={
                    "operation": "pulse",
                    "relay": 1,
                    "interval": 1,
                    "times": 9,
                    "period": 1,
                    "tag": "TODOS LOS DISPOSITIVOS durante 1 min",
                    "user": {}
                },
                headers=headers)
            if response.status_code == 200:
                print("Successfully restart device")
                print("response::::", response)
                reset_json = response.json()
                print("response_json::::", reset_json)
                return success_response(
                    data=reset_json
                )
            else:
                print(f"Failed to post device, status code: {response.status_code}, response: {response.text}")
                return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            print(f"Request exception occurred: {e}")
            return pd.DataFrame()

    async def is_response_good(self, response: JSONResponse) -> bool:
        """
        Evalúa la respuesta de una petición de API basada en el código de estado HTTP.

        :param response: Respuesta de la petición a la API
        :return: True si la respuesta es correcta, False en caso contrario
        """
        try:
            # Verificar el código de estado de la respuesta
            if response.status_code >= 400:
                return False

            # Obtener el contenido de la respuesta como JSON
            response_json = response.body.decode('utf-8')
            response_data = json.loads(response_json)

            print("data:::::", response_data.get('data')['status'])
            # Verificar el campo 'status' en el JSON
            if response_data.get('data')['status'] == 0:
                return True
            else:
                return False

        except Exception as e:
            # Manejar cualquier excepción al interpretar el JSON
            print(f"Error al interpretar el JSON: {e}")
            return False

    async def monitoring_any_device_up(self, devices_pmi_df):
        pmi_status: bool = False
        timeout_offline = 5 * 60  # 5 min
        start_time_offline = time.time()
        devices_pmi_status_df = None
        while True:
            try:
                # ping de todos los dispositivos
                devices_pmi_status_df = await self.get_devices_pmi_status_connection(devices_pmi_df,
                                                                                     is_initial_status=False)
                # evaluar si hay algun dispositivo que ya se levanto
                is_any_device_up = self.is_any_pmi_up(devices_pmi_status_df)  #is_pmi_up(devices_pmi_status_df, is_initial_status=False)
                if is_any_device_up:
                    # si ya se levanto uno salir del while, sino manetenerse
                    pmi_status = True
                    break
            except Exception as e:
                print(f"Error durante ping de PMIs: {str(e)}")

            if time.time() - start_time_offline > timeout_offline:
                print("Tiempo fuera de linea agotado en PMI.")
                return failure_response(message="Verificación de reboot tiempo de espera agotado para PMI")

            # Ajustar el intervalo entre intentos de ping
            time.sleep(10)

        return pmi_status, devices_pmi_status_df

    async def reset_pmi_case_all_up(self, object_id, devices_pmi_df):
        pmi_status = True
        response = None
        if object_id:
            response = await self.send_request_api_reset(object_id)
        else:
            return failure_response(message="Object ID necesario para la petición")
        if not await self.is_response_good(response):
            print("PMI Arriba No se realizó con éxito el reset, hubo falla en la petición")
            return failure_response(message="PMI Arriba No se realizó con éxito el reset, hubo falla en la petición")
        # Monitorear durante 4 minutos para verificar que to-do esté abajo
        timeout_offline = 4 * 60  # 4 minutos
        start_time_offline = time.time()
        devices_pmi_status_df = None
        while True:
            try:
                # Ping de todos los dispositivos
                devices_pmi_status_df = await self.get_devices_pmi_status_connection(devices_pmi_df,
                                                                                     is_initial_status=False)
                # Evaluar si al menos uno está abajo
                is_pmi_down = await self.is_any_pmi_down(devices_pmi_status_df)   #is_pmi_up(devices_pmi_status_df, is_initial_status=False)
                if is_pmi_down:
                    # Verificar que al menos uno este arriba
                    pmi_status, devices_pmi_status_df = await self.monitoring_any_device_up(devices_pmi_df)
                    if pmi_status:
                        print("Reset de PMI exitoso")
                        # El listado de dispositivos con el estatus y mensaje de respuesta
                        return success_response(
                            data=devices_pmi_status_df.to_dict(orient="records"),
                            message="Reset de PMI exitoso"
                        )
                    else:
                        print("Reset de PMI no exitoso")
                        return failure_response(
                            data=devices_pmi_status_df.to_dict(orient="records"),
                            message="No se realizó con éxito el reset"
                        )
            except Exception as e:
                print(f"Error durante ping de PMIs: {str(e)}")
                return failure_response(message="Error durante ping de PMIs")

            if time.time() - start_time_offline > timeout_offline:
                print("Tiempo fuera de línea agotado en PMI.")
                # Al terminar los 5 minutos to-do sigue arriba
                if not is_pmi_down:
                    print("not is_pmi_down")
                    return failure_response(message="No se realizó con éxito el reset, PMI sigue conectado",
                                            recommendation="Reintentar el reset",
                                            data=devices_pmi_status_df.to_dict(orient="records"))
                print("if is_pmi_up:")
                return failure_response(message="Verificación de reboot tiempo de espera agotado para PMI")

            # Ajustar el intervalo entre intentos de ping
            await asyncio.sleep(10)

    async def reset_pmi_case_all_down(self, object_id, devices_pmi_df):
        pmi_status = False
        if object_id:
            response = await self.send_request_api_reset(object_id)
        else:
            return failure_response(message="Object ID necesario para la petición")
        if not await self.is_response_good(response):
            print("No se realizó con éxito el reset, hubo falla en la petición")
            # Agregar la lógica para retornar el error por endpoint
            return failure_response(message="No se realizó con éxito el reset, hubo falla en la petición")

        pmi_status, devices_pmi_status_df = await self.monitoring_any_device_up(devices_pmi_df)

        if pmi_status:
            print("Reset de PMI exitoso")
            # El listado de dispositivos con el estatus y mensaje de respuesta
            return success_response(
                data=devices_pmi_status_df.to_dict(orient="records"),
                message="Reset de PMI exitoso"
            )

        return failure_response(message="No se realizó con éxito el reset")


