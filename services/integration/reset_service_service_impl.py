from services.integration.reset_service_facade import ResetServiceFacade
from utils.settings import Settings
import requests
import pandas as pd
from infraestructure.cassia import CassiaResetRepository
from utils.traits import success_response
from fastapi import Depends
from typing import Optional


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

    async def restart_reset(self, object_id) -> pd.DataFrame:
        token = await self.authenticate()

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
