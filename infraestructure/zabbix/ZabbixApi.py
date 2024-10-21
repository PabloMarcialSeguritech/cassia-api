import httpx
from utils.settings import Settings
from fastapi import HTTPException, status
import time
SETTINGS = Settings()


class ZabbixApi:

    def __init__(self) -> None:
        print("A")
        self.ZABBIX_URL = SETTINGS.zabbix_server_url
        print("B")
        self.ZABBIX_USER = SETTINGS.zabbix_user
        print("C")
        self.ZABBIX_PASSWORD = SETTINGS.zabbix_password
        print("D")
        self.ZABBIX_VERSION_6 = "6.0"
        print("E")

    async def get_zabbix_version(self) -> str:
        async with httpx.AsyncClient() as client:
            params = {
                "jsonrpc": "2.0",
                "method": "apiinfo.version",
                "params": [],
                "id": 1,
            }
            respuesta = await client.post(self.ZABBIX_URL, json=params, timeout=40)
            if respuesta.status_code == 200:
                # Si la solicitud es exitosa, obtener la versión de Zabbix
                respuesta_json = respuesta.json()
                if 'result' in respuesta_json:
                    version = respuesta_json['result']
                    return version
                else:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Error al obtener version de zabbix"
                    )
            else:
                # Si hay un error en la solicitud, imprimir el código de estado
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Error al obtener version de zabbix"
                )

    async def get_zabbix_token(self) -> str:
        async with httpx.AsyncClient() as client:
            version = await self.get_zabbix_version()
            if version < self.ZABBIX_VERSION_6:
                params = {
                    "jsonrpc": "2.0",
                    "method": "user.login",
                    "params": {
                        "user": self.ZABBIX_USER,
                        "password": self.ZABBIX_PASSWORD
                    },
                    "id": 1,
                    "auth": None
                }
                respuesta = await client.post(self.ZABBIX_URL, json=params, timeout=60)
                if respuesta.status_code == 200:
                    respuesta_json = respuesta.json()
                    if 'result' in respuesta_json:
                        token = respuesta_json['result']
                        return token
                    else:
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Credenciales de Zabbix incorrectas"
                        )
                else:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Credenciales de Zabbix incorrectas"
                    )
            else:
                params = {
                    "jsonrpc": "2.0",
                    "method": "user.login",
                    "params": {
                        "username": self.ZABBIX_USER,
                        "password": self.ZABBIX_PASSWORD
                    },
                    "id": 1
                }

                respuesta = await client.post(self.ZABBIX_URL, json=params, timeout=60)

                if respuesta.status_code == 200:
                    respuesta_json = respuesta.json()

                    if 'result' in respuesta_json:
                        token = respuesta_json['result']
                        return token
                    else:
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Credenciales de Zabbix incorrectas"
                        )
                else:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Credenciales de Zabbix incorrectas"
                    )

    async def do_request(self, method: str, params: dict):
        try:

            token = await self.get_zabbix_token()
            request = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "auth": token,
                "id": 1
            }
            async with httpx.AsyncClient() as client:
                respuesta = await client.post(self.ZABBIX_URL, json=request, timeout=120)
                print(respuesta)
                if respuesta.status_code == 200:
                    respuesta_json = respuesta.json()
                    if 'result' in respuesta_json:
                        result = respuesta_json['result']
                        return result
                else:
                    respuesta_json = respuesta_json()
                    error = respuesta_json['error']
                    print(
                        f"Error al hacer peticion {method} en la api de zabbix, {error}")
                    return error
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Error al hacer peticion {method} en la api de zabbix, {error}"
                    )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error al hacer peticion {method}: {e}"
            )

    async def do_request_new(self, method: str, params: dict):
        try:

            token = await self.get_zabbix_token()
            request = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "auth": token,
                "id": 1
            }
            print(request)
            async with httpx.AsyncClient() as client:
                respuesta = await client.post(self.ZABBIX_URL, json=request, timeout=120)
                response = {'success': False, 'result': None}
                if respuesta.status_code == 200:
                    respuesta_json = respuesta.json()
                    return respuesta_json
                    if 'result' in respuesta_json:
                        result = respuesta_json['result']
                        response["result"] = result
                        response["success"] = True
                        return response
                    else:
                        if 'error' in respuesta_json:
                            result = respuesta_json['error']
                            response["result"] = result
                            response["success"] = False
                            return response
                        else:
                            return response
                else:
                    respuesta_json = respuesta_json()
                    return respuesta_json
                    error = respuesta_json['error']
                    print(
                        f"Error al hacer peticion {method} en la api de zabbix, {error}")
                    return error
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Error al hacer peticion {method} en la api de zabbix, {error}"
                    )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error al hacer peticion {method}: {e}"
            )
