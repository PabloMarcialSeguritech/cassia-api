from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix, DB_C5
from sqlalchemy import text
from utils.traits import success_response
import numpy as np
from paramiko import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import AuthenticationException, BadHostKeyException, SSHException
from cryptography.fernet import Fernet
from models.interface_model import Interface as InterfaceModel
from models.cassia_config import CassiaConfig
from models.cassia_arch_traffic_events import CassiaArchTrafficEvent
import socket
import pyzabbix
from pyzabbix.api import ZabbixAPI
from fastapi.exceptions import HTTPException
from fastapi import status


settings = Settings()


async def get_templates():
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session() """

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        """ session.close() """
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al concectar con Zabbix",
        )

    params = {
        "output": ['templateid', 'name']
    }
    response = api_zabbix.do_request(method='template.get',
                                     params=params)
    """ print(response) """

    return success_response(data=response)


async def get_groups():
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session() """

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        """ session.close() """
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al concectar con Zabbix",
        )

    params = {
        "output": ['groupid', 'name']
    }
    response = api_zabbix.do_request(method='hostgroup.get',
                                     params=params)
    """ print(response) """

    return success_response(data=response)


async def get_proxys():
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session() """

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        """ session.close() """
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al concectar con Zabbix",
        )

    params = {
        "output": ["proxy_hostid", "host", "proxyid"]
    }
    response = api_zabbix.do_request(method='proxy.get',
                                     params=params)
    """ print(response) """

    return success_response(data=response)


async def create_host():
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session() """

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        """ session.close() """
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al concectar con Zabbix",
        )

    params = {
        "host": "TEST BACK con proxy inventory location active interface",
        "interfaces": [
            {
                "type": 1,
                "main": 1,
                "useip": 1,
                "ip": "192.12.12.15",
                "dns": "",
                "port": "10050"
            }
        ],

        "groups": [
            {
                "groupid": "54"
            }
        ],
        "templates": [{
            "templateid": "10186"
        }],

        "inventory_mode": 0,
        "inventory": {
            "location": "Celaya prueba",
            "location_lat": "-12.12",
            "location_lon": "12.12"
        },
        'status': 0,
        "proxy_hostid": "10515",
    }

    """ NOTAS
    Para dar de alta con un templateid de ICMPing se tiene que dar de alta una interfaz
    {
        "host": "TEST BACK con proxy y con template ICMPing", nombre del host
        "interfaces": [ Interfaces del host
            {
                "type": 1, tipo de interfaz
                "main": 1, principal
                "useip": 1, poner ip
                "ip": "192.12.12.12",
                "dns": "", poner en blando si no hay
                "port": "10050" puerto
            }
        ],

        "groups": [ grupos del host
            {
                "groupid": "54"
            }
        ],
        "templates": [{ templates asociados al host
            "templateid": "10186" por ejemplo ICMPing
        }],

        "inventory_mode": 0, -1 disabled, 0 manual, 1 automatic
        "inventory": {
            "location": "Ubicación_física",
            "alias": "Alias_del_equipo",
            "location_lat":"-12.12",
            "location_lon":"12.12"
            
            // Puedes agregar más campos según sea necesario
        }
        'status': 0, 0 default(monitored, active), 1 unmonitored(inactive)
        "proxy_hostid": "10515", id del proxy
    }
       """
    try:
        response = api_zabbix.do_request(method='host.create',
                                         params=params)
    except Exception as e:
        print(e)
        print(type(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=format(e),
        )

    print(response)

    return success_response(data=response)


async def update_host():
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session() """

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        """ session.close() """
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al concectar con Zabbix",
        )
    """ NOTAS
    Si se pasa algun parametro se tienen que pasar todas las propiedades de nuevo, si no, las que no se pasen se borran
    Por ejemplo si pasamos groups con menos groups de los originales solo se conservaran los que se definieron al principio,

    {
        "host": "TEST BACK con proxy y con template ICMPing", nombre del host
        "interfaces": [ Interfaces del host
            {
                "type": 1, tipo de interfaz
                "main": 1, principal
                "useip": 1, poner ip
                "ip": "192.12.12.12",
                "dns": "", poner en blando si no hay
                "port": "10050" puerto
            }
        ],

        "groups": [ grupos del host
            {
                "groupid": "54"
            }
        ],
        "templates": [{ templates asociados al host
            "templateid": "10186" por ejemplo ICMPing
        }],

        "inventory_mode": 0, -1 disabled, 0 manual, 1 automatic
        "inventory": {
            "location": "Ubicación_física",
            "alias": "Alias_del_equipo",
            "location_lat":"-12.12"
            "location_lon":"12.12"
            
            // Puedes agregar más campos según sea necesario
        }
        'status': 0, 0 default(monitored, active), 1 unmonitored(inactive)
        "proxy_hostid": "10515", id del proxy
    }
       """

    params = {
        "hostid": 23158,
        "inventory_mode": 0,
        'status': 0
    }
    try:
        response = api_zabbix.do_request(method='host.update',
                                         params=params)
    except Exception as e:
        print(e)
        print(type(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=format(e),
        )

    print(response)

    return success_response(data=response)


async def get_host(hostid):
    """ db_zabbix = DB_Zabbix()
    session = db_zabbix.Session() """

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        """ session.close() """
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al concectar con Zabbix",
        )

    params = {
        "output": "extend",
        "hostids": [hostid],
        "selectGroups": ["groupid", "name"],
        "selectParentTemplates": ["templateid", "name"],
        "selectInterfaces": ["interfaceid", "ip", "dns", "port"],
        "selectInventory": ["inventory_mode", "type", "location", "location_lat", "location_lon"]
    }
    response = api_zabbix.do_request(method='host.get',
                                     params=params)
    """ print(response) """

    return success_response(data=response)
