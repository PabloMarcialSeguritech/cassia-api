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
from models.cassia_host_credentials import HostCredential
from schemas import host_management_schema
import socket
import pyzabbix
from cryptography.fernet import Fernet
from pyzabbix.api import ZabbixAPI
from fastapi.exceptions import HTTPException
from fastapi import status


settings = Settings()


async def get_hosts():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    hosts = text("call sp_cassiaHostAdmList()")
    hosts = pd.DataFrame(session.execute(hosts)).replace(np.nan, "")

    session.close()
    return success_response(data=hosts.to_dict(orient="records"))


async def get_protocols():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    protocols = text("select * from cassia_host_protocols")
    protocols = pd.DataFrame(session.execute(protocols)).replace(np.nan, "")
    if not protocols.empty:
        protocols['id'] = protocols["protocol_id"]
        protocols['value'] = protocols["name"]
    session.close()
    return success_response(data=protocols.to_dict(orient="records"))


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
            "location_lat":"-12.12",
            "location_lon":"12.12",
            "alias": "Alias_del_equipo",
            "name":"Nombre_equipo"
            
            // Puedes agregar más campos según sea necesario
        }
        'status': 0, 0 default(monitored, active), 1 unmonitored(inactive)
        "proxy_hostid": "10515", id del proxy
    }
       """


def check_lenght(arreglos: list):
    lenght = len(arreglos[0])
    for arreglo in arreglos:
        if lenght != len(arreglo):
            return False
    return True


def check_same_values(arreglo: list):
    number = arreglo[0]
    for numero in arreglo:
        if number != numero:
            return False
    return True


def validate_data(datos: list):
    for dato in datos:
        if not dato or dato == "":
            return False
    return True


async def create_host(host_data: host_management_schema.HostManagementBase):
    if not check_lenght([host_data.credentials_protocol_ids, host_data.credentials_users, host_data.credentials_password, host_data.credentials_ports]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El tamaño del arreglo de las credenciales no concuerda.",
        )
    if not check_same_values(host_data.credentials_protocol_ids):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Solo puede existir 1 credencial por protocolo.",
        )
    agent_valid = validate_data(
        [host_data.interface_agent_ip, host_data.interface_agent_port])
    snmp_valid = validate_data([host_data.interface_snmp_ip, host_data.interface_snmp_port,
                               host_data.interface_snmp_version, host_data.interface_snmp_community])
    if not (agent_valid or snmp_valid):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debe existir al menos una interfaz del host.",
        )

    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al concectar con Zabbix",
        )
    interfaces = []
    if agent_valid:
        interfaces.append({
            "type": 1,
            "main": 1,
            "useip": 1,
            "ip": host_data.interface_agent_ip,
            "dns": host_data.interface_agent_dns,
            "port": host_data.interface_agent_port
        },
        )
    if snmp_valid:
        interfaces.append(
            {
                "type": 2,
                "main": 1,
                "useip": 1,
                "ip": host_data.interface_snmp_ip,
                "dns": host_data.interface_snmp_dns,
                "port": host_data.interface_snmp_port,
                "details": {
                    "version": host_data.interface_snmp_version,
                    "community": host_data.interface_snmp_community
                }

            }
        )
    params = {
        "host": host_data.host,
        "interfaces": interfaces,
        "groups": [{"groupid": host_group} for host_group in host_data.host_groups],
        "templates": [{"templateid": host_template} for host_template in host_data.host_templates],
        "inventory_mode": host_data.inventory_mode,
        "inventory": {
            "location_lat": host_data.location_lat,
            "location_lon": host_data.location_lon,
            "alias": host_data.alias,
            "name": host_data.name
        },
        'status': host_data.status,
        "proxy_hostid": host_data.proxy_hostid,
    }

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

    hostid = response['result']['hostids'][0]
    try:
        for index in range(len(host_data.credentials_protocol_ids)):
            host_credential = HostCredential(
                user=host_data.credentials_users[index],
                password=encrypt(
                    host_data.credentials_password[index], settings.ssh_key_gen),
                protocol_id=host_data.credentials_protocol_ids[index],
                port=host_data.credentials_ports[index],
                hostid=hostid
            )
            session.add(host_credential)
            session.commit()
        session.close()
    except Exception as e:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=format(e),
        )
    return success_response(data=response)


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


async def update_host(hostid, host_data: host_management_schema.HostManagementBase):
    if not check_lenght([host_data.credentials_protocol_ids, host_data.credentials_users, host_data.credentials_password, host_data.credentials_ports]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El tamaño del arreglo de las credenciales no concuerda.",
        )
    if not check_same_values(host_data.credentials_protocol_ids):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Solo puede existir 1 credencial por protocolo.",
        )
    agent_valid = validate_data(
        [host_data.interface_agent_ip, host_data.interface_agent_port])
    snmp_valid = validate_data([host_data.interface_snmp_ip, host_data.interface_snmp_port,
                               host_data.interface_snmp_version, host_data.interface_snmp_community])
    if not (agent_valid or snmp_valid):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debe existir al menos una interfaz del host.",
        )
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

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
        "hostid": hostid,
        "host": host_data.host,
        "groups": [{"groupid": host_group} for host_group in host_data.host_groups],
        "templates": [{"templateid": host_template} for host_template in host_data.host_templates],
        "inventory_mode": host_data.inventory_mode,
        "inventory": {
            "location_lat": host_data.location_lat,
            "location_lon": host_data.location_lon,
            "alias": host_data.alias,
            "name": host_data.name
        },
        'status': host_data.status,
        "proxy_hostid": host_data.proxy_hostid,
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
    responses = []
    responses.append(response)
    print(response)
    if agent_valid:
        params = {
            "interfaceid": host_data.interface_agent_id,
            "ip": host_data.interface_agent_ip,
            "dns": host_data.interface_agent_dns,
            "port": host_data.interface_agent_port
        }
        try:
            response = api_zabbix.do_request(method='hostinterface.update',
                                             params=params)

        except Exception as e:
            print(e)
            print(type(e))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=format(e),
            )
        responses.append(response)
        print(response)
    if snmp_valid:

        params = {
            "interfaceid": host_data.interface_snmp_id,
            "ip": host_data.interface_snmp_ip,
            "dns": host_data.interface_snmp_dns,
            "port": host_data.interface_snmp_port,
            "details": {
                "version": host_data.interface_snmp_version,
                "community": host_data.interface_snmp_community
            }
        }
        try:
            response = api_zabbix.do_request(method='hostinterface.update',
                                             params=params)

        except Exception as e:
            print(e)
            print(type(e))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=format(e),
            )
        responses.append(response)
        print(response)

    for index in range(len(host_data.credentials_protocol_ids)):
        host_credential_search = session.query(
            HostCredential).filter(HostCredential.protocol_id == host_data.credentials_protocol_ids[index],
                                   HostCredential.hostid == hostid).first()
        if host_credential_search:
            host_credential_search.user = host_data.credentials_users[index]
            host_credential_search.port = host_data.credentials_ports[index]
            if host_data.credentials_password[index] != "":
                host_credential_search.password = encrypt(
                    host_data.credentials_password[index], settings.ssh_key_gen)
            session.commit()
            session.refresh(host_credential_search)
        else:
            host_credential = HostCredential(
                user=host_data.credentials_users[index],
                password=encrypt(
                    host_data.credentials_password[index], settings.ssh_key_gen),
                protocol_id=host_data.credentials_protocol_ids[index],
                port=host_data.credentials_ports[index],
                hostid=hostid
            )
            session.add(host_credential)
            session.commit()
    protocols_ids = list(map(str, host_data.credentials_protocol_ids)) if host_data.credentials_protocol_ids else [
        '0']
    protocols_ids = ",".join(protocols_ids)
    credential_ids = text(
        f"""DELETE FROM cassia_host_credentials where host_credential_id in 
        (SELECT id FROM (Select host_credential_id as id from cassia_host_credentials where protocol_id not in({protocols_ids})) as subquery)""")
    credential_ids = session.execute(credential_ids)
    session.commit()
    session.close()

    return success_response(message="Host actualizado correctamente")


async def get_host(hostid):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    try:
        api_zabbix = ZabbixAPI(settings.zabbix_server_url)
        api_zabbix.login(user=settings.zabbix_user,
                         password=settings.zabbix_password)

    except:
        session.close()
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
        "selectInventory": ["inventory_mode", "type", "name", "alias", "location_lat", "location_lon"]
    }
    response = api_zabbix.do_request(method='host.get',
                                     params=params)

    credentials = text(f"""
select chc.protocol_id ,chp.name,chc.`user`,chc.port  from cassia_host_credentials chc 
left join cassia_host_protocols chp on chp.protocol_id =chc.protocol_id 
                       where chc.hostid={hostid}""")
    credentials = pd.DataFrame(
        session.execute(credentials)).replace(np.nan, "")
    response = response['result'][0]
    response.update({'credentials': credentials.to_dict(orient="records")})
    return success_response(data=response)


def encrypt(plaintext, key):
    fernet = Fernet(key)
    return fernet.encrypt(plaintext.encode())


def decrypt(encriptedText, key):
    fernet = Fernet(key)
    return fernet.decrypt(encriptedText.encode())
