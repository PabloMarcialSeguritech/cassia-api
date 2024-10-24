from schemas import discovered_device_schema
from infraestructure.database import DB
from fastapi import HTTPException
from typing import List
import paramiko
from scapy.all import ARP, Ether, srp
import ipaddress
import csv
import io
from fastapi.responses import StreamingResponse
from utils.traits import success_response
import json
# Diccionario de proxies predefinidos
PROXIES = {
    1: {"id": 2, "name": "Server-gto", "hostname": "172.18.200.17", "username": "zabbix", "password": "123qwe...", "port": 22},
    10436: {"id": 3, "name": "prx-silao", "hostname": "172.18.37.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10445: {"id": 4, "name": "prx-abasolo", "hostname": "172.18.1.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10446: {"id": 5, "name": "prx-acambaro", "hostname": "172.18.2.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10447: {"id": 6, "name": "prx-apaseoelalto", "hostname": "172.18.3.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10448: {"id": 7, "name": "prx-apaseoelgrande", "hostname": "172.18.4.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10449: {"id": 8, "name": "prx-celaya", "hostname": "172.18.6.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10450: {"id": 9, "name": "prx-cortazar", "hostname": "172.18.10.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10451: {"id": 10, "name": "prx-doloreshidalgo", "hostname": "172.18.13.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10452: {"id": 11, "name": "prx-guanajuato", "hostname": "172.18.14.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10453: {"id": 12, "name": "prx-irapuato", "hostname": "172.18.16.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10454: {"id": 13, "name": "prx-leon", "hostname": "172.18.19.214", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10455: {"id": 14, "name": "prx-manueldoblado", "hostname": "172.18.7.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10456: {"id": 15, "name": "prx-penjamo", "hostname": "172.18.22.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10457: {"id": 16, "name": "prx-purisima", "hostname": "172.18.24.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10458: {"id": 17, "name": "prx-salamanca", "hostname": "172.18.26.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10459: {"id": 18, "name": "prx-sanfelipe", "hostname": "172.18.29.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10460: {"id": 19, "name": "prx-sanfrancisco", "hostname": "172.18.30.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10461: {"id": 20, "name": "prx-sanluis", "hostname": "172.18.32.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10462: {"id": 21, "name": "prx-sanmiguel", "hostname": "172.18.33.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10463: {"id": 22, "name": "prx-valle", "hostname": "172.18.42.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10512: {"id": 23, "name": "prx-moroleon", "hostname": "172.18.20.46", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10513: {"id": 24, "name": "prx-salvatierra", "hostname": "172.18.27.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    10515: {"id": 25, "name": "prx-c5i", "hostname": "172.18.200.106", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22},
    23157: {"id": 26, "name": "prx-yuriria", "hostname": "172.18.45.62", "username": "zabbix", "password": "Z4bb1x.gt0", "port": 22}
}


def is_valid_ip(ip: str) -> bool:
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False


async def get_discovered_devices(proxies: discovered_device_schema.ProxyRequest, db: DB):
    discovered_devices = []
    errors = []
    for req in proxies:
        if req.proxyId is None:
            devices = await discover_locally(req.segment)
        elif req.proxyId in PROXIES:
            proxy = PROXIES[req.proxyId]
            result_via_ssh = await discover_via_ssh(proxy, req.segment)
            devices = result_via_ssh['devices']
            if result_via_ssh['errors'] is not None:
                errors.append(
                    {'proxy': proxy['name'], 'proxy_ip': proxy['hostname'], 'errors': result_via_ssh['errors']})
        else:
            raise HTTPException(
                status_code=400, detail=f"ProxyId {req.proxyId} no encontrado.")
        discovered_devices.extend(devices)
    response = {'devices': discovered_devices,
                'errors': errors}
    return success_response(data=response)


async def download_discovery_devices(proxies: discovered_device_schema.ProxyRequest, db: DB):
    json_response_devices = await get_discovered_devices(proxies, db)
    response_body = json_response_devices.body
    response_data = json.loads(response_body)
    print(response_data)
    type(response_data)

    devices = response_data['data']['devices']

    # Crear el archivo CSV en memoria
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ip", "mac_address", "proxyId",
                    "proxyName", "name", "host"])

    for device in devices:
        print(device)
        writer.writerow([device['ip'], device['mac_address'],
                        device['proxyId'], device['proxyName'], device['name'], device['host']])

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=discovered_devices.csv"}
    )
    pass
    # Función para descubrimiento local


async def discover_locally(segment: str) -> List[discovered_device_schema.DiscoveredDevice]:
    print(f"Iniciando escaneo local en segmento: {segment}")
    devices = []
    arp_request = ARP(pdst=segment)
    broadcast = Ether(dst="ff:ff:ff:ff:ff:ff")
    packet = broadcast / arp_request
    answered, _ = srp(packet, timeout=2, verbose=False)

    for sent, received in answered:
        if is_valid_ip(received.psrc):
            name = f"{received.psrc}_{received.hwsrc}".replace(
                '.', "_").replace(":", "_")
            devices.append(discovered_device_schema.DiscoveredDevice(
                ip=received.psrc,
                mac_address=received.hwsrc,
                proxyId=None,
                proxyName="Local",
                name=name,
                host=name
            ))
    print(f"Dispositivos encontrados: {devices}")
    return devices

# Función para descubrimiento remoto vía SSH


async def discover_via_ssh(proxy: dict, segment: str) -> List[discovered_device_schema.DiscoveredDevice]:
    print(
        f"Iniciando conexión SSH a {proxy['hostname']} con usuario {proxy['username']}")
    devices = []
    result = {
        'proxy': proxy["hostname"],
        'devices': [],
        'errors': None
    }
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("CREDENCIALES")
        print(f"hostname: {proxy['hostname']}")
        print(f"username: {proxy['username']}")
        print(f"password: {proxy['password']}")
        print(f"port: {proxy['port']}")

        ssh.connect(
            hostname=proxy["hostname"],
            username=proxy["username"],
            password=proxy["password"],
            port=proxy["port"]
        )
        print(f"Conexión SSH exitosa a {proxy['hostname']}")

        command = f"echo {proxy['password']} | sudo -S arp-scan {segment}"
        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode().strip()
        error_output = stderr.read().decode().strip()

        if error_output:
            print(f"Errores: {error_output}")
            result['errors'] = error_output

        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 2 and is_valid_ip(parts[0]):
                ip, mac = parts[0], parts[1]
                name = f"{ip}_{mac}".replace(
                    '.', "_").replace(":", "_")
                devices.append(discovered_device_schema.DiscoveredDevice(
                    ip=ip,
                    mac_address=mac,
                    proxyId=proxy["id"],
                    proxyName=proxy["name"],
                    name=f"{ip}{mac}",
                    host=f"{ip}{mac}"
                ))

    except Exception as e:
        result['errors'] = str(e)
        return result
        print(f"Error en proxy {proxy['hostname']}: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error en proxy {proxy['hostname']}: {str(e)}")
    finally:
        ssh.close()
    return result
