from utils.settings import Settings
from utils.db import DB_Zabbix
from models.interface_model import Interface as InterfaceModel
from pythonping import ping
from utils.traits import success_response

settings = Settings()


def create_ping(hostid):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    interface = session.query(InterfaceModel).filter(
        InterfaceModel.hostid == hostid).first()
    if interface is None:
        return success_response(message="Host no encontrado")
    else:
        result = ping(interface.ip, count=1)
        data = {
            "online": "false",
            "hostid": str(interface.hostid),
            "ip": interface.ip
        }
        if result.success():
            data['online'] = 'true'
    session.close()
    return success_response(data=data)
