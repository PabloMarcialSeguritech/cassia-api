from utils.settings import Settings
import pandas as pd
from utils.db import DB_Zabbix
from sqlalchemy import text
import numpy as np
from utils.traits import success_response
from models.interface_action import InterfaceAction
from models.interface_model import Interface
from models.cassia_actions import CassiaAction
from fastapi import HTTPException, status


async def get_host_by_ip(ip: str):
    db_zabbix = DB_Zabbix()
    with db_zabbix.Session() as session:
        try:
            query = text(f"""
            SELECT h.hostid ,h.name,i2.interfaceid  FROM hosts h 
        join interface i2 on i2.hostid =h.hostid 
            where h.hostid in (select DISTINCT i.hostid from interface i 
            where ip = :ip)   
            """)
            results = pd.DataFrame(session.execute(
                query, {"ip": ip})).replace(np.nan, "")

            return success_response(data=results.to_dict(orient="records"))
        except Exception as e:
            return success_response(success=False, status_code=500, message=str(e))


async def get_actions():
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    query = text(f"""
    select ca.action_id,ca.name, count(ia.interface_id) as aplicados,ca.active
from cassia_action ca
left join interface_action ia on ca.action_id =ia.action_id
where ca.is_general=0
group by ca.action_id,ca.name
    """)
    results = pd.DataFrame(session.execute(query)).replace(np.nan, "")
    session.close()
    return success_response(data=results.to_dict(orient="records"))


async def get_ci_element_relations(action_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    action = text(
        f"select action_id,name,protocol from cassia_action ca where action_id={action_id}")
    action = pd.DataFrame(session.execute(action)).replace(np.nan, "")
    if action.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Action not exists")

    relations = text(f"""
    select ia.int_act_id ,i.hostid, i.interfaceid, i.ip, h.name
from interface_action ia left join interface i on
ia.interface_id = i.interfaceid left join hosts h 
on i.hostid = h.hostid where ia.action_id ={action_id}""")
    relations = pd.DataFrame(
        session.execute(relations)).replace(np.nan, "")

    action = action.to_dict(orient='records')[0]

    response = action
    relations = {'relations': relations.to_dict(orient="records")}
    response.update(relations)

    session.close()
    return success_response(data=response)


async def create_interface_action_relation(action_id, affected_interface_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()
    action = text(
        f"select action_id from cassia_action ca where action_id={action_id}")
    action = pd.DataFrame(session.execute(action)).replace(np.nan, "")
    if action.empty:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Action not exists")
    interface = session.query(Interface).filter(
        Interface.interfaceid == affected_interface_id
    ).first()
    if not interface:
        session.close()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Interface not exists")
    interface_action = session.query(InterfaceAction).filter(
        InterfaceAction.action_id == action_id,
        InterfaceAction.interface_id == affected_interface_id
    ).first()

    if interface_action:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Relation already exists")

    interface_action_create = InterfaceAction(
        interface_id=affected_interface_id,
        action_id=action_id
    )

    session.add(interface_action_create)
    session.commit()
    session.refresh(interface_action_create)
    session.close()
    return success_response(data=interface_action_create)


async def delete_interface_action_relation(int_act_id):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    relation = session.query(InterfaceAction).filter(
        InterfaceAction.int_act_id == int_act_id
    ).first()

    if not relation:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Relation not exist")
    session.delete(relation)
    session.commit()
    session.close()
    return success_response(message="Relacion eliminada correctamente")


async def change_status(action_id, status):
    db_zabbix = DB_Zabbix()
    session = db_zabbix.Session()

    action = session.query(CassiaAction).filter(
        CassiaAction.action_id == action_id
    ).first()
    if not action:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Action not exist")
    if status:
        action.active = 1
    else:
        action.active = 0
    session.commit()
    session.refresh(action)
    return success_response(data=action, message="Acción actualizada correctamente")
