from fastapi import status, HTTPException
from infraestructure.cassia import cassia_host_groups_repository
from infraestructure.cassia import cassia_group_types_repository
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from utils.traits import success_response
from schemas import cassia_host_groups_schema
from infraestructure.database import DB


async def get_host_groups(db: DB):
    host_groups = await cassia_host_groups_repository.get_cassia_host_groups(db)
    return success_response(data=host_groups.to_dict(orient="records"))


async def crate_host_group(db: DB, group_data: cassia_host_groups_schema.CassiaHostGroupSchema):
    host_group_type = await cassia_group_types_repository.get_cassia_group_type_by_id(db, group_data.type_id)
    if host_group_type.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Tipo de grupo no existente")
    try:
        # PASO 1: CREACION DE GRUPO NATIVO DE ZABBIX MEDIANTE API POR HTTP
        zabbix_api = ZabbixApi()

        params = {
            'name': group_data.name
        }
        zabbix_result = await zabbix_api.do_request(
            method="hostgroup.create", params=params)

        groupids = zabbix_result['groupids']
        # PASO 2: ASIGNACION DE TIPADO CASSIA A GRUPO NATIVO CREADO
        if len(groupids) < 1:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Error al crear grupo en Zabbix")
        groupid = groupids[0]
        is_type_assigned = await cassia_host_groups_repository.asignar_group_type_cassia_groupid(db, groupid, group_data.type_id)
        if is_type_assigned:
            return success_response(message='Grupo creado correctamente')
        else:
            return success_response(success=False, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, message="Error al asignar el grupo")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en crate_host_group: {e}")


async def delete_host_group(groupid: int, db: DB):
    group = await cassia_host_groups_repository.get_host_group_by_groupid(groupid, db)
    if group.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El host group no existe")
    groupid_relations = await cassia_host_groups_repository.get_groupid_relations_by_groupid(db, groupid)
    if not groupid_relations.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="El grupo tiene host asignados por lo que no se puede eliminar")
    try:
        zabbix_api = ZabbixApi()

        params = [groupid]

        result = await zabbix_api.do_request(
            method="hostgroup.delete", params=params)
        if result:
            return success_response(message='Grupo eliminado correctamente')
        else:
            return success_response(success=False, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, message="Error al eliminar el grupo")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en delete_host_group: {e}")
