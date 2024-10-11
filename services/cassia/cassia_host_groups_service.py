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

async def update_host_group(group_data, db):
    group_dict = group_data.dict()
    hostgroup_id = group_dict['groupid']
    hostgroup_name = group_dict['name']
    hostgroup_type_id = group_dict['type_id']

    # Obtener el hostgroup existente
    hostgroup = await cassia_host_groups_repository.get_relation_cassia_host_groups_types_by_group_id(hostgroup_id, db)

    # Verificar si el hostgroup existe
    if not hostgroup.empty:
        # Comprobar si existe un tipo de grupo asociado
        if hostgroup['cassia_group_type_id'].isnull().all():
            is_type_assigned = await cassia_host_groups_repository.asignar_group_type_cassia_groupid(db, hostgroup_id, group_data.type_id)
            if not is_type_assigned:
                return success_response(success=False, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                        message="Error al asignar el grupo")
        # Realizar la actualización
        is_correct = await cassia_host_groups_repository.update_host_group_name_and_type_id(
                    hostgroup_id, hostgroup_name, hostgroup_type_id, db)

        if is_correct:
            return success_response(
                    message="HostGroup actualizado correctamente")
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al actualizar el HostGroup")

    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No existe el host group con ID {hostgroup_id}."
        )


async def get_host_devices(db):
    host_devices = await cassia_host_groups_repository.get_host_devices(db)
    return success_response(data=host_devices.to_dict(orient="records"))