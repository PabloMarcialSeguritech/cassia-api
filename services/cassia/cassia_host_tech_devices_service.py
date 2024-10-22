from fastapi import status, HTTPException
from fastapi.responses import FileResponse
from infraestructure.cassia import cassia_host_tech_devices_repository, cassia_user_repository
from infraestructure.cassia import cassia_group_types_repository
from infraestructure.zabbix.ZabbixApi import ZabbixApi
from models.cassia_user_session import CassiaUserSession
from schemas.cassia_audit_schema import CassiaAuditSchema
from services.cassia import cassia_audit_service
from utils.actions_modules_enum import AuditModule, AuditAction
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export, get_df_by_filetype
from fastapi import File
from schemas import cassia_host_groups_schema
from infraestructure.database import DB
import asyncio
import pandas as pd
from schemas import cassia_host_device_tech_schema


async def get_technologies(db):
    host_devices = await cassia_host_tech_devices_repository.get_host_devices(db)
    if not host_devices.empty:
        host_devices['id'] = host_devices['dispId']
    return success_response(data=host_devices.to_dict(orient="records"))


async def update_technology(technology_data: cassia_host_device_tech_schema.CassiaHostDeviceTechSchema, current_user, db):
    tech_dict = technology_data.dict()
    tech_disp_id = tech_dict['dispId']
    tech_visible_name = tech_dict['visible_name']

    tech = await cassia_host_tech_devices_repository.get_technology_by_id(tech_disp_id, db)
    # Verificar si el hostgroup existe
    if not tech.empty:
        is_correct = await cassia_host_tech_devices_repository.update_host_device_tech(tech_disp_id, tech_visible_name,  db)

        if is_correct:
            await update_tech_audit_log(technology_data, tech, current_user,db)
            return success_response(
                message="Tecnología actualizada correctamente")
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al actualizar la Tecnología")

    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No existe la tecnología a actualizar."
        )


async def export_technologies_data(export_data: cassia_host_device_tech_schema.CassiaHostDeviceTechExportSchema, db: DB):
    if len(export_data.dispIds) <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"Selecciona al menos una tecnología")
    dispIds = ",".join([str(dispId) for dispId in export_data.dispIds])
    print("id de tecnologias::", dispIds)
    technologies_data = await cassia_host_tech_devices_repository.get_cassia_tech_devices_by_ids(dispIds, db)
    try:
        now = get_datetime_now_str_with_tz()
        export_file = await generate_file_export(technologies_data, page_name='grupos', filename=f"groups - {now}",
                                                 file_type=export_data.file_type)
        return export_file
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en export_groups_data {e}")


async def update_tech_audit_log(tech_data_new: cassia_host_device_tech_schema.CassiaHostDeviceTechSchema,
                                 tech_data_current,
                                 current_user: CassiaUserSession,
                                 db: DB):
    try:
        module_id = AuditModule.TECHNOLOGIES.value
        action_id = AuditAction.UPDATE.value

        # Obtener el usuario actual
        user = await cassia_user_repository.get_user_by_id(current_user.user_id, db)

        if not tech_data_current.empty:
            # Crear el detalle de la auditoría con los nombres de los tipos de grupo
            detail = (
                f"Se actualizó una tecnología. Sus datos anteriores eran: visible_name {tech_data_current.iloc[0]['visible_name']} "
                f"Sus nuevos datos son: visible_name {tech_data_new.visible_name} ")

            # Crear el objeto de auditoría
            cassia_audit_schema = CassiaAuditSchema(
                user_name=user.name,
                user_email=user.mail,
                summary=detail,
                id_audit_action=action_id,
                id_audit_module=module_id
            )

            # Crear el registro de auditoría
            await cassia_audit_service.create_audit_log(cassia_audit_schema, db)

    except Exception as e:
        print(f"Error en update_tech_audit_log: {e}")
