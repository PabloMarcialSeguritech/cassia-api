from datetime import datetime
from fastapi import HTTPException,status
from infraestructure.database import DB
from infraestructure.cassia import cassia_audit_repository, cassia_users_groups_repository
from utils.traits import success_response, get_datetime_now_str_with_tz
from utils.exports_imports_functions import generate_file_export
from schemas.cassia_audit_schema import CassiaAuditSchema
from sqlalchemy.ext.asyncio import AsyncSession


async def get_user_groups(db):
    users_groups_df = None
    try:
        users_groups_df = await cassia_users_groups_repository.get_users_groups(db)
        return success_response(data=users_groups_df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener todos logs de auditoria: {e}")


async def create_user_group(user_group_data, db):
    try:

        user_group = await cassia_users_groups_repository.create_user_group(user_group_data, db)
        if user_group.id:
            return success_response(message="Registro de grupo de usuario exitoso")
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Excepción no se pudo crear el registro de grupo de usuario")

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepción en create_user_group {e}")


async def modify_user_group(user_group_id, user_group_data, db: DB):
    user_group_data_dict = user_group_data.dict()
    session: AsyncSession = await db.get_session()  # Obtener la sesión

    # Validar si los campos están presentes
    if 'name' not in user_group_data_dict:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El campo 'nombre' es requerido."
        )

    # Extraer y validar datos
    user_group_name = user_group_data_dict['name']
    user_group_description = user_group_data_dict.get('description', None)

    if not user_group_name or not isinstance(user_group_name, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El nombre del grupo de usuario debe ser una cadena de texto válida."
        )

    try:
        # Obtener el grupo de usuarios por ID
        user_group = await cassia_users_groups_repository.get_user_group_by_id(user_group_id, session)

        if user_group is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No existe el Grupo de Usuario."
            )

        # Actualizar el grupo de usuarios
        updated_user_group = await cassia_users_groups_repository.update_user_group(user_group, user_group_data, session)

        if updated_user_group:
            return success_response(
                message="Grupo de usuarios actualizado correctamente"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error al actualizar el grupo de usuarios."
            )

    except HTTPException as e:
        raise e  # Relanzar la excepción si ya es un HTTPException
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al actualizar el grupo de usuarios con ID {user_group_id}: {str(e)}"
        )
    finally:
        await session.close()  # Cerrar la sesión al final
