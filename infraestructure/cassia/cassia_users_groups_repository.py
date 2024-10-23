from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np
from schemas.cassia_user_group_schema import CassiaUserGroupSchema
from models.cassia_users_groups import CassiaUsersGroups
from  utils.traits import get_datetime_now_with_tz
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select

async def get_users_groups(db):
    users_groups_df = None
    try:
        query_statement_get_users_groups = DBQueries(
        ).query_statement_get_users_groups
        users_groups_data = await db.run_query(query_statement_get_users_groups)
        users_groups_df = pd.DataFrame(
            users_groups_data).replace(np.nan, None)
        return users_groups_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener la auditoria: {e}")


async def create_user_group(user_group_data: CassiaUserGroupSchema, db):
    try:
        session = await db.get_session()
        model = CassiaUsersGroups(
            name=user_group_data.name,
            description=user_group_data.description
        )
        session.add(model)
        await session.commit()
        await session.refresh(model)
        return model
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en create_user_group: {e}")


'''async def get_user_group_by_id(user_group_id, db):
    user_group_df = None
    try:
        query_statement_get_user_group = DBQueries(
        ).builder_query_statement_get_user_group_by_id(user_group_id)
        user_group_data = await db.run_query(query_statement_get_user_group)
        user_group_df = pd.DataFrame(
            user_group_data).replace(np.nan, None)
        return user_group_df
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al obtener el brand en get_brand_editable: {e}")
'''

async def get_user_group_by_id(user_group_id: int, session: AsyncSession) -> CassiaUsersGroups:
    try:
        result = await session.execute(select(CassiaUsersGroups).filter_by(id=user_group_id))
        user_group = result.scalars().first()

        # Retornar el objeto de base de datos o None si no se encuentra
        return user_group

    except Exception as e:
        raise Exception(f"Error al obtener el grupo de usuarios por ID: {str(e)}")


# Actualizar grupo de usuarios
async def update_user_group(user_group: CassiaUsersGroups, user_group_data: CassiaUserGroupSchema
                            ,session: AsyncSession) -> CassiaUsersGroups:
    try:

        user_group.name = user_group_data.name
        user_group.description = user_group_data.description
        # Confirmar los cambios en la base de datos
        await session.commit()
        await session.refresh(user_group)  # Refrescar para devolver los datos actualizados
        return user_group

    except Exception as e:
        await session.rollback()  # Revertir los cambios en caso de error
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error al actualizar el grupo de usuarios: {e}")