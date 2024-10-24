import numpy as np
import pandas as pd

from infraestructure.database import DB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from infraestructure.db_queries_model import DBQueries
from models.user_model import User
from fastapi import HTTPException, status
from utils.traits import get_datetime_now_with_tz


async def get_user_by_id(user_id: int, db: DB):
    try:
        session: AsyncSession = await db.get_session()  # Obtener la sesión asíncrona
        query = select(User).where(User.user_id == user_id)  # Crear el query para obtener el usuario
        result = await session.execute(query)  # Ejecutar la consulta
        user = result.scalars().first()  # Obtener el primer resultado

        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Usuario con ID {user_id} no encontrado"
            )

        return user
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error get_user_by_id: {e}"
        )


async def get_user_by_email(mail, db: DB):
    print("func get_user_by_email")
    user_df = pd.DataFrame()
    try:
        print(11)
        query_statement_get_user_by_email = DBQueries().builder_query_statement_get_user_by_email(mail)
        print(12)
        user = await db.run_query(query_statement_get_user_by_email)
        print(f"Resultado de la consulta (tipo {type(user)}): {user}")  # Verifica el tipo de user
        print(13)

        if user:
            # Si 'user' tiene datos, crea el DataFrame
            user_df = pd.DataFrame(user).replace(np.nan, None)
        else:
            print("No se encontró el usuario con ese correo")

        print(14)
        return user_df

    except Exception as e:
        print(f"Excepcion en get_user_by_email: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_user_by_email: {e}")


async def create_user(user, db: DB):
    try:
        session = await db.get_session()
        model = User(
            mail=user.mail,
            name=user.name,
            password=user.password,
            created_at=get_datetime_now_with_tz()
        )
        session.add(model)
        await session.commit()
        await session.refresh(model)
        return model
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en create_user: {e}")


async def get_groups_ids_by_user_id(id, db):
    print("func get_groups_ids_by_user_id")
    user_groups_df = pd.DataFrame()
    try:
        print(11)
        query_statement_get_groups_ids_by_user_id = DBQueries().builder_get_groups_ids_by_user_id(id)
        print(12)
        user_groups = await db.run_query(query_statement_get_groups_ids_by_user_id)
        print(f"Resultado de la consulta (tipo {type(user_groups)}): {user_groups}")  # Verifica el tipo de user
        print(13)

        if user_groups:
            # Si 'user' tiene datos, crea el DataFrame
            user_groups_df = pd.DataFrame(user_groups).replace(np.nan, None)
        else:
            print("No se encontró el usuario con ese correo")

        print(14)
        return user_groups_df

    except Exception as e:
        print(f"Excepcion en get_groups_ids_by_user_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_groups_ids_by_user_id: {e}")


async def link_user_groups(user_id, new_group_ids, db):
    print("func link_user_groups")
    user_groups_df = None
    try:
        print(0)
        query_statement_create_link_user_groups = DBQueries(
        ).builder_query_statement_create_link_user_groups(user_id, new_group_ids)
        await db.run_query(query_statement_create_link_user_groups)
        print(1)
        return True
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al crear la asociación de usuario con grupos de usuarios: {e}")


async def unlink_all_user_groups(user_id, db):

    try:
        if user_id > 0 and user_id is not None:
            query_statement_unlink_user_groups = DBQueries(
            ).builder_query_statement_delete_user_groups(user_id)
            await db.run_query(query_statement_unlink_user_groups)
            return True
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Identificador de Usuario es obligatorio")

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error al desasociaciar el usuario con grupos de usuarios: {e}")
