from infraestructure.database import DB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from models.user_model import User
from fastapi import HTTPException, status


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
