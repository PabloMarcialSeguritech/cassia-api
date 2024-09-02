from fastapi import APIRouter
from fastapi import Depends, status
from services import auth_service2
from services.cassia import diagnosta_service
from infraestructure.database import DB
from dependencies import get_db
import pandas as pd
diagnosta_router = APIRouter(prefix="/diagnosta")


@diagnosta_router.get(
    '/{hostid_or_ip}',
    tags=["Cassia - Diagnosta"],
    status_code=status.HTTP_200_OK,
    summary="Get actions catalog",
    dependencies=[Depends(auth_service2.get_current_user_session)]
)
async def analize_host(hostid_or_ip: str, db: DB = Depends(get_db)):
    print(db)
    print(id(db))
    db_connection = db
    database_response = pd.DataFrame(await db_connection.run_query("SELECT * FROM cassia_users"))
    print(database_response)
    print(db.pool)
    if db.pool is not None:
        print(f"POOL SIZE: {db.pool.size}")
    return
    return await diagnosta_service.analize_host(hostid_or_ip)
