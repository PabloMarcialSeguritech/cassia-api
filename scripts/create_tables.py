from utils.db import DB_Auth
from models.user_model import User
from services.auth_service import get_password_hash


def create_tables():
    DB_Auth.Base.metadata.create_all(bind=DB_Auth.engine)
    db_user = User(
        username="admin",
        email="admin@gmail.com",
        password=get_password_hash("12345678"),
    )
    db = DB_Auth.Session()
    db.add(db_user)
    db.commit()
