# dependencies.py
from fastapi import Request
from infraestructure.database import DB


def get_db(request: Request) -> DB:
    db = request.app.state.db
    if not db:
        raise RuntimeError("DB instance not found in app.state")
    return db
