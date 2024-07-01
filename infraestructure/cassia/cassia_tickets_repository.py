import pandas as pd
from sqlalchemy import text
import numpy as np
from datetime import datetime
import pytz
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_tickets_async import CassiaTicketAsync
from models.cassia_tickets_async_test import CassiaTicketAsyncTest  # PINK

from fastapi import status, HTTPException


async def create_cassia_ticket(ticket_data, user_id, is_cassia_event):
    db_model = DB()
    try:
        session = await db_model.get_session()
        # PINK
        ticket = CassiaTicketAsyncTest(
            tracker_id=ticket_data.tracker_id,
            user_id=user_id,
            clock=ticket_data.clock,
            created_at=datetime.now(pytz.timezone('America/Mexico_City')),
            event_id=ticket_data.event_id,
            is_cassia_event=is_cassia_event
        )
        session.add(ticket)
        print(ticket)
        await session.commit()
        await session.refresh(ticket)
        print(ticket)
        return ticket
    except Exception as e:
        print(f"Excepcion generada en create_cassia_ticket: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en create_cassia_ticket {e}"
        )
    finally:
        await session.close()


async def delete_cassia_ticket(ticket_id):
    db_model = DB()
    try:
        # PINK
        query_delete_ticket = DBQueries(
        ).builder_query_statement_delete_cassia_ticket_by_id_test(ticket_id)
        await db_model.start_connection()
        await db_model.run_query(query_delete_ticket)
        return True
    except Exception as e:
        print(f"Excepcion generada en delete_cassia_ticket: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en delete_cassia_ticket {e}"
        )
    finally:
        await db_model.close_connection()


async def get_cassia_ticket(ticket_id):
    db_model = DB()
    try:
        # PINK
        query_get_ticket = DBQueries(
        ).builder_query_statement_get_cassia_ticket_by_id_test(ticket_id)
        await db_model.start_connection()
        ticket_data = await db_model.run_query(query_get_ticket)
        ticket_df = pd.DataFrame(ticket_data).replace(np.nan, None)
        return ticket_df
    except Exception as e:
        print(f"Excepcion generada en get_cassia_ticket: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en get_cassia_ticket {e}"
        )
    finally:
        await db_model.close_connection()
