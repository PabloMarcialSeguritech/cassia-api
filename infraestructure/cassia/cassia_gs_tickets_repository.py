from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import status, HTTPException
import pandas as pd
from utils.settings import Settings
import json
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from schemas import cassia_gs_ticket_schema
SETTINGS = Settings()
gs_connection_string = SETTINGS.gs_connection_string


async def get_host_data(hostid) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_host_data_gs_ticket_by_host_id = DBQueries(
        ).builder_query_statement_get_host_data_gs_ticket_by_host_id(hostid)
        await db_model.start_connection()

        host_data = await db_model.run_query(query_statement_get_host_data_gs_ticket_by_host_id)
        host_df = pd.DataFrame(host_data)
        return host_df

    except Exception as e:
        print(f"Excepcion en get_host_data: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_data: {e}")
    finally:
        await db_model.close_connection()


async def get_active_tickets_by_afiliation(afiliacion) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_active_tickets_by_afiliation = DBQueries(
        ).builder_query_statement_get_active_tickets_by_afiliation(afiliacion)
        await db_model.start_connection()

        tickets_data = await db_model.run_query(query_statement_get_active_tickets_by_afiliation)
        tickets_df = pd.DataFrame(tickets_data)
        return tickets_df

    except Exception as e:
        print(f"Excepcion en get_host_data: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_data: {e}")
    finally:
        await db_model.close_connection()


async def get_ticket_by_ticket_id(ticket_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_ticket_by_ticket_id = DBQueries(
        ).builder_query_statement_get_ticket_by_ticket_id(ticket_id)
        await db_model.start_connection()

        ticket_data = await db_model.run_query(query_statement_get_ticket_by_ticket_id)
        ticket_df = pd.DataFrame(ticket_data)
        return ticket_df

    except Exception as e:
        print(f"Excepcion en get_ticket_by_ticket_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_ticket_by_ticket_id: {e}")
    finally:
        await db_model.close_connection()


async def create_ticket(host_data, comment, mail):
    queue_name = 'cassia-gser'
    message_data = {
        "serviceId": 3738,
        "location": host_data['alias'],
        "comment": comment,
        "engineer": mail,
        "serialNumber": host_data['hardware_no_serie']
    }
    message_content = json.dumps(message_data)
    try:
        with ServiceBusClient.from_connection_string(conn_str=gs_connection_string, logging_enable=True) as servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=queue_name)
            message = ServiceBusMessage(
                message_content, subject="createticket")
            sender.send_messages(message)
            print(f"Sent: {message_content} with subject: createticket")
            print(f"Message_id: {message.message_id}")
            print(f"correlation_id: {message.correlation_id}")
            return message
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Error al crear el ticket en SGS")


async def create_ticket_comment(ticket_data: cassia_gs_ticket_schema.CassiaGSTicketCommentSchema, mail):
    queue_name = 'cassia-gser'
    message_data = {
        "ticketId": ticket_data.ticket_id,
        "comment": ticket_data.comment,
        "engineer": mail,
    }
    message_content = json.dumps(message_data)
    subject = "ticketcommentinternalnote" if ticket_data.comment_type == "internal_note" else "ticketcommentprogresssolution"
    try:
        with ServiceBusClient.from_connection_string(conn_str=gs_connection_string, logging_enable=True) as servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=queue_name)
            message = ServiceBusMessage(
                message_content, subject="subject")
            sender.send_messages(message)
            print(f"Sent: {message_content} with subject: createticket")
            print(f"Message_id: {message.message_id}")
            print(f"correlation_id: {message.correlation_id}")
            return message
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Error al agregar comentario al ticket en SGS")
