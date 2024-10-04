from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import status, HTTPException
import pandas as pd
import numpy as np
from utils.settings import Settings
import json
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from schemas import cassia_gs_ticket_schema
from models import cassia_gs_tickets, cassia_gs_tickets_detail
from utils import traits
from datetime import datetime, timedelta
SETTINGS = Settings()
gs_connection_string = SETTINGS.gs_connection_string


async def get_last_error_tickets_pool(db):

    try:
        now = traits.get_datetime_now_with_tz()
        date = now - timedelta(hours=2)
        formatted_time = date.strftime("%Y-%m-%d %H:%M:%S")
        query_get_active_gs_tickets = DBQueries(
        ).builder_query_statement_get_last_ticket_with_error(formatted_time)
        tickets_error_data = await db.run_query(query_get_active_gs_tickets)
        tickets_error_df = pd.DataFrame(
            tickets_error_data).replace(np.nan, None)
        if tickets_error_df.empty:
            tickets_error_df = pd.DataFrame(columns=['afiliacion', 'error'])
        return tickets_error_df

    except Exception as e:
        print(f"Excepcion en get_last_error_tickets: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_last_error_tickets: {e}")


async def get_last_error_tickets():
    db_model = DB()
    try:
        now = traits.get_datetime_now_with_tz()
        date = now - timedelta(hours=2)
        formatted_time = date.strftime("%Y-%m-%d %H:%M:%S")
        query_get_active_gs_tickets = DBQueries(
        ).builder_query_statement_get_last_ticket_with_error(formatted_time)
        await db_model.start_connection()

        tickets_error_data = await db_model.run_query(query_get_active_gs_tickets)
        tickets_error_df = pd.DataFrame(
            tickets_error_data).replace(np.nan, None)
        return tickets_error_df

    except Exception as e:
        print(f"Excepcion en get_last_error_tickets: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_last_error_tickets: {e}")
    finally:
        await db_model.close_connection()


async def get_serial_numbers_by_host_ids_pool(hostids, db):

    try:
        query_statement_get_serial_numbers_by_host_ids = DBQueries(
        ).builder_query_statement_get_serial_numbers_by_host_ids(hostids)

        serial_no_data = await db.run_query(query_statement_get_serial_numbers_by_host_ids)
        serial_no_df = pd.DataFrame(serial_no_data).replace(np.nan, None)
        return serial_no_df

    except Exception as e:
        print(f"Excepcion en get_serial_numbers_by_host_ids: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_serial_numbers_by_host_ids: {e}")


async def get_serial_numbers_by_host_ids(hostids):
    db_model = DB()
    try:
        query_statement_get_serial_numbers_by_host_ids = DBQueries(
        ).builder_query_statement_get_serial_numbers_by_host_ids(hostids)
        await db_model.start_connection()

        serial_no_data = await db_model.run_query(query_statement_get_serial_numbers_by_host_ids)
        serial_no_df = pd.DataFrame(serial_no_data).replace(np.nan, None)
        return serial_no_df

    except Exception as e:
        print(f"Excepcion en get_serial_numbers_by_host_ids: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_serial_numbers_by_host_ids: {e}")
    finally:
        await db_model.close_connection()


async def get_active_tickets_pool(db):
    try:
        query_get_active_gs_tickets = DBQueries(
        ).query_get_active_gs_tickets

        tickets_data = await db.run_query(query_get_active_gs_tickets)
        tickets_df = pd.DataFrame(tickets_data).replace(np.nan, None)
        return tickets_df

    except Exception as e:
        print(f"Excepcion en get_active_tickets: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_active_tickets: {e}")


async def get_active_tickets():
    db_model = DB()
    try:
        query_get_active_gs_tickets = DBQueries(
        ).query_get_active_gs_tickets
        await db_model.start_connection()

        tickets_data = await db_model.run_query(query_get_active_gs_tickets)
        tickets_df = pd.DataFrame(tickets_data).replace(np.nan, None)
        if tickets_df.empty:
            tickets_df = pd.DataFrame(
                columns=['afiliacion', 'ticket_id', 'status'])
        return tickets_df

    except Exception as e:
        print(f"Excepcion en get_active_tickets: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_active_tickets: {e}")
    finally:
        await db_model.close_connection()


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


async def get_event_resets_by_affiliation_and_date_pool(afiliacion, date, db) -> pd.DataFrame:
    try:
        query_statement_get_resets_by_afiliation_and_date = DBQueries(
        ).builder_query_statement_get_resets_by_afiliation_and_date(afiliacion, date)
        print(query_statement_get_resets_by_afiliation_and_date)
        tickets_data = await db.run_query(query_statement_get_resets_by_afiliation_and_date)
        tickets_df = pd.DataFrame(tickets_data).replace(np.nan, None)
        return tickets_df

    except Exception as e:
        print(
            f"Excepcion en get_event_resets_by_affiliation_and_date_pool: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_event_resets_by_affiliation_and_date_pool: {e}")


async def get_event_tickets_by_affiliation_and_date_pool(afiliacion, date, db) -> pd.DataFrame:
    try:
        query_statement_get_active_tickets_by_afiliation_and_date = DBQueries(
        ).builder_query_statement_get_active_tickets_by_afiliation_and_date(afiliacion, date)

        tickets_data = await db.run_query(query_statement_get_active_tickets_by_afiliation_and_date)
        tickets_df = pd.DataFrame(tickets_data).replace(np.nan, None)
        return tickets_df

    except Exception as e:
        print(f"Excepcion en get_event_tickets_by_affiliation_and_date: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_event_tickets_by_affiliation_and_date: {e}")


async def get_event_tickets_by_affiliation_and_date(afiliacion, date) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_active_tickets_by_afiliation_and_date = DBQueries(
        ).builder_query_statement_get_active_tickets_by_afiliation_and_date(afiliacion, date)
        await db_model.start_connection()

        tickets_data = await db_model.run_query(query_statement_get_active_tickets_by_afiliation_and_date)
        tickets_df = pd.DataFrame(tickets_data).replace(np.nan, None)
        return tickets_df

    except Exception as e:
        print(f"Excepcion en get_event_tickets_by_affiliation_and_date: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_event_tickets_by_affiliation_and_date: {e}")
    finally:
        await db_model.close_connection()


async def get_active_tickets_by_hostid(hostid) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_active_tickets_by_hostid = DBQueries(
        ).builder_query_statement_get_active_tickets_by_hostid(hostid)
        await db_model.start_connection()

        tickets_data = await db_model.run_query(query_statement_get_active_tickets_by_hostid)
        tickets_df = pd.DataFrame(tickets_data)
        return tickets_df

    except Exception as e:
        print(f"Excepcion en get_active_tickets_by_hostid: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_active_tickets_by_hostid: {e}")
    finally:
        await db_model.close_connection()


async def get_active_tickets_by_afiliation_reset(afiliacion) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_active_tickets_by_afiliation_reset = DBQueries(
        ).builder_query_statement_get_active_tickets_by_afiliation_reset(afiliacion)
        await db_model.start_connection()

        tickets_data = await db_model.run_query(query_statement_get_active_tickets_by_afiliation_reset)
        tickets_df = pd.DataFrame(tickets_data)
        return tickets_df

    except Exception as e:
        print(f"Excepcion en get_active_tickets_by_afiliation_reset: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_active_tickets_by_afiliation_reset: {e}")
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


async def get_ticket_detail_by_ticket_id_pool(ticket_id, db) -> pd.DataFrame:
    try:
        query_statement_get_ticket_detail_by_ticket_id = DBQueries(
        ).builder_query_statement_get_ticket_detail_by_ticket_id(ticket_id)
        ticket_data = await db.run_query(query_statement_get_ticket_detail_by_ticket_id)
        ticket_df = pd.DataFrame(ticket_data)
        return ticket_df
    except Exception as e:
        print(f"Excepcion en get_ticket_by_ticket_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_ticket_by_ticket_id: {e}")


async def get_ticket_detail_by_ticket_id(ticket_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_ticket_detail_by_ticket_id = DBQueries(
        ).builder_query_statement_get_ticket_detail_by_ticket_id(ticket_id)
        await db_model.start_connection()

        ticket_data = await db_model.run_query(query_statement_get_ticket_detail_by_ticket_id)
        ticket_df = pd.DataFrame(ticket_data)
        return ticket_df

    except Exception as e:
        print(f"Excepcion en get_ticket_by_ticket_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_ticket_by_ticket_id: {e}")
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
    """ service_id = await get_service_id()
    if service_id.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Error al crear el ticket en SGS - Service ID no configurado en Base de Datos") """
    message_data = {
        "serviceId": host_data['service_id'],
        "location": host_data['alias'],
        "comment": comment,
        "engineer": mail,
        "serialNumber":  host_data['hardware_no_serie'],
        "macAddress": host_data['mac_address']
    }
    """ message_data = {
        "serviceId": 1849,
        "location": host_data['alias'],
        "comment": comment,
        "engineer": "engineer.cassia@seguritech.com",
        "serialNumber": host_data['hardware_no_serie']
    } """
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
    """ message_data = {
        "ticketId": ticket_data.ticket_id,
        "comment": ticket_data.comment,
        "engineer": "engineer.cassia@seguritech.com",
    } """
    message_content = json.dumps(message_data)
    subject = "ticketcommentinternalnote" if ticket_data.comment_type == "internal_note" else "ticketcommentprogresssolution"
    print(subject)
    try:
        with ServiceBusClient.from_connection_string(conn_str=gs_connection_string, logging_enable=True) as servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=queue_name)
            message = ServiceBusMessage(
                message_content, subject=subject)
            sender.send_messages(message)
            print(f"Sent: {message_content} with subject: {subject}")
            print(f"Message_id: {message.message_id}")
            print(f"correlation_id: {message.correlation_id}")
            message.subject = subject
            return message
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Error al agregar comentario al ticket en SGS")


async def save_ticket_data(ticket_data, host_data, created_ticket, user) -> pd.DataFrame:
    db_model = DB()
    try:
        session = await db_model.get_session()
        now = traits.get_datetime_now_with_tz()
        cassia_gs_ticket = cassia_gs_tickets.CassiaGSTicketsModel(
            user_id=user.user_id,
            afiliacion=host_data['alias'],
            no_serie=host_data['hardware_no_serie'],
            host_id=ticket_data.hostid,
            last_update=now,
            status="solicitado",
            message_id=created_ticket.message_id,
            requested_at=now,
            created_with_mail=None,
            user_mail=user.mail,
            service_id=host_data['service_id'],
            mac_address=host_data['mac_address']
        )
        session.add(cassia_gs_ticket)
        await session.commit()
        await session.refresh(cassia_gs_ticket)
        cassia_gs_ticket_detail = cassia_gs_tickets_detail.CassiaGSTicketsDetailModel(
            cassia_gs_tickets_id=cassia_gs_ticket.cassia_gs_tickets_id,
            ticket_id=None,
            type="ticketcommentinternalnote",
            comment=ticket_data.comment,
            status="creado",
            user_email=user.mail,
            file_url=None,
            last_update=now,
            message_id=None,
            requested_at=now,
            created_at=now,
            created_with_mail=user.mail
        )
        session.add(cassia_gs_ticket_detail)
        await session.commit()
        await session.refresh(cassia_gs_ticket_detail)
        return cassia_gs_ticket

    except Exception as e:
        print(f"Excepcion en save_ticket_data: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en save_ticket_data: {e}")
    finally:
        await session.close()


async def save_ticket_comment_data(ticket_data, created_ticket_comment, mail, created_ticket_gs_id) -> pd.DataFrame:
    db_model = DB()
    try:
        session = await db_model.get_session()
        now = traits.get_datetime_now_with_tz()
        cassia_gs_ticket_detail = cassia_gs_tickets_detail.CassiaGSTicketsDetailModel(
            cassia_gs_tickets_id=created_ticket_gs_id,
            ticket_id=ticket_data.ticket_id,
            type=created_ticket_comment.subject,
            comment=ticket_data.comment,
            status="solicitado",
            user_email=mail,
            file_url=None,
            last_update=now,
            message_id=created_ticket_comment.message_id,
            requested_at=now,
            created_at=now,
            created_with_mail=mail
        )
        session.add(cassia_gs_ticket_detail)
        await session.commit()
        await session.refresh(cassia_gs_ticket_detail)
        return cassia_gs_ticket_detail

    except Exception as e:
        print(f"Excepcion en save_ticket_comment_data: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en save_ticket_comment_data: {e}")
    finally:
        await session.close()


async def get_service_id() -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_service_id = DBQueries(
        ).query_statement_get_service_id
        await db_model.start_connection()

        service_id_data = await db_model.run_query(query_statement_get_service_id)
        service_id_data_df = pd.DataFrame(service_id_data)
        return service_id_data_df

    except Exception as e:
        print(f"Excepcion en get_service_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_service_id: {e}")
    finally:
        await db_model.close_connection()


async def get_mac_address_by_hostid(hostid) -> pd.DataFrame:
    db_model = DB()
    try:
        query_statement_get_mac_address_by_hostid = DBQueries(
        ).builder_query_statement_get_mac_address_by_hostid(hostid)
        await db_model.start_connection()

        mac_address_data = await db_model.run_query(query_statement_get_mac_address_by_hostid)
        mac_address_data_df = pd.DataFrame(mac_address_data)
        return mac_address_data_df

    except Exception as e:
        print(f"Excepcion en get_mac_address_by_hostid: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_mac_address_by_hostid: {e}")
    finally:
        await db_model.close_connection()


async def create_ticket_comment_avance_solucion(ticket_data):
    queue_name = 'cassia-gser'
    message_data = {
        "ticketId": ticket_data['ticketId'],
        "comment": ticket_data['comment'],
        "engineer": ticket_data['engineer'],
    }

    message_content = json.dumps(message_data)
    subject = "ticketcommentprogresssolution"
    print(subject)
    try:
        with ServiceBusClient.from_connection_string(conn_str=gs_connection_string, logging_enable=True) as servicebus_client:
            sender = servicebus_client.get_queue_sender(queue_name=queue_name)
            message = ServiceBusMessage(
                message_content, subject=subject)
            sender.send_messages(message)
            print(f"Sent: {message_content} with subject: {subject}")
            print(f"Message_id: {message.message_id}")
            print(f"correlation_id: {message.correlation_id}")
            message.subject = subject
            return message
    except:
        return False
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Error al agregar comentario al ticket en SGS")


async def save_ticket_comment_avance_solucion(ticket_data, created_ticket_comment, mail, created_ticket_gs_id) -> pd.DataFrame:
    db_model = DB()
    try:
        session = await db_model.get_session()
        now = traits.get_datetime_now_with_tz()
        cassia_gs_ticket_detail = cassia_gs_tickets_detail.CassiaGSTicketsDetailModel(
            cassia_gs_tickets_id=created_ticket_gs_id,
            ticket_id=ticket_data['ticketId'],
            type="ticketcommentprogresssolution",
            comment=ticket_data['comment'],
            status="solicitado",
            user_email=mail,
            file_url=None,
            last_update=now,
            message_id=created_ticket_comment.message_id,
            requested_at=now,
            created_at=now,
            created_with_mail=mail
        )
        session.add(cassia_gs_ticket_detail)
        await session.commit()
        await session.refresh(cassia_gs_ticket_detail)
        return cassia_gs_ticket_detail

    except Exception as e:
        print(f"Excepcion en save_ticket_comment_avance_solucion: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en save_ticket_comment_avance_solucion: {e}")
    finally:
        await session.close()
