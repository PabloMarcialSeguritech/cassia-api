from fastapi import status, HTTPException
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
from models.cassia_ci_document1 import CassiaCIDocument1
import pandas as pd
import numpy as np
import os
import ntpath
import shutil
from datetime import datetime


async def get_ci_element(element_id):
    db_model = DB()
    try:
        query_get_ci_element = DBQueries(
        ).builder_query_statement_get_ci_element_by_id(element_id)
        await db_model.start_connection()

        ci_element_data = await db_model.run_query(query_get_ci_element)
        ci_element_df = pd.DataFrame(ci_element_data)
        return ci_element_df

    except Exception as e:
        print(f"Excepcion en get_ci_element: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_ci_element: {e}")
    finally:
        await db_model.close_connection()


async def get_ci_element_docs(element_id):
    db_model = DB()
    try:
        query_get_ci_element_docs = DBQueries(
        ).builder_query_statement_get_ci_element_docs_by_id(element_id)
        await db_model.start_connection()

        ci_element_docs_data = await db_model.run_query(query_get_ci_element_docs)
        ci_element_docs_df = pd.DataFrame(ci_element_docs_data)
        return ci_element_docs_df

    except Exception as e:
        print(f"Excepcion en get_ci_element_docs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_ci_element_docs: {e}")
    finally:
        await db_model.close_connection()


async def get_ci_element_doc_by_id(doc_id):
    db_model = DB()
    try:
        query_get_ci_element_doc = DBQueries(
        ).builder_query_statement_get_ci_element_doc_by_id(doc_id)
        await db_model.start_connection()

        ci_element_doc_data = await db_model.run_query(query_get_ci_element_doc)
        ci_element_doc_df = pd.DataFrame(ci_element_doc_data)
        return ci_element_doc_df

    except Exception as e:
        print(f"Excepcion en get_ci_element_doc_by_id: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_ci_element_doc_by_id: {e}")
    finally:
        await db_model.close_connection()


async def save_document(element_id, item, is_file):
    filename = ""
    path = ""
    if is_file:
        upload_dir = os.path.join(
            os.getcwd(), f"uploads/cis/{element_id}")
        # Create the upload directory if it doesn't exist
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)
        # get the destination path
        timestamp = str(datetime.now().timestamp()).replace(".", "")
        file_dest = os.path.join(
            upload_dir, f"{timestamp}{item.filename}")
        print(file_dest)
        # copy the file contents
        with open(file_dest, "wb") as buffer:
            shutil.copyfileobj(item.file, buffer)
        path = file_dest
        filename = item.filename

    else:
        filename = item
        path = item

    db_model = DB()
    try:
        session = await db_model.get_session()
        ci_document = CassiaCIDocument1(
            element_id=element_id,
            path=path,
            filename=filename,
            is_link=0 if is_file else 1
        )
        session.add(ci_document)
        await session.commit()

        return True

    except Exception as e:
        print(f"Excepcion en save_document: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en save_document: {e}")
    finally:
        await session.close()


async def delete_document(doc_df):
    db_model = DB()

    try:
        path = doc_df['path'][0]
        if os.path.exists(path):
            os.remove(path)
        query_delete_ci_element_doc = DBQueries(
        ).builder_query_statement_delete_ci_element_doc_by_id(doc_df['doc_id'][0])
        await db_model.start_connection()
        await db_model.run_query(query_delete_ci_element_doc)
        return True

    except Exception as e:
        print(f"Excepcion en delete_document: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en delete_document: {e}")
    finally:
        await db_model.close_connection()
