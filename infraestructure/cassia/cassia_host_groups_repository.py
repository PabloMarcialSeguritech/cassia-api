from infraestructure.database import DB
from infraestructure.db_queries_model import DBQueries
from fastapi import HTTPException, status
import pandas as pd
import numpy as np


async def get_cassia_host_group_by_name(name, db: DB) -> pd.DataFrame:
    host_groups_df = None
    try:
        query_statement_get_host_groups_by_ids = DBQueries(
        ).builder_query_statement_get_host_group_by_name(name)

        host_groups_data = await db.run_query(query_statement_get_host_groups_by_ids)
        host_groups_df = pd.DataFrame(
            host_groups_data).replace(np.nan, None)
        return host_groups_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_group_by_name: {e}")


async def get_cassia_host_groups_by_ids(groupids, db: DB) -> pd.DataFrame:
    host_groups_df = None
    try:
        query_statement_get_host_groups_by_ids = DBQueries(
        ).builder_query_statement_get_host_groups_by_ids(groupids)

        host_groups_data = await db.run_query(query_statement_get_host_groups_by_ids)
        host_groups_df = pd.DataFrame(
            host_groups_data).replace(np.nan, None)
        return host_groups_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_groups_by_ids: {e}")


async def get_host_group_by_groupid(groupid, db: DB) -> pd.DataFrame:
    host_group_df = None
    try:
        query_statement_get_cassia_host_groups = DBQueries(
        ).builder_query_statement_get_host_group_by_groupid(groupid)

        host_group_data = await db.run_query(query_statement_get_cassia_host_groups)
        host_group_df = pd.DataFrame(
            host_group_data).replace(np.nan, None)
        return host_group_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_group_by_groupid: {e}")


async def get_cassia_host_groups(db: DB) -> pd.DataFrame:
    host_groups_df = None
    try:
        query_statement_get_cassia_host_groups = DBQueries(
        ).query_statement_get_cassia_host_groups

        host_groups_data = await db.run_query(query_statement_get_cassia_host_groups)
        host_groups_df = pd.DataFrame(
            host_groups_data).replace(np.nan, None)
        return host_groups_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_cassia_host_groups: {e}")


async def asignar_group_type_cassia_groupid(db, groupid, type_id) -> bool:
    is_type_assigned = False
    try:
        query_statement_assign_type_to_groupid_cassia = DBQueries(
        ).builder_query_statement_assign_type_to_groupid_cassia(groupid, type_id)

        is_type_assigned = await db.run_query(query_statement_assign_type_to_groupid_cassia)

        return True
    except Exception as e:
        return False


async def get_groupid_relations_by_groupid(db, groupid) -> pd.DataFrame:
    groupid_relations_df = pd.DataFrame()
    try:
        query_statement_get_groupid_relations_by_groupid = DBQueries(
        ).builder_query_statement_get_groupid_relations_by_groupid(groupid)

        groupid_relations_data = await db.run_query(query_statement_get_groupid_relations_by_groupid)
        groupid_relations_df = pd.DataFrame(
            groupid_relations_data).replace(np.nan, None)
        return groupid_relations_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_groupid_relations_by_groupid: {e}")


async def get_relation_cassia_host_groups_types_by_group_id(hostgroup_id, db):
    try:
        query = DBQueries().builder_get_relation_cassia_host_groups_types_by_group_id(hostgroup_id)
        host_group_data = await db.run_query(query)
        host_group_df = pd.DataFrame(host_group_data).replace(np.nan, None)
        return host_group_df
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Error en get_host_group_by_id: {e}")


async def update_host_group_name_and_type_id(hostgroup_id, hostgroup_name, hostgroup_type_id, db):
    try:
        query = DBQueries().builder_update_host_group_name_and_id_type(
            hostgroup_id, hostgroup_name, hostgroup_type_id)
        await db.run_query(query)
        return True
    except Exception as e:
        print(f"Exception in update_host_group_name_and_type_id: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excepcion generada en update_cassia_maintenance {e}"
        )
