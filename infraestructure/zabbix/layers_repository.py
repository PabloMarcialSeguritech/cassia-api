from fastapi import HTTPException, status
from infraestructure.database_model import DB
from infraestructure.db_queries_model import DBQueries
import pandas as pd
import numpy as np


async def get_towers_pool(db) -> pd.DataFrame:
    try:
        sp_get_towers = DBQueries().stored_name_get_towers
        towers_data = await db.run_stored_procedure(sp_get_towers, ())
        towers_df = pd.DataFrame(towers_data).replace(np.nan, None)
        return towers_df
    except Exception as e:
        print(f"Excepcion en get_towers: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_towers: {e}")


async def get_towers() -> pd.DataFrame:
    db_model = DB()
    try:
        sp_get_towers = DBQueries().stored_name_get_towers
        await db_model.start_connection()
        towers_data = await db_model.run_stored_procedure(sp_get_towers, ())
        towers_df = pd.DataFrame(towers_data).replace(np.nan, None)
        return towers_df
    except Exception as e:
        print(f"Excepcion en get_towers: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_towers: {e}")
    finally:
        await db_model.close_connection()


async def get_host_down_excepciones_pool(db) -> pd.DataFrame:

    try:
        sp_get_host_downs_excepciones = DBQueries().stored_name_get_host_down_excepcion

        host_downs_excepciones_data = await db.run_query(sp_get_host_downs_excepciones)
        host_downs_excepciones_df = pd.DataFrame(
            host_downs_excepciones_data).replace(np.nan, None)
        if host_downs_excepciones_df.empty:
            host_downs_excepciones_df = pd.DataFrame(columns=['hostid'])
        return host_downs_excepciones_df
    except Exception as e:
        print(f"Excepcion en get_host_down_excepciones_pool: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_down_excepciones_pool: {e}")


async def get_host_down_excepciones_pool(db) -> pd.DataFrame:
    try:
        sp_get_host_downs_excepciones = DBQueries().stored_name_get_host_down_excepcion
        host_downs_excepciones_data = await db.run_query(sp_get_host_downs_excepciones)
        host_downs_excepciones_df = pd.DataFrame(
            host_downs_excepciones_data).replace(np.nan, None)
        if host_downs_excepciones_df.empty:
            host_downs_excepciones_df = pd.DataFrame(columns=['hostid'])
        return host_downs_excepciones_df
    except Exception as e:
        print(f"Excepcion en get_host_down_excepciones: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_down_excepciones: {e}")


async def get_host_down_excepciones() -> pd.DataFrame:
    db_model = DB()
    try:
        sp_get_host_downs_excepciones = DBQueries().stored_name_get_host_down_excepcion
        await db_model.start_connection()
        host_downs_excepciones_data = await db_model.run_query(sp_get_host_downs_excepciones)
        host_downs_excepciones_df = pd.DataFrame(
            host_downs_excepciones_data).replace(np.nan, None)
        if host_downs_excepciones_df.empty:
            host_downs_excepciones_df = pd.DataFrame(columns=['hostid'])
        return host_downs_excepciones_df
    except Exception as e:
        print(f"Excepcion en get_host_down_excepciones: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_down_excepciones: {e}")
    finally:
        await db_model.close_connection()


async def get_host_up_pool(municipality_id, dispId, subtype_id, db) -> pd.DataFrame:

    try:
        sp_get_host_up = DBQueries().stored_name_get_host_up
        host_up_data = await db.run_stored_procedure(sp_get_host_up, (municipality_id, dispId, ''))
        host_up_df = pd.DataFrame(host_up_data).replace(np.nan, None)
        if host_up_df.empty:
            host_up_df = pd.DataFrame(columns=['hostid'])
        return host_up_df
    except Exception as e:
        print(f"Excepcion en get_host_up: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_up: {e}")


async def get_host_up(municipality_id, dispId, subtype_id) -> pd.DataFrame:
    db_model = DB()
    try:
        sp_get_host_up = DBQueries().stored_name_get_host_up
        await db_model.start_connection()
        host_up_data = await db_model.run_stored_procedure(sp_get_host_up, (municipality_id, dispId, ''))
        host_up_df = pd.DataFrame(host_up_data).replace(np.nan, None)
        if host_up_df.empty:
            host_up_df = pd.DataFrame(columns=['hostid'])
        return host_up_df
    except Exception as e:
        print(f"Excepcion en get_host_up: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_up: {e}")
    finally:
        await db_model.close_connection()


async def get_host_downs_pool(municipality_id, dispId, subtype_id, db) -> pd.DataFrame:
    try:
        sp_get_host_downs = DBQueries().stored_name_get_host_downs

        host_downs_data = await db.run_stored_procedure(sp_get_host_downs, (municipality_id, dispId, ''))
        host_downs_df = pd.DataFrame(host_downs_data).replace(np.nan, None)
        if host_downs_df.empty:
            host_downs_df = pd.DataFrame(columns=['hostid'])
        return host_downs_df
    except Exception as e:
        print(f"Excepcion en get_host_downs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_downs: {e}")


async def get_host_downs_pool(municipality_id, dispId, subtype_id, db) -> pd.DataFrame:
    try:
        sp_get_host_downs = DBQueries().stored_name_get_host_downs

        host_downs_data = await db.run_stored_procedure(sp_get_host_downs, (municipality_id, dispId, ''))
        host_downs_df = pd.DataFrame(host_downs_data).replace(np.nan, None)
        if host_downs_df.empty:
            host_downs_df = pd.DataFrame(columns=['hostid'])
        return host_downs_df
    except Exception as e:
        print(f"Excepcion en get_host_downs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_downs: {e}")


async def get_host_downs(municipality_id, dispId, subtype_id) -> pd.DataFrame:
    db_model = DB()
    try:
        sp_get_host_downs = DBQueries().stored_name_get_host_downs
        await db_model.start_connection()
        host_downs_data = await db_model.run_stored_procedure(sp_get_host_downs, (municipality_id, dispId, ''))
        host_downs_df = pd.DataFrame(host_downs_data).replace(np.nan, None)
        if host_downs_df.empty:
            host_downs_df = pd.DataFrame(columns=['hostid'])
        return host_downs_df
    except Exception as e:
        print(f"Excepcion en get_host_downs: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_downs: {e}")
    finally:
        await db_model.close_connection()


async def get_host_downs_dependientes_pool(db) -> pd.DataFrame:
    try:
        # PINK
        query_get_host_downs_dependientes = DBQueries(
        ).query_get_host_downs_dependientes_test

        host_downs_dependientes_data = await db.run_query(query_get_host_downs_dependientes)
        host_downs_dependientes_df = pd.DataFrame(
            host_downs_dependientes_data).replace(np.nan, None)
        return host_downs_dependientes_df
    except Exception as e:
        print(f"Excepcion en get_host_downs_dependientes_pool: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_downs_dependientes_pool: {e}")


async def get_host_downs_dependientes() -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        query_get_host_downs_dependientes = DBQueries(
        ).query_get_host_downs_dependientes_test
        await db_model.start_connection()
        host_downs_dependientes_data = await db_model.run_query(query_get_host_downs_dependientes)
        host_downs_dependientes_df = pd.DataFrame(
            host_downs_dependientes_data).replace(np.nan, None)
        return host_downs_dependientes_df
    except Exception as e:
        print(f"Excepcion en get_host_downs_dependientes: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_downs_dependientes: {e}")
    finally:
        await db_model.close_connection()


async def get_host_downs_dependientes_filtro_pool(municipality_id, dispId, db) -> pd.DataFrame:

    try:
        # PINK
        sp_get_host_dependientes_filtro = DBQueries(
        ).stored_name_get_dependents_diagnostic_problems_test  # ACTUALIZAR NOMBRE
        host_downs_dependientes_data = await db.run_stored_procedure(sp_get_host_dependientes_filtro, (municipality_id, dispId))
        host_downs_dependientes_df = pd.DataFrame(
            host_downs_dependientes_data).replace(np.nan, None)
        return host_downs_dependientes_df
    except Exception as e:
        print(f"Excepcion en get_host_downs_dependientes_filtro: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_downs_dependientes_filtro: {e}")


async def get_host_downs_dependientes_filtro(municipality_id, dispId) -> pd.DataFrame:
    db_model = DB()
    try:
        # PINK
        sp_get_host_dependientes_filtro = DBQueries(
        ).stored_name_get_dependents_diagnostic_problems_test  # ACTUALIZAR NOMBRE
        await db_model.start_connection()
        host_downs_dependientes_data = await db_model.run_stored_procedure(sp_get_host_dependientes_filtro, (municipality_id, dispId))
        host_downs_dependientes_df = pd.DataFrame(
            host_downs_dependientes_data).replace(np.nan, None)
        return host_downs_dependientes_df
    except Exception as e:
        print(f"Excepcion en get_host_downs_dependientes_filtro: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_host_downs_dependientes_filtro: {e}")
    finally:
        await db_model.close_connection()


async def get_rfid_readings_by_municipality_pool(municipality_name, db) -> pd.DataFrame:
    try:
        query_get_rfid_readings_by_municipality = DBQueries(
        ).builder_query_statement_get_rfid_readings_by_municipality_name(municipality_name)

        rfid_readings_data = await db.run_query(query_get_rfid_readings_by_municipality)
        rfid_readings_df = pd.DataFrame(
            rfid_readings_data).replace(np.nan, None)
        return rfid_readings_df
    except Exception as e:
        print(f"Excepcion en get_rfid_readings_by_municipality: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_rfid_readings_by_municipality: {e}")


async def get_rfid_readings_by_municipality(municipality_name) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_rfid_readings_by_municipality = DBQueries(
        ).builder_query_statement_get_rfid_readings_by_municipality_name(municipality_name)
        await db_model.start_connection()
        rfid_readings_data = await db_model.run_query(query_get_rfid_readings_by_municipality)
        rfid_readings_df = pd.DataFrame(
            rfid_readings_data).replace(np.nan, None)
        return rfid_readings_df
    except Exception as e:
        print(f"Excepcion en get_rfid_readings_by_municipality: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_rfid_readings_by_municipality: {e}")
    finally:
        await db_model.close_connection()


async def get_rfid_readings_global_pool(db) -> pd.DataFrame:
    try:
        query_get_rfid_readings_global = DBQueries(
        ).query_statement_get_rfid_readings_global
        rfid_readings_data = await db.run_query(query_get_rfid_readings_global)
        rfid_readings_df = pd.DataFrame(
            rfid_readings_data).replace(np.nan, None)
        return rfid_readings_df
    except Exception as e:
        print(f"Excepcion en get_rfid_readings_global: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_rfid_readings_global: {e}")


async def get_rfid_readings_global() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_rfid_readings_global = DBQueries(
        ).query_statement_get_rfid_readings_global
        await db_model.start_connection()
        rfid_readings_data = await db_model.run_query(query_get_rfid_readings_global)
        rfid_readings_df = pd.DataFrame(
            rfid_readings_data).replace(np.nan, None)
        return rfid_readings_df
    except Exception as e:
        print(f"Excepcion en get_rfid_readings_global: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_rfid_readings_global: {e}")
    finally:
        await db_model.close_connection()


async def get_rfid_host_active_pool(db) -> pd.DataFrame:
    try:
        query_get_rfid_host_active = DBQueries(
        ).query_get_rfid_acitve
        rfid_host_active_data = await db.run_query(query_get_rfid_host_active)
        rfid_host_active_df = pd.DataFrame(
            rfid_host_active_data).replace(np.nan, None)
        return rfid_host_active_df
    except Exception as e:
        print(f"Excepcion en get_rfid_host_active: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_rfid_host_active: {e}")


async def get_rfid_host_active() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_rfid_host_active = DBQueries(
        ).query_get_rfid_acitve
        await db_model.start_connection()
        rfid_host_active_data = await db_model.run_query(query_get_rfid_host_active)
        rfid_host_active_df = pd.DataFrame(
            rfid_host_active_data).replace(np.nan, None)
        return rfid_host_active_df
    except Exception as e:
        print(f"Excepcion en get_rfid_host_active: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_rfid_host_active: {e}")
    finally:
        await db_model.close_connection()


async def get_max_serverities_by_tech_pool(tech_id, db) -> pd.DataFrame:
    try:
        query_get_max_severities_by_tech = DBQueries(
        ).builder_query_statement_get_max_severities_by_tech(tech_id)

        max_severities_data = await db.run_query(query_get_max_severities_by_tech)
        max_severities_df = pd.DataFrame(
            max_severities_data).replace(np.nan, None)
        return max_severities_df
    except Exception as e:
        print(f"Excepcion en get_max_serverities_by_tech: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_max_serverities_by_tech: {e}")


async def get_max_serverities_by_tech(tech_id) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_max_severities_by_tech = DBQueries(
        ).builder_query_statement_get_max_severities_by_tech(tech_id)
        await db_model.start_connection()
        max_severities_data = await db_model.run_query(query_get_max_severities_by_tech)
        max_severities_df = pd.DataFrame(
            max_severities_data).replace(np.nan, None)
        return max_severities_df
    except Exception as e:
        print(f"Excepcion en get_max_serverities_by_tech: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_max_serverities_by_tech: {e}")
    finally:
        await db_model.close_connection()


async def get_lpr_readings_by_municipality_pool(municipality_name, db) -> pd.DataFrame:

    try:
        query_get_lpr_readings_by_municipality = DBQueries(
        ).builder_query_statement_get_lpr_readings_by_municipality_name(municipality_name)

        lpr_readings_data = await db.run_query(query_get_lpr_readings_by_municipality)
        lpr_readings_df = pd.DataFrame(
            lpr_readings_data).replace(np.nan, None)
        return lpr_readings_df
    except Exception as e:
        print(f"Excepcion en get_lpr_readings_by_municipality: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_lpr_readings_by_municipality: {e}")


async def get_lpr_readings_by_municipality(municipality_name) -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_lpr_readings_by_municipality = DBQueries(
        ).builder_query_statement_get_lpr_readings_by_municipality_name(municipality_name)
        await db_model.start_connection()
        lpr_readings_data = await db_model.run_query(query_get_lpr_readings_by_municipality)
        lpr_readings_df = pd.DataFrame(
            lpr_readings_data).replace(np.nan, None)
        return lpr_readings_df
    except Exception as e:
        print(f"Excepcion en get_lpr_readings_by_municipality: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_lpr_readings_by_municipality: {e}")
    finally:
        await db_model.close_connection()


async def get_lpr_readings_global_pool(db) -> pd.DataFrame:
    try:
        query_get_lpr_readings_global = DBQueries(
        ).query_statement_get_lpr_readings_global

        lpr_readings_data = await db.run_query(query_get_lpr_readings_global)
        lpr_readings_df = pd.DataFrame(
            lpr_readings_data).replace(np.nan, None)
        return lpr_readings_df
    except Exception as e:
        print(f"Excepcion en get_lpr_readings_global: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_lpr_readings_global: {e}")


async def get_lpr_readings_global() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_lpr_readings_global = DBQueries(
        ).query_statement_get_lpr_readings_global
        await db_model.start_connection()
        lpr_readings_data = await db_model.run_query(query_get_lpr_readings_global)
        lpr_readings_df = pd.DataFrame(
            lpr_readings_data).replace(np.nan, None)
        return lpr_readings_df
    except Exception as e:
        print(f"Excepcion en get_lpr_readings_global: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_lpr_readings_global: {e}")
    finally:
        await db_model.close_connection()


async def get_lpr_host_active_pool(db) -> pd.DataFrame:
    try:
        query_get_lpr_host_active = DBQueries(
        ).query_get_lpr_acitve

        lpr_host_active_data = await db.run_query(query_get_lpr_host_active)
        lpr_host_active_df = pd.DataFrame(
            lpr_host_active_data).replace(np.nan, None)
        return lpr_host_active_df
    except Exception as e:
        print(f"Excepcion en get_lpr_host_active: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_lpr_host_active: {e}")


async def get_lpr_host_active() -> pd.DataFrame:
    db_model = DB()
    try:
        query_get_lpr_host_active = DBQueries(
        ).query_get_lpr_acitve
        await db_model.start_connection()
        lpr_host_active_data = await db_model.run_query(query_get_lpr_host_active)
        lpr_host_active_df = pd.DataFrame(
            lpr_host_active_data).replace(np.nan, None)
        return lpr_host_active_df
    except Exception as e:
        print(f"Excepcion en get_lpr_host_active: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_lpr_host_active: {e}")
    finally:
        await db_model.close_connection()


async def get_switch_connectivity_layer(municipality_id) -> pd.DataFrame:
    db_model = DB()
    try:
        sp_get_switch_connectivity = DBQueries(
        ).stored_name_switch_connectivity
        await db_model.start_connection()
        switch_connectivity_data = await db_model.run_stored_procedure(sp_get_switch_connectivity, (municipality_id,))
        switch_connectivity_df = pd.DataFrame(
            switch_connectivity_data).replace(np.nan, None)
        return switch_connectivity_df
    except Exception as e:
        print(f"Excepcion en get_switch_connectivity_layer: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Excepcion en get_switch_connectivity_layer: {e}")
    finally:
        await db_model.close_connection()
