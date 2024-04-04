import os
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.pool import NullPool
from utils.settings import Settings
import aiomysql

settings = Settings()

class DB:

    def __init__(self):
        self.host = settings.db_zabbix_host
        self.port = int(settings.db_zabbix_port)
        self.db = settings.db_zabbix_name
        self.user = settings.db_zabbix_user
        self.password = settings.db_zabbix_pass
        self.connection = None

    async def start_connection(self):
        if self.connection is None:
            try:
                self.connection = await aiomysql.connect(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    user=self.user,
                    password=self.password,
                    autocommit=True
                )
                print("Database connection successfully established.")
            except aiomysql.Error as e:
                print(f"Error while establishing database connection: {e}")
                raise

    async def run_stored_procedure(self, sp_name, sp_params):
        if self.connection is None:
            await self.start_connection()

        try:
            async with self.connection.cursor() as cursor:
                await cursor.callproc(sp_name, sp_params)
                result_sets = []
                more_results = True
                while more_results:
                    results = await cursor.fetchall()
                    if cursor.description:
                        column_names = [column[0] for column in cursor.description]
                        result_dicts = [dict(zip(column_names, row)) for row in results]
                        result_sets.extend(result_dicts)
                    more_results = await cursor.nextset()
                return result_sets
        except aiomysql.Error as error:
            print(f"Error while calling stored procedure: {error}")
            return []

    async def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
            print("Database connection closed.")

    async def run_query(self, query, params=None):
        if self.connection is None:
            await self.start_connection()
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(query, params)
                result_sets = []
                if cursor.description:
                    column_names = [column[0] for column in cursor.description]
                    results = await cursor.fetchall()
                    result_dicts = [dict(zip(column_names, row)) for row in results]
                    result_sets.extend(result_dicts)
                return result_sets
        except aiomysql.Error as error:
            print(f"Error while executing query: {error}")
            return []
