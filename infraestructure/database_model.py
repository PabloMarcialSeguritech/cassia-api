import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.pool import NullPool
from utils.settings import Settings
import aiomysql

settings = Settings()


class DB:
    Base = declarative_base()

    def __init__(self):
        self.host = settings.db_zabbix_host
        self.port = int(settings.db_zabbix_port)
        self.db = settings.db_zabbix_name
        self.user = settings.db_zabbix_user
        self.password = settings.db_zabbix_pass

        self.connection = None
        self.DATABASE_URL = f"mysql+aiomysql://{self.user}:{self.password}@{self.host}/{self.db}"

        """ if settings.env == "dev":
            self.server = SSHTunnelForwarder((settings.ssh_host, int(settings.ssh_port)),
                                             ssh_password=settings.ssh_pass,
                                             ssh_username=settings.ssh_user,
                                             remote_bind_address=(settings.ssh_remote_bind_address, int(
                                                 settings.ssh_remote_bind_port)),
                                             local_bind_address=("127.0.0.1", 3306))
            self.server.start()
            self.connection_string = f"mysql+aiomysql://{self.user}:{self.password}@{self.host}:{self.server.local_bind_port}/{self.db}"
        else:
            self.connection_string = f"mysql+aiomysql://{self.user}:{self.password}@{self.host}/{self.db}"
         """
        self.engine = create_async_engine(self.DATABASE_URL, echo=False)

    async def get_session(self):
        async with AsyncSession(self.engine) as session:
            return session

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
                        column_names = [column[0]
                                        for column in cursor.description]
                        result_dicts = [dict(zip(column_names, row))
                                        for row in results]
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
                    result_dicts = [dict(zip(column_names, row))
                                    for row in results]
                    result_sets.extend(result_dicts)
                return result_sets
        except aiomysql.Error as error:
            print(f"Error while executing query: {error}")
            return []

    async def run_statement_query(self, query):
        if self.connection is None:
            await self.start_connection()
        try:
            async with self.connection.cursor() as cursor:
                print("aa")
                await cursor.execute(query)
                print("bb")
                await self.connection.commit()
                print("cc")
                return True
        except aiomysql.Error as error:
            print(f"Error while executing query: {error}")
            return False
