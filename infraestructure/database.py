import aiomysql
from sshtunnel import SSHTunnelForwarder
import asyncio
from dotenv import load_dotenv
import os

# Carga las variables de entorno desde el archivo .env
load_dotenv()


class DB:
    def __init__(self):
        useSSH = os.getenv('DB_USE_SSH')
        self.host = os.getenv('DB_HOST')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.db = os.getenv('DB_NAME')
        print("DB_USE_SSH: ", useSSH)
        self.use_ssh = False
        if useSSH == "0":
            print("database no requiere ssh tunel")
            self.use_ssh = False
        elif useSSH == "1":
            print("database requiere ssh tunel")
            self.host = 'localhost'
            self.use_ssh = True
            self.ssh_host = os.getenv('DB_SSH_HOST')  # IP del servidor SSH
            self.ssh_username = os.getenv('DB_SSH_USER')
            self.ssh_password = os.getenv('DB_SSH_PASSWORD')
        self.port = 3306
        self.remote_port = 3306  # Puerto remoto real de la base de datos
        self.pool = None
        self.ssh_tunnel = None

    async def start_ssh_tunnel(self):
        if self.use_ssh:
            self.ssh_tunnel = SSHTunnelForwarder(
                (self.ssh_host, 22),
                ssh_username=self.ssh_username,
                ssh_password=self.ssh_password,
                remote_bind_address=('127.0.0.1', self.remote_port),
                # Puedes especificar un puerto local si es necesario
                local_bind_address=('0.0.0.0', self.port)
            )
            self.ssh_tunnel.start()
            self.port = self.ssh_tunnel.local_bind_port
            print(f"SSH Tunnel established on localhost:{self.port}")

    async def start_pool(self):
        if self.pool is None:
            await self.start_ssh_tunnel()  # Establecer túnel SSH si es necesario
            try:
                self.pool = await aiomysql.create_pool(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=self.db,
                    minsize=5,  # Número mínimo de conexiones en el pool
                    maxsize=20,  # Número máximo de conexiones en el pool
                    autocommit=True,
                )
                print("Database connection pool successfully established.")
            except aiomysql.Error as e:
                print(
                    f"Error while establishing database connection pool: {e}")
                raise

    async def run_stored_procedure(self, sp_name, sp_params):
        if self.pool is None:
            await self.start_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
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

    async def run_query(self, query, params=None, display_results=False):
        if self.pool is None:
            await self.start_pool()

        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                result_sets = []
                try:
                    await cursor.execute(query, params)
                    if display_results:
                        if cursor.description:
                            results = await cursor.fetchall()
                            for row in results:
                                print(row)
                            result_sets.extend(results)
                        else:
                            print(
                                f"Query executed successfully. Rows affected: {cursor.rowcount}")
                    else:
                        if cursor.description:
                            results = await cursor.fetchall()
                            result_sets.extend(results)
                except aiomysql.Error as error:
                    print(f"Error while executing query: {error}")
                    return []

                return result_sets

    async def close_pool(self):
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
            print("Database connection pool closed.")
        if self.ssh_tunnel:
            self.ssh_tunnel.stop()
            print("SSH Tunnel closed.")
