import os
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from utils.settings import Settings
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.pool import NullPool

settings = Settings()


class DB_Auth():
    DB_NAME = settings.db_name
    DB_USER = settings.db_user
    DB_PASS = settings.db_pass
    DB_HOST = settings.db_host
    DB_PORT = settings.db_port

    connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    engine = create_engine(connection_string, echo=True)

    Session = sessionmaker(bind=engine)

    Base = declarative_base()


class DB_Prueba():
    DB_NAME = settings.dbp_name
    DB_USER = settings.dbp_user
    DB_PASS = settings.dbp_pass
    DB_HOST = settings.dbp_host
    DB_PORT = settings.dbp_port

    connection_string = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string, echo=False,
                           poolclass=NullPool, pool_recycle=1800, pool_pre_ping=True)

    Session = sessionmaker(bind=engine)

    Base = declarative_base()


class DB_Zabbix():  # Zabbix DB connection class
    DB_NAME = settings.db_zabbix_name
    DB_USER = settings.db_zabbix_user
    DB_PASS = settings.db_zabbix_pass
    DB_HOST = settings.db_zabbix_host
    DB_PORT = settings.db_zabbix_port

    server = None
    connection_string = ""

    engine = None

    Session = None

    Base = declarative_base()

    def __init__(self) -> None:

        if settings.env == "dev":
            self.server = SSHTunnelForwarder((settings.ssh_host, int(settings.ssh_port)),
                                             ssh_password=settings.ssh_pass,
                                             ssh_username=settings.ssh_user,
                                             remote_bind_address=(settings.ssh_remote_bind_address, int(
                                                 settings.ssh_remote_bind_port)),
                                             local_bind_address=("127.0.0.1", 3306))
            self.server.start()
            self.connection_string = f"mysql+mysqldb://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.server.local_bind_port}/{self.DB_NAME}"
        else:
            self.connection_string = f"mysql+mysqldb://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

        self.engine = create_engine(
            self.connection_string, echo=False, poolclass=NullPool, pool_recycle=1800)
        self.Session = sessionmaker(bind=self.engine)

    def stop(self) -> None:
        if settings.env == "dev":
            self.server.stop()


class DB_C5():
    DB_C5_NAME = settings.db_c5_name
    DB_C5_USER = settings.db_c5_user
    DB_C5_PASS = settings.db_c5_pass
    DB_C5_HOST = settings.db_c5_host
    DB_C5_PORT = settings.db_c5_port

    connection_string = f"mssql+pymssql://{DB_C5_USER}:{DB_C5_PASS}@{DB_C5_HOST}:{DB_C5_PORT}/{DB_C5_NAME}"

    engine = create_engine(connection_string, echo=False,
                           poolclass=NullPool, pool_recycle=1800, pool_pre_ping=True)

    Session = sessionmaker(bind=engine)

    Base = declarative_base()
