import os

from pydantic import BaseSettings
from dotenv import load_dotenv
load_dotenv()


class Settings(BaseSettings):
    dbp_name: str = os.getenv("DBP_NAME")
    dbp_user: str = os.getenv('DBP_USER')
    dbp_pass: str = os.getenv('DBP_PASS')
    dbp_host: str = os.getenv('DBP_HOST')
    dbp_port: str = os.getenv('DBP_PORT')

    db_name: str = os.getenv("DB_NAME")
    db_user: str = os.getenv('DB_USER')
    db_pass: str = os.getenv('DB_PASS')
    db_host: str = os.getenv('DB_HOST')
    db_port: str = os.getenv('DB_PORT')

    db_zabbix_name: str = os.getenv("DB_ZABBIX_NAME")
    db_zabbix_user: str = os.getenv('DB_ZABBIX_USER')
    db_zabbix_pass: str = os.getenv('DB_ZABBIX_PASS')
    db_zabbix_host: str = os.getenv('DB_ZABBIX_HOST')
    db_zabbix_port: str = os.getenv('DB_ZABBIX_PORT')

    ssh_host = os.getenv("SSH_HOST")
    ssh_port = os.getenv("SSH_PORT")
    ssh_user = os.getenv("SSH_USER")
    ssh_pass = os.getenv("SSH_PASS")
    ssh_remote_bind_address = os.getenv("SSH_REMOTE_BIND_ADDRESS")
    ssh_remote_bind_port = os.getenv("SSH_REMOTE_BIND_PORT")

    secret_key: str = os.getenv('SECRET_KEY')
    token_expire: int = os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES')
    refresh_token_expire: int = os.getenv('REFRESH_TOKEN_EXPIRE_MINUTES')

    zabbix_api_token: str = os.getenv('ZABBIX_API_TOKEN')
    zabbix_server_url: str = os.getenv('ZABBIX_SERVER_URL')
