import os

from pydantic import BaseSettings
from dotenv import load_dotenv

env = os.getenv('ENVIRONMENT', 'qa')
dotenv_path = f'.env.{env}'
load_dotenv(dotenv_path=dotenv_path, verbose=True)


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

    db_c5_name: str = os.getenv("DB_C5_NAME")
    db_c5_user: str = os.getenv('DB_C5_USER')
    db_c5_pass: str = os.getenv('DB_C5_PASS')
    db_c5_host: str = os.getenv('DB_C5_HOST')
    db_c5_port: str = os.getenv('DB_C5_PORT')

    ssh_host: str = os.getenv("SSH_HOST")
    ssh_port: str = os.getenv("SSH_PORT")
    ssh_user: str = os.getenv("SSH_USER")
    ssh_pass: str = os.getenv("SSH_PASS")
    ssh_remote_bind_address: str = os.getenv("SSH_REMOTE_BIND_ADDRESS")
    ssh_remote_bind_port: str = os.getenv("SSH_REMOTE_BIND_PORT")

    secret_key: str = os.getenv('SECRET_KEY')
    token_expire: int = os.getenv('ACCESS_TOKEN_EXPIRE_MINUTES')
    refresh_token_expire: int = os.getenv('REFRESH_TOKEN_EXPIRE_MINUTES')

    zabbix_api_token: str = os.getenv('ZABBIX_API_TOKEN')
    zabbix_server_url: str = os.getenv('ZABBIX_SERVER_URL')

    env: str = f'{env}'

    MAIL_USERNAME: str = os.getenv('MAIL_USERNAME')
    MAIL_PASSWORD: str = os.getenv('MAIL_PASSWORD')
    MAIL_FROM: str = os.getenv('MAIL_FROM')
    MAIL_PORT: int = int(os.getenv('MAIL_PORT'))
    MAIL_SERVER: str = os.getenv('MAIL_SERVER')
    MAIL_FROM_NAME: str = os.getenv('MAIN_FROM_NAME')

    ssh_host_client: str = os.getenv('SSH_HOST_CLIENT')
    ssh_user_client: str = os.getenv('SSH_HOST_USER')
    ssh_pass_client: str = os.getenv('SSH_HOST_PASS')
    ssh_key_gen: str = os.getenv('SSH_KEY_GEN')

    cassia_server_ip: str = os.getenv('CASSIA_SERVER_IP')
    cassia_traffic: bool = False
    db_c5_instancia_nombrada: bool = False
    try:
        cassia_traffic: bool = True if int(
            os.getenv('TRAFFIC')) == 1 else False

    except:
        pass
    slack_notify: bool = False
    try:
        slack_notify: bool = True if int(
            os.getenv('SLACK_NOTIFY')) == 1 else False
    except:
        pass
    try:
        db_c5_instancia_nombrada: bool = True if int(
            os.getenv('DB_C5_INSTANCIA_NOMBRADA')) == 1 else False
    except:
        pass
    abreviatura_estado: str = ""
    try:
        abreviatura_estado: str = os.getenv('ABREVIATURA_ESTADO')
    except:
        pass
    estado: str = ""
    try:
        estado: str = os.getenv('ESTADO')
    except:
        pass

    zabbix_user: str = "Admin"
    try:
        zabbix_user: str = os.getenv('ZABBIX_USER')
    except:
        pass
    zabbix_password: str = "zabbix"
    try:
        zabbix_password: str = os.getenv('ZABBIX_PASSWORD')
    except:
        pass

    """ cassia_traffic: bool = True if int(os.getenv('TRAFFIC')) == 1 else False """

    slack_token: str = ""
    try:
        slack_token: str = os.getenv('SLACK_TOKEN')
    except:
        pass

    slack_channel: str = ""
    try:
        slack_channel: str = os.getenv('SLACK_CHANNEL')
    except:
        pass

    slack_bot: str = ""
    try:
        slack_bot: str = os.getenv('SLACK_BOT')
    except:
        pass

    slack_problem_severities: str = "4,5"
    try:
        slack_problem_severities: str = os.getenv('SLACK_PROBLEM_SEVERITIES')
    except:
        pass

    cassia_syslog: bool = False
    try:
        cassia_syslog: bool = True if int(
            os.getenv('SYSLOG')) == 1 else False

    except:
        pass
    db_syslog_name: str = ""
    db_syslog_user: str = ""
    db_syslog_pass: str = ""
    db_syslog_host: str = ""
    db_syslog_port: str = ""
    try:
        db_syslog_name: str = os.getenv("DB_SYSLOG_NAME")
        db_syslog_user: str = os.getenv('DB_SYSLOG_USER')
        db_syslog_pass: str = os.getenv('DB_SYSLOG_PASS')
        db_syslog_host: str = os.getenv('DB_SYSLOG_HOST')
        db_syslog_port: str = os.getenv('DB_SYSLOG_PORT')
    except:
        pass

    cassia_traffic_syslog: bool = False
    try:
        cassia_traffic_syslog: bool = True if int(
            os.getenv('TRAFFIC_SYSLOG')) == 1 else False
    except:
        pass
    cassia_diagnosta: bool = False
    cassia_diagnosta_api_url: str = ""
    try:
        cassia_diagnosta: bool = True if int(
            os.getenv('DIAGNOSTA')) == 1 else False
        cassia_diagnosta_api_url = os.getenv('DIAGNOSTA_API_URL')
    except:
        pass
