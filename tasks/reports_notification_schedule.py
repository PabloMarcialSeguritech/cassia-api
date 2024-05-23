from rocketry import Grouper
from rocketry.conds import every
from datetime import datetime, timedelta
import pytz
import utils.settings as settings
import time
from infraestructure.cassia import cassia_reports_notifications_repository
from infraestructure.zabbix import AlertsRepository
import asyncio
import pandas as pd
import tempfile
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
# Creating the Rocketry app
report_scheduler = Grouper()
settings = settings.Settings()
send_reports = settings.send_reports


""" @slack_scheduler.cond('slack_notify')
def is_traffic():
    return slack_notify


@slack_scheduler.task(("every 60 seconds & slack_notify"), execution="thread")
 """


@report_scheduler.cond('send_reports')
def is_send_reports():
    return send_reports


@report_scheduler.task(("every 1 minute & send_reports"), execution="thread")
async def process_notifications():
    now = datetime.now(pytz.timezone("America/Mexico_City"))
    lower_limit = (now - timedelta(seconds=30)).strftime('%H:%M:%S')
    upper_limit = (now + timedelta(seconds=30)).strftime('%H:%M:%S')
    reports_to_process = await cassia_reports_notifications_repository.get_reports_to_process(lower_limit, upper_limit)
    procesos = []
    print(reports_to_process)
    for ind in reports_to_process.index:
        match reports_to_process['internal_name'][ind]:
            case 'reporte_odd':
                if reports_to_process['monthly'][ind] == 1:
                    dia = now.day
                    if reports_to_process['day_of_month'][ind] == dia:
                        procesos.append(odd_report('Mensual'))
                        print(f"Mensual en dia {dia}")
                        continue
                if reports_to_process['weekly'][ind] == 1:
                    dia = now.weekday()+1
                    if reports_to_process['day_of_week'][ind] == dia:
                        print(f"Semanal en dia {dia}")
                        procesos.append(odd_report('Semanal'))
                        continue
                if reports_to_process['daily'][ind] == 1:
                    print(f"Diario en dia {dia}")
                    procesos.append(odd_report('Diario'))
                    continue
    await asyncio.gather(*procesos)


async def odd_report(frecuencia):
    problems = await AlertsRepository.get_problems_filter('0', '', '', '6')
    file_path = ''
    if not problems.empty:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            xlsx_filename = temp_file.name
            file_path = xlsx_filename
            with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:
                problems = problems.sort_values(by='fecha', ascending=False)
                problems = problems.drop(columns=['diferencia', 'fecha'])
                problems.to_excel(
                    writer, sheet_name='Data', index=False)
    user_mails = await cassia_reports_notifications_repository.get_user_emails_by_report_id(1)
    if not user_mails.empty:
        mails = user_mails['mail'].to_list()
    # Configuración del servidor de correo
    # Cambia esto por el servidor SMTP de tu proveedor
    smtp_server = settings.MAIL_SERVER
    smtp_port = settings.MAIL_PORT  # Normalmente 587 para TLS, 465 para SSL
    # Cambia esto por tu dirección de correo electrónico
    smtp_user = settings.MAIL_FROM
    # Cambia esto por tu contraseña de correo electrónico
    smtp_password = settings.MAIL_PASSWORD
    fecha = datetime.now(pytz.timezone("America/Mexico_City")
                         ).strftime('%d/%m/%Y %H:%M:%S')
    # Configuración del correo
    from_email = settings.MAIL_FROM_NAME
    to_emails = mails
    subject = f'Reporte {frecuencia} de ODD'
    estado = settings.abreviatura_estado
    body = f'\nSe adjunta reporte {frecuencia} de dispositivos Down del estado de {estado} correspondiente al dia.\n\nSaludos.'

    # Crear el objeto de mensaje
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = ','.join(to_emails)
    msg['Subject'] = subject

    # Adjuntar el cuerpo del correo
    msg.attach(MIMEText(body, 'plain'))

    # Adjuntar el archivo de Excel
    attachment = open(file_path, 'rb')
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    f'attachment; filename={os.path.basename(file_path)}')
    msg.attach(part)

    # Conectar al servidor SMTP y enviar el correo
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Usar TLS
        server.login(smtp_user, smtp_password)
        text = msg.as_string()
        server.sendmail(from_email, to_emails, text)
        print('Correo enviado exitosamente')
    except Exception as e:
        print(f'Error al enviar el correo: {e}')
    finally:
        server.quit()
