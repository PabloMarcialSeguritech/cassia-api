from infraestructure.database import DB
from infraestructure.cassia import cassia_media_telegram_infraestructure
from schemas import cassia_media_telegram_schema
from fastapi import HTTPException
import requests
from utils.traits import success_response
import pandas as pd


async def get_config(db: DB):
    telegram_config = await cassia_media_telegram_infraestructure.get_telegram_config(db)
    return success_response(data=telegram_config.to_dict(orient="records"))


async def get_groups(db: DB):
    groups = await cassia_media_telegram_infraestructure.get_telegram_groups(db)
    return success_response(data=groups.to_dict(orient='records'))


async def link_telegram_group(group_data: cassia_media_telegram_schema.LinkTelegramGroupRequest, db: DB):
    link_telegram_result = await cassia_media_telegram_infraestructure.link_telegram_group(group_data, db)
    if link_telegram_result:
        return success_response(message="Grupo vinculado correctamente.")
    else:
        raise HTTPException(
            status_code=500, detail=f"Error al vincular grupo de telegram")


async def discovery_new_groups(db: DB):
    get_telegram_config = await cassia_media_telegram_infraestructure.get_telegram_config(db)
    exist_config_values = set(['bot_token', 'group_secret_word_verify']).issubset(
        get_telegram_config['name'].values)
    if not exist_config_values:
        raise HTTPException(
            status_code=500, detail=f"No existen los valores de configuración de telegram, contacte a soporte para configurarlos.")
    BOT_TOKEN = get_telegram_config.loc[get_telegram_config['name'] == 'bot_token', 'value'].astype(
        'str').values[0]
    PALABRA_SEGURIDAD = get_telegram_config.loc[get_telegram_config['name'] == 'group_secret_word_verify', 'value'].astype(
        'str').values[0]
    URL = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    try:
        # Obtener grupos ya registrados
        grupos_registrados = await cassia_media_telegram_infraestructure.get_telegram_groups(db)
        grupos_registrados_ids = {grupos_registrados["groupid"][ind]
                                  for ind in grupos_registrados.index}

        print("Grupos ya existentes: ", grupos_registrados)

        # Obtener actualizaciones de la API de Telegram
        response = requests.get(URL)
        if response.status_code != 200:
            raise HTTPException(
                status_code=500, detail="Error al obtener actualizaciones del bot."
            )

        data = response.json()
        if not data.get("ok", False):
            raise HTTPException(
                status_code=500, detail="La respuesta de Telegram no es válida."
            )

        grupos_detectados = []

        # Filtrar los grupos descubiertos que no están registrados
        for update in data.get("result", []):
            message = update.get("message", {})
            chat = message.get("chat", {})
            text = message.get("text", "")
            print(message)
            if PALABRA_SEGURIDAD in text:
                groupid = chat.get("id")
                # Verifica si el grupo aún no está registrado
                if groupid not in grupos_registrados_ids:
                    grupo_info = {
                        "name": chat.get("title", "Grupo sin nombre"),
                        "mensaje": text,
                        "groupid": groupid,
                    }
                    print("grupo detectado info: ", grupo_info)
                    grupos_detectados.append(grupo_info)
        df = pd.DataFrame(grupos_detectados)
        if not df.empty:
            df = df.drop_duplicates(subset=['groupid'])
        return success_response(data=df.to_dict(orient='records'))

    except Exception as e:
        raise Exception(f"Error al procesar los mensajes: {str(e)}")
