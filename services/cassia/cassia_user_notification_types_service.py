from infraestructure.cassia import cassia_user_notification_types_repository
from fastapi import status, HTTPException
from schemas import cassia_user_notification_types_schema
from utils.traits import success_response
import pandas as pd


async def get_techs():
    techs = await cassia_user_notification_types_repository.get_techs()
    return success_response(data=techs.to_dict(orient="records"))


async def get_notification_types():
    notification_types = await cassia_user_notification_types_repository.get_notification_types()
    return success_response(data=notification_types.to_dict(orient="records"))


async def get_users_notification_types_old():
    users = await cassia_user_notification_types_repository.get_users()
    user_notifications = await cassia_user_notification_types_repository.get_users_notifications_types()
    response = []
    if not users.empty:
        for ind in users.index:
            registros_usuario = user_notifications.loc[user_notifications['user_id']
                                                       == users['user_id'][ind]]
            notification_types = []
            for ind2 in registros_usuario.index:
                notification_types.append({
                    'cassia_notification_type_id': int(registros_usuario['cassia_notification_type_id'][ind2]),
                    'name': registros_usuario['name'][ind2],
                })
            response_row = {'user_id': int(users['user_id'][ind]),
                            'mail': users['mail'][ind],
                            'name': users['name'][ind],
                            'user_notification_types': notification_types}
            response.append(response_row)
    return success_response(data=response)


async def get_users_notification_types():
    users = await cassia_user_notification_types_repository.get_users()
    user_notifications = await cassia_user_notification_types_repository.get_users_notifications_types()
    response = []
    if not users.empty:
        for ind in users.index:
            notification_types = []
            if not user_notifications.empty:
                registros_usuario = user_notifications.loc[user_notifications['user_id']
                                                           == users['user_id'][ind]]
                for ind2 in registros_usuario.index:
                    notification_types.append({
                        'cassia_notification_type_id': int(registros_usuario['cassia_notification_type_id'][ind2]),
                        'name': registros_usuario['name'][ind2],
                    })
            response_row = {'user_id': int(users['user_id'][ind]),
                            'mail': users['mail'][ind],
                            'name': users['name'][ind],
                            'user_notification_types': notification_types}
            response.append(response_row)
    return success_response(data=response)


async def get_user_notification_types(user_id):
    user = await cassia_user_notification_types_repository.get_user(user_id)
    if user.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Usuario no encontrado")
    user_notifications_techs = await cassia_user_notification_types_repository.get_user_notifications_techs(user_id)
    techs = await cassia_user_notification_types_repository.get_techs()
    response = {
        'user_id': int(user['user_id'][0]),
        'mail': user['mail'][0],
        'name': user['name'][0],
    }
    notifications = []
    if not user_notifications_techs.empty:
        user_notifications_techs['check'] = 0
    for ind in user_notifications_techs.index:
        if user_notifications_techs['check'][ind] != 0:
            continue
        registros_tecnologias = user_notifications_techs.loc[user_notifications_techs['cassia_notification_type_id']
                                                             == user_notifications_techs['cassia_notification_type_id'][ind]]
        print(registros_tecnologias)
        user_notifications_techs.loc[user_notifications_techs['cassia_notification_type_id']
                                     == registros_tecnologias['cassia_notification_type_id'][ind], 'check'] = 1
        notification_techs = []
        print(registros_tecnologias)
        if registros_tecnologias['cassia_tech_id'].isnull().any():
            for ind2 in techs.index:
                notification_techs.append(
                    {'cassia_tech_id': int(techs['cassia_tech_id'][ind2]),
                     'tech_name': techs['tech_name'][ind2], })
        else:
            for ind2 in registros_tecnologias.index:
                notification_techs.append(
                    {'cassia_tech_id': int(registros_tecnologias['cassia_tech_id'][ind2]),
                     'tech_name': registros_tecnologias['tech_name'][ind2], })
        notifications.append({
            'cassia_user_notification_type_id': int(user_notifications_techs['cassia_notification_type_id'][ind]),
            'name': user_notifications_techs['name'][ind],
            'techs': notification_techs
        })
    response.update({'notifications': notifications})

    return success_response(data=response)


async def update_user_notification_tech(notification_data: cassia_user_notification_types_schema.CassiaUserNotificationTechsSchema):

    user = await cassia_user_notification_types_repository.get_user(notification_data.user_id)
    if user.empty:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Usuario no encontrado")
    delete_info = await cassia_user_notification_types_repository.delete_info(notification_data.user_id)
    for cassia_notification_type_id, techs in zip(notification_data.cassia_notification_type_id, notification_data.cassia_tech_id):
        techs_list = ['Null' if x == 0 else x for x in techs]
        created_registers = await cassia_user_notification_types_repository.create_user_notification_type(notification_data.user_id, cassia_notification_type_id, techs_list)

    return success_response(message="Información almacenada correctamente")


async def update_users_notifications(users_notification_data: cassia_user_notification_types_schema.CassiaUserNotificationSchema):
    delete_info = await cassia_user_notification_types_repository.delete_info_users(users_notification_data.user_ids)
    values = []
    for user_id, cassia_notification_types in zip(users_notification_data.user_ids, users_notification_data.cassia_notification_type_ids):
        for cassia_notification_type_id in cassia_notification_types:
            values.append(f"({user_id},{cassia_notification_type_id})")

    if len(values) > 0:
        values = ", ".join([value for value in values])

        created_registers = await cassia_user_notification_types_repository.create_users_notification_types(values)

    return success_response(message="Registros actualizados correctamente")
