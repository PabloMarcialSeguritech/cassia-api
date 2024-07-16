from fastapi import status, HTTPException, UploadFile
from infraestructure.cassia import cassia_technologies_repository
from infraestructure.cassia import cassia_criticalities_repository
from utils.traits import success_response
from schemas import cassia_criticality_schema
from datetime import datetime
import os
import shutil
from starlette.datastructures import UploadFile as UploadFileInstance

""" async def get_technologies():
    techs = await cassia_technologies_repository.get_technologies()
    return success_response(data=techs.to_dict(orient="records"))
 """


async def get_criticalities():
    criticalities = await cassia_criticalities_repository.get_criticalities()
    if not criticalities.empty:
        criticalities['id'] = criticalities['cassia_criticality_id']
    return success_response(data=criticalities.to_dict(orient="records"))


async def get_criticality(cassia_criticality_id):
    criticality = await cassia_criticalities_repository.get_criticality_by_id(cassia_criticality_id)
    if criticality.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Criticidad no encontrada")
    return success_response(data=criticality.to_dict(orient="records")[0])


def check_images(file_type):
    if file_type not in ["image/jpeg", "image/png", "image/gif"]:
        return False
    return True


async def create_criticality(criticality_data: cassia_criticality_schema.CassiaCriticalitySchema, icon):
    exist_level = await cassia_criticalities_repository.get_criticality_level(
        criticality_data.level)
    if not exist_level.empty:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El nivel de criticidad ya existe.")

    file_dest = None
    filename = None
    if isinstance(icon, UploadFileInstance):
        print(icon.content_type)
        if not check_images(icon.content_type):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="El icono solo puede ser una imagen jpeg, png o gif")

        upload_dir = os.path.join(
            os.getcwd(), f"uploads/criticality_icons/")
        # Create the upload directory if it doesn't exist
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)
        # get the destination path
        timestamp = str(datetime.now().timestamp()).replace(".", "")
        file_dest = os.path.join(
            upload_dir, f"{timestamp}{icon.filename}")
        filename = f"{timestamp}{icon.filename}"
        # copy the file contents
        with open(file_dest, "wb") as buffer:
            shutil.copyfileobj(icon.file, buffer)
    print(file_dest)
    create_register = await cassia_criticalities_repository.create_criticality(criticality_data, file_dest, filename)
    print(create_register)
    return success_response(message="Registro creado correctamente", data=create_register)


async def update_criticality(cassia_criticality_id, criticality_data: cassia_criticality_schema.CassiaCriticalitySchema, icon):
    criticality_exist = await cassia_criticalities_repository.get_criticality_by_id(cassia_criticality_id)
    if criticality_exist.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Criticidad no encontrada")
    exist_level = await cassia_criticalities_repository.get_criticality_level(
        criticality_data.level)

    if not exist_level.empty:
        if exist_level['cassia_criticality_id'][0] != cassia_criticality_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="El nivel de criticidad ya existe.")
    actual_icon = criticality_exist['icon'][0]
    is_new_icon = isinstance(icon, UploadFileInstance)
    file_dest = None
    filename = None

    if is_new_icon:
        if not check_images(icon.content_type):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="El icono solo puede ser una imagen jpeg, png o gif")
        if actual_icon:
            if os.path.exists(actual_icon):
                os.remove(actual_icon)

        upload_dir = os.path.join(
            os.getcwd(), f"uploads/criticality_icons/")
        # Create the upload directory if it doesn't exist
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)
        # get the destination path
        timestamp = str(datetime.now().timestamp()).replace(".", "")
        file_dest = os.path.join(
            upload_dir, f"{timestamp}{icon.filename}")
        filename = f"{timestamp}{icon.filename}"
        # copy the file contents
        with open(file_dest, "wb") as buffer:
            shutil.copyfileobj(icon.file, buffer)
    print(filename)

    update_register = await cassia_criticalities_repository.update_criticality(cassia_criticality_id, criticality_data, file_dest, filename)
    return success_response(message="Registro actualizado correctamente")


async def delete_criticality(cassia_technology_id):
    criticality_exist = await cassia_criticalities_repository.get_criticality_by_id(cassia_technology_id)
    if criticality_exist.empty:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Criticidad no encontrada")
    deleted_criticality = await cassia_criticalities_repository.delete_criticality_by_id(cassia_technology_id)
    return success_response(message="Registro eliminado correctamente")
