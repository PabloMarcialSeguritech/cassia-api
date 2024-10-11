from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import inspect
from typing import Type

from fastapi import Form
from pydantic import BaseModel
from pydantic.fields import ModelField
import pytz
from datetime import datetime
from .settings import Settings

settings = Settings()


def success_response(message: str = "success", success: bool = True, data: any = "", status_code: int = 200):
    response = {
        'message': message,
        'success': success,
        'data': data,
        'status_code': status_code
    }
    return JSONResponse(content=jsonable_encoder(response), status_code=status_code)


def success_response_with_alert(message: str = "success", success: bool = True, data: any = "", status_code: int = 200, alert=None):
    response = {
        'message': message,
        'success': success,
        'data': data,
        'status_code': status_code,
        'alert': alert
    }
    return JSONResponse(content=jsonable_encoder(response), status_code=status_code)


def as_form(cls: Type[BaseModel]):
    new_parameters = []

    for field_name, model_field in cls.__fields__.items():
        model_field: ModelField  # type: ignore

        new_parameters.append(
            inspect.Parameter(
                model_field.alias,
                inspect.Parameter.POSITIONAL_ONLY,
                default=Form(...) if model_field.required else Form(
                    model_field.default),
                annotation=model_field.outer_type_,
            )
        )

    async def as_form_func(**data):
        return cls(**data)

    sig = inspect.signature(as_form_func)
    sig = sig.replace(parameters=new_parameters)
    as_form_func.__signature__ = sig  # type: ignore
    setattr(cls, 'as_form', as_form_func)
    return cls


def failure_response(status: str = "no ejecutado con exito", message: str = "unsuccessful", success: bool = False,
                     data: any = {"action": "false"}, status_code: int = 200,
                     recommendation: str = ""):
    response = {
        'status': status,
        'message': message,
        'success': success,
        'data': data,
        'status_code': status_code,
        'recommendation': recommendation
    }
    return JSONResponse(content=jsonable_encoder(response), status_code=status_code)


def get_datetime_now_str_with_tz(tz=settings.time_zone, strf="%Y-%m-%d %H:%M:%S"):
    return datetime.now(pytz.timezone(tz)).strftime(strf)


def get_datetime_now_with_tz(tz=settings.time_zone):
    return datetime.now(pytz.timezone(tz))


def timestamp_to_date_tz(timestamp, tz=settings.time_zone):
    """
    Convierte un timestamp UNIX a una fecha en formato 'd/m/Y H:M:s' en la zona horaria de Ciudad de México.

    Args:
        timestamp (int): El timestamp UNIX a convertir.

    Returns:
        str: La fecha formateada.
    """
    # Definir la zona horaria de Ciudad de México
    tz_mexico = pytz.timezone(tz)

    # Convertir el timestamp a un objeto datetime en UTC
    fecha_utc = datetime.fromtimestamp(timestamp, tz=pytz.utc)

    # Convertir a la zona horaria de Ciudad de México
    fecha_tz = fecha_utc.astimezone(tz_mexico)

    return fecha_tz.strftime('%d/%m/%Y %H:%M:%S')
