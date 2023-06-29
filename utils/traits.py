from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder


def success_response(message: str = "success", success: bool = True, data: any = "", status_code: int = 200):
    response = {
        'message': message,
        'success': success,
        'data': data,
        'status_code': status_code
    }
    return JSONResponse(content=jsonable_encoder(response), status_code=status_code)
