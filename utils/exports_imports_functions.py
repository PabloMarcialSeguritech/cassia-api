import tempfile
import pandas as pd
from fastapi.responses import FileResponse
import json
import io
import tempfile
import mimetypes


async def generate_file_export(data: pd.DataFrame, page_name, filename, file_type):
    match file_type:
        case 'excel':
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
                xlsx_filename = temp_file.name
                with pd.ExcelWriter(xlsx_filename, engine="xlsxwriter") as writer:
                    data.to_excel(writer, sheet_name=page_name, index=False)
                    return FileResponse(xlsx_filename, headers={"Content-Disposition": f"attachment; filename={filename}"}, media_type="application/vnd.ms-excel", filename=filename)
        case 'csv':
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
                csv_filename = temp_file.name
                data.to_csv(csv_filename, index=False)
                return FileResponse(
                    csv_filename,
                    headers={
                        "Content-Disposition": f"attachment; filename={filename}"},
                    media_type="text/csv",
                    filename=filename
                )
        case 'json':
            with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
                json_filename = temp_file.name
                data.to_json(json_filename, orient='records',
                             lines=False)
                return FileResponse(
                    json_filename,
                    headers={
                        "Content-Disposition": f"attachment; filename={filename}"},
                    media_type="application/json",
                    filename=filename
                )


async def get_df_by_filetype(file) -> pd.DataFrame:
    mime_type, _ = mimetypes.guess_type(file.filename)
    df = pd.DataFrame()
    result = True
    exception = None
    match mime_type:
        case "application/json":
            try:
                content = await file.read()
                content = content.decode('utf-8')
                json_data = json.loads(content)

                # Convertimos el JSON a DataFrame
                df = pd.DataFrame(json_data)
            except json.JSONDecodeError as e:
                exception = e
                result = False
        case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            try:
                # Intentamos cargar el archivo como Excel
                content = await file.read()
                content = content.decode('utf-8')
                # Usamos pandas para leer el archivo Excel
                df = pd.read_excel(content)
            except Exception as e:
                exception = e
                result = False
        case "application/vnd.ms-excel":
            try:
                # Intentamos cargar el archivo como Excel
                content = await file.read()
                content = content.decode('utf-8')
                # Usamos pandas para leer el archivo Excel
                df = pd.read_excel(content)
            except Exception as e:
                exception = e
                result = False
        case "text/csv":
            try:
                # Intentamos cargar el archivo como Excel
                content = await file.read()
                content = content.decode('utf-8')
                csv_file = io.StringIO(content)

                # Usamos pandas para leer el archivo Excel
                df = pd.read_csv(csv_file)
            except Exception as e:
                exception = e
                result = False
    return {'df': df, 'result': result, 'exception': exception}
