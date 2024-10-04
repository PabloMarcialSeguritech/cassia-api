from rocketry import Grouper
from rocketry.conds import daily
from utils.settings import Settings
from services.integration.reset_service_service_impl import ResetServiceImpl

# Crear la aplicación Rocketry
resets_schedule = Grouper()

# Cargar la configuración
SETTINGS = Settings()

# Definir la bandera "resets" usando la configuración
resets = SETTINGS.cassia_resets


@resets_schedule.cond('resets')
def is_resets():
    return resets


# Definir la tarea para que se ejecute todos los días a las 6:00 AM y a las 18:00 PM si la bandera "resets" está activa
@resets_schedule.task((daily.at("06:00") | daily.at("18:00")) & is_resets, execution="thread")
async def merge_resets():
    reset_service = ResetServiceImpl()
    resets = await reset_service.merge_resets()
    print(resets)
