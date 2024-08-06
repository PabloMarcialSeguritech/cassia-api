from rocketry import Grouper
from utils.settings import Settings


# Creating the Rocketry app
resets_schedule = Grouper()

# Creating some tasks
SETTINGS = Settings()
# Definir la bandera "resets" usando la configuración
flag_resets = Flag("resets", value=SETTINGS.cassia_resets)

@resets_schedule.cond('resets')
def is_syslog():
    return resets


@resets_schedule.task(("every 30 seconds & resets"), execution="thread")
async def update_syslog_data():

    '''
    todo logica
    '''