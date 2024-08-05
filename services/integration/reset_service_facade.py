

class ResetServiceFace:

    async def authenticate(self):
        raise NotImplementedError("Método no implementado")

    async def get_devices(self, token):
        raise NotImplementedError("Método no implementado")

    async def extract_device_info(self, devices):
        raise NotImplementedError("Método no implementado")

    async def get_cassia_resets(self):
        raise NotImplementedError("Método no implementado")

    async def merge_resets(self):
        raise NotImplementedError("Método no implementado")