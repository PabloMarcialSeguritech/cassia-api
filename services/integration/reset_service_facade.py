

class ResetServiceFacade:

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

    async def restart_reset(self, object_id, host_id):
        raise NotImplementedError("Método no implementado")

    async def getDispositivosRelacionadosCapa1(self, hostid):
        raise NotImplementedError("Método no implementado")

    async def reset_pmi(self, afiliacion):
        raise NotImplementedError("Método no implementado")

    async def get_object_id_by_affiliation(self, affiliation):
        raise NotImplementedError("Método no implementado")