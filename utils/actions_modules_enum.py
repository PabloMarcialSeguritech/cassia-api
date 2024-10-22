from enum import Enum

class AuditAction(Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3
    ACK = 4
    CLOSE = 5
    RESET = 6

class AuditModule(Enum):
    GROUPS = 1
    PROXIES = 2
    TECHNOLOGIES = 3
    BRANDS = 4
    MODELS = 5
    HOSTS = 6

