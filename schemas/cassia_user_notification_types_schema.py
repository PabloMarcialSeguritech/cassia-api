from pydantic import BaseModel
from typing import List, Optional
from pydantic import BaseModel
from pydantic import Field


class CassiaUserNotificationTechsSchema(BaseModel):
    user_id: int = Field(...,
                         example=1)
    cassia_notification_type_id: List[int] = Field(...,
                                                   example=[1, 2])

    cassia_tech_id: List[List[int]] = Field(...,
                                            example=[[1], [0]])