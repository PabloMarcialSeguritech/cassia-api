from typing import List
from pydantic import BaseModel,  validator


class CassiaUserReportNotificationSchema(BaseModel):
    user_ids: List[int]
    cassia_report_frequency_schedule_ids: List[List[int]]

    @validator('cassia_report_frequency_schedule_ids')
    def check_lengths(cls, v, values):
        if 'user_ids' in values and len(v) != len(values['user_ids']):
            raise ValueError(
                'Both user_ids and cassia_report_frequency_schedule_ids must have the same length')
        return v
