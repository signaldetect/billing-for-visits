from decimal import Decimal
from typing import Final, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

VISITS_TOPIC: Final = "visits.events"
VISIT_COMPLETED_V1: Final = "visits.visit_completed.v1"


class VisitCompletedV1(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_type: Literal["visits.visit_completed.v1"] = VISIT_COMPLETED_V1
    event_id: UUID
    occurred_at: AwareDatetime
    visit_id: UUID
    start_date_time: AwareDatetime
    end_date_time: AwareDatetime
    rate: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def _end_after_start(self) -> Self:
        if self.end_date_time <= self.start_date_time:
            raise ValueError("end_date_time must be after start_date_time")
        return self
