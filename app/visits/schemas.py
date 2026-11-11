from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from app.visits.status import VisitStatus


class VisitIn(BaseModel):
    start_date_time: AwareDatetime
    end_date_time: AwareDatetime
    description: str = ""
    rate: Decimal = Field(ge=0, max_digits=10, decimal_places=2)
    status: VisitStatus = VisitStatus.SCHEDULED

    @model_validator(mode="after")
    def _end_after_start(self) -> Self:
        if self.end_date_time <= self.start_date_time:
            raise ValueError("end_date_time must be after start_date_time")
        return self


class VisitOut(BaseModel):
    id: UUID
    start_date_time: AwareDatetime
    end_date_time: AwareDatetime
    description: str
    rate: Decimal
    status: VisitStatus
