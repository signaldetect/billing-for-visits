from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.contracts.events import VisitCompletedV1

START, END = datetime(2026, 11, 11, 8, tzinfo=UTC), datetime(2026, 11, 11, 9, 30, tzinfo=UTC)


def test_visit_completed_json_shape():
    e = VisitCompletedV1(event_id=uuid4(), occurred_at=END, visit_id=uuid4(),
                         start_date_time=START, end_date_time=END, rate=Decimal("40.00"))
    data = e.model_dump(mode="json")
    assert data["event_type"] == "visits.visit_completed.v1"
    assert data["rate"] == "40.00"
    assert data["start_date_time"] == "2026-11-11T08:00:00Z"
    assert VisitCompletedV1.model_validate(data) == e


def test_visit_completed_rejects_naive_datetime():
    with pytest.raises(ValidationError):
        VisitCompletedV1(event_id=uuid4(), occurred_at=END, visit_id=uuid4(),
                         start_date_time=START.replace(tzinfo=None), end_date_time=END, rate=Decimal("1"))


def test_visit_completed_rejects_end_not_after_start():
    with pytest.raises(ValidationError):
        VisitCompletedV1(event_id=uuid4(), occurred_at=END, visit_id=uuid4(),
                         start_date_time=START, end_date_time=START, rate=Decimal("1"))


def test_visit_completed_rejects_negative_rate():
    with pytest.raises(ValidationError):
        VisitCompletedV1(event_id=uuid4(), occurred_at=END, visit_id=uuid4(),
                         start_date_time=START, end_date_time=END, rate=Decimal("-1"))


@pytest.mark.parametrize("rate", ["1e30", "0.001", "100000000.00"])
def test_visit_completed_rejects_rate_outside_column_bounds(rate):
    with pytest.raises(ValidationError):
        VisitCompletedV1(event_id=uuid4(), occurred_at=END, visit_id=uuid4(),
                         start_date_time=START, end_date_time=END, rate=Decimal(rate))
