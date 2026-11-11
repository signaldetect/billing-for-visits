from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.billing.pricing import calculate_amount

T = datetime(2026, 11, 11, 8, tzinfo=UTC)


def test_doc_example():
    assert calculate_amount(Decimal("40.00"), T, T + timedelta(minutes=90)) == Decimal("60.00")


def test_fraction_rounds_to_cents():
    assert calculate_amount(Decimal("10.00"), T, T + timedelta(minutes=80)) == Decimal("13.33")


def test_half_cent_rounds_up():
    assert calculate_amount(Decimal("0.30"), T, T + timedelta(minutes=1)) == Decimal("0.01")


def test_mixed_utc_offsets_use_absolute_time():
    start = datetime(2026, 11, 11, 8, tzinfo=timezone(timedelta(hours=2)))
    end = datetime(2026, 11, 11, 7, 30, tzinfo=UTC)
    assert calculate_amount(Decimal("40.00"), start, end) == Decimal("60.00")


def test_empty_duration_is_rejected():
    with pytest.raises(ValueError):
        calculate_amount(Decimal("40.00"), T, T)
