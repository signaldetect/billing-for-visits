from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal

_MICROS_PER_HOUR = 3_600_000_000
_CENT = Decimal("0.01")


def calculate_amount(rate: Decimal, start: datetime, end: datetime) -> Decimal:
    if end <= start:
        raise ValueError("end must be after start")
    micros = (end - start) // timedelta(microseconds=1)
    return (rate * micros / _MICROS_PER_HOUR).quantize(_CENT, rounding=ROUND_HALF_UP)
