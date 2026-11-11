from collections.abc import Mapping
from enum import StrEnum
from typing import Final


class VisitStatus(StrEnum):
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELED = "canceled"
    MISSED = "missed"


TRANSITIONS: Final[Mapping[VisitStatus, frozenset[VisitStatus]]] = {
    VisitStatus.IN_PROGRESS: frozenset({VisitStatus.COMPLETED}),
}


def sources_for(target: VisitStatus) -> list[VisitStatus]:
    return sorted(source for source, targets in TRANSITIONS.items() if target in targets)
