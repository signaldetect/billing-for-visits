from app.visits.status import VisitStatus, sources_for


def test_only_in_progress_can_be_completed():
    assert sources_for(VisitStatus.COMPLETED) == [VisitStatus.IN_PROGRESS]


def test_nothing_moves_back_to_scheduled():
    assert sources_for(VisitStatus.SCHEDULED) == []
