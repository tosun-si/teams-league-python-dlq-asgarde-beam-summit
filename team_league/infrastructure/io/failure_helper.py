import traceback

from asgarde.failure import Failure
from pampy import match, _

from team_league.domain.exception.team_stats_validation_exception import TeamStatsValidationException


def get_failure_error(failure: Failure) -> str:
    return match(failure,
                 lambda f: isinstance(f.exception, TeamStatsValidationException), lambda f: str(f.exception.errors),
                 _, lambda f: _get_exception_traceback(f))


def _get_exception_traceback(failure: Failure) -> str:
    if failure.exception is None:
        raise AttributeError("The traceback could not be retrieved because the exception is None")

    return ''.join(traceback.format_exception(None, failure.exception, failure.exception.__traceback__))
