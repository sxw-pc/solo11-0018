"""Base errors shared by the history query and export modules.

Every error carries a stable machine-readable ``code`` and a distinct process
exit code.  A downstream audit system can therefore tell a malformed cursor
from a cursor that belongs to another filter set, or from one that lies
outside the requested time window, without parsing English/Chinese prose.
"""

from __future__ import annotations


class HistoryError(Exception):
    """Base class for every user-visible history error."""

    code = "history_error"
    exit_code = 2

    def __init__(self, message: str, *, detail: dict[str, object] | None = None
                 ) -> None:
        super().__init__(message)
        self.message = message
        self.detail = dict(detail or {})

    def as_dict(self) -> dict[str, object]:
        return {"error": self.code, "message": self.message,
                "detail": self.detail}


class QueryError(HistoryError):
    """Caller supplied an invalid filter or an empty/impossible range."""

    code = "invalid_query"
    exit_code = 2


class InvalidFilterError(QueryError):
    code = "invalid_filter"


class RangeError(QueryError):
    code = "invalid_range"


class CursorError(HistoryError):
    """A continuation token could not be used with this query."""

    code = "cursor_error"
    exit_code = 3


class CursorMalformed(CursorError):
    """The token is not a pagewatch cursor, is corrupt, or has no checksum."""

    code = "cursor_malformed"


class CursorFilterMismatch(CursorError):
    """The cursor was issued for a different filter set."""

    code = "cursor_filter_mismatch"
    exit_code = 4


class CursorOutOfRange(CursorError):
    """The cursor position lies outside the half-open query window."""

    code = "cursor_out_of_range"
    exit_code = 5


class ExportError(HistoryError):
    """The JSONL export could not be completed safely."""

    code = "export_error"
    exit_code = 6
