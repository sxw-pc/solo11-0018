"""Discovery and selection of monitoring rounds.

A *round* is one ``run-once`` invocation.  Rounds are not recorded
explicitly; they are reconstructed from the probe ledger by clustering
distinct ``checked_at`` timestamps: two probes belong to the same round
when their timestamps are at most ``gap_seconds`` apart.  With the
frozen clock used by tests and demos every probe of a round shares one
exact timestamp, so clustering is exact there.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from ..clock import parse_iso

DEFAULT_ROUND_GAP_SECONDS = 2.0


@dataclass(frozen=True)
class Round:
    """One monitoring round reconstructed from the probe ledger."""

    index: int          # 1-based, chronological over all discovered rounds
    start: float        # earliest probe timestamp of the round
    end: float          # latest probe timestamp of the round

    def contains(self, moment: float) -> bool:
        return self.start <= moment <= self.end


def discover_rounds(connection: sqlite3.Connection,
                    gap_seconds: float = DEFAULT_ROUND_GAP_SECONDS) -> list[Round]:
    """Cluster distinct probe timestamps into chronological rounds."""
    rows = connection.execute(
        "SELECT DISTINCT checked_at FROM probes ORDER BY checked_at"
    ).fetchall()
    timestamps = [float(row["checked_at"]) for row in rows]
    clusters: list[list[float]] = []
    for moment in timestamps:
        if clusters and moment - clusters[-1][-1] <= gap_seconds:
            clusters[-1].append(moment)
        else:
            clusters.append([moment])
    return [
        Round(index=position + 1, start=cluster[0], end=cluster[-1])
        for position, cluster in enumerate(clusters)
    ]


def _select_by_timestamp(rounds: list[Round], value: str) -> Round:
    moment = parse_iso(value)
    for candidate in rounds:
        if candidate.contains(moment):
            return candidate
    raise ValueError(f"no monitoring round contains timestamp {value!r}")


def select_rounds(rounds: list[Round], *, selectors: list[str] | None = None,
                  last: int | None = None) -> list[Round]:
    """Pick rounds by 1-based index or ISO/epoch timestamp.

    Without selectors and without ``last`` every discovered round is
    returned.  Selections are unioned, de-duplicated and returned in
    chronological order.
    """
    if last is not None and last < 1:
        raise ValueError("--last must be a positive integer")
    chosen: dict[int, Round] = {}
    for raw in selectors or []:
        value = raw.strip()
        try:
            index = int(value)
        except ValueError:
            found = _select_by_timestamp(rounds, value)
            chosen[found.index] = found
            continue
        if index < 1 or index > len(rounds):
            raise ValueError(
                f"round index {index} out of range (1..{len(rounds)})")
        chosen[index] = rounds[index - 1]
    if last is not None:
        for candidate in rounds[-last:]:
            chosen[candidate.index] = candidate
    if not chosen and not selectors and last is None:
        return list(rounds)
    return [chosen[index] for index in sorted(chosen)]
