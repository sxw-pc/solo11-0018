"""Alert eligibility rules."""

from __future__ import annotations

from ..models import Rules


def should_alert(severity: str, rules: Rules) -> bool:
    return severity in set(rules.alert_severities)


def alert_message(watch_name: str, severity: str, event_id: int,
                  change_rate: float) -> str:
    return (f"watch={watch_name} severity={severity} event={event_id} "
            f"change_rate={change_rate:.4f}")
