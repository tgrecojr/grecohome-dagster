"""The per-collection spec that drives every bronze check.

A subject builds a list of :class:`CollectionCheckConfig` (one per collection it
wants checked) and passes each to the check builders. Subjects only *describe*
their collections here; they never reimplement check logic.

Pure-Python note: the original spec used a DuckDB ``event_date_sql`` expression.
Because these checks read bronze with the stdlib (no DuckDB), the event date is
identified by a *field name* instead — :attr:`event_date_field`, interpreted per
:attr:`reader` (a dotted JSON path on the record, or a CSV column name).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from dagster import AssetKey

#: A bronze payload's stored form.
Reader = Literal["json", "csv", "txt"]

#: Where a collection's *event* date comes from. ``"partition"`` means the hive
#: ``dt=`` folder is the event date (capture-once/immutable sources); ``"payload"``
#: means it lives inside the payload (e.g. a CSV reading timestamp, a Whoop
#: ``start`` field) and must be parsed; ``"none"`` is a current-only snapshot with
#: no meaningful timeline (completeness is skipped).
EventDateSource = Literal["payload", "partition", "none"]

#: The four check families, by short name. Used by :attr:`enabled_checks`.
ALL_CHECKS: frozenset[str] = frozenset({"freshness", "completeness", "schema", "content"})


@dataclass(frozen=True)
class CollectionCheckConfig:
    """How to check one bronze collection.

    Args:
        source: Data provider, lowercase (e.g. ``"whoop"``). Matches the bronze
            path segment ``{bronze_root}/{source}/...``.
        collection: Dataset within the source (e.g. ``"sleep"``). Matches the
            bronze path segment ``.../{collection}/...``.
        asset_key: The existing bronze ``AssetKey`` these checks attach to.
        reader: Stored payload form — ``"json"``, ``"csv"`` or ``"txt"``.
        unnest_records: ``True`` when a JSON payload is a ``{"records": [...]}``
            wrapper (Whoop); the schema signature and event dates then look at a
            record, not the wrapper.
        event_date_source: Where the event date comes from (see
            :data:`EventDateSource`).
        event_date_field: When ``event_date_source == "payload"``, the field that
            carries the event timestamp — a JSON key on the (unnested) record for
            ``reader == "json"`` (dotted paths allowed, e.g. ``"score.start"``),
            or a column name for ``reader == "csv"``. Ignored otherwise.
        cadence_hours: Freshness tolerance: how many hours between captures is
            normal, *before* :attr:`grace_hours` is added.
        grace_hours: Slack added to :attr:`cadence_hours` before a collection is
            called stale (absorbs scheduler jitter / a single missed tick).
        cadence_days: Completeness tolerance: a gap in the event timeline larger
            than this many days is surfaced (WARN).
        expected_empty: ``True`` for collections that are *legitimately* always
            empty on this hardware (e.g. Garmin ``hrv`` on a device without the
            sensor). Content-health then passes on empty payloads, and freshness
            does not fail when the collection has zero captures (those endpoints
            write nothing when empty).
        enabled_checks: Which of the four families to build for this collection.
        recent_partitions: Bound each check to the trailing N ``dt=`` partitions
            by default, so checks stay fast as bronze grows.
    """

    source: str
    collection: str
    asset_key: AssetKey
    reader: Reader = "json"
    unnest_records: bool = False
    event_date_source: EventDateSource = "partition"
    event_date_field: str | None = None
    cadence_hours: float = 26.0
    grace_hours: float = 6.0
    cadence_days: int = 2
    expected_empty: bool = False
    enabled_checks: frozenset[str] = ALL_CHECKS
    recent_partitions: int = 14

    def __post_init__(self) -> None:
        if self.event_date_source == "payload" and not self.event_date_field:
            raise ValueError(
                f"{self.source}/{self.collection}: event_date_source='payload' "
                "requires event_date_field"
            )
        unknown = self.enabled_checks - ALL_CHECKS
        if unknown:
            raise ValueError(
                f"{self.source}/{self.collection}: unknown checks {sorted(unknown)}; "
                f"valid: {sorted(ALL_CHECKS)}"
            )

    @property
    def check_name_prefix(self) -> str:
        """Stable prefix for this collection's check names, e.g. ``whoop_sleep``."""
        return f"{self.source}_{self.collection}"
