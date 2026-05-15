from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ParseStats:
    """Counts of parse outcomes for a scrape — an audit trail for dropped listings.

    The parsers silently skipped listings with invalid coordinates or parse
    errors. This makes those drops countable so completeness can be reported
    honestly (see METHODOLOGY.md → Coverage & completeness).
    """

    parsed: int = 0
    dropped_zero_coords: int = 0       # latitude/longitude both 0.0
    dropped_parse_error: int = 0       # raised an exception mid-parse
    dropped_missing_id: int = 0        # no usable platform id

    @property
    def dropped_total(self) -> int:
        return (
            self.dropped_zero_coords
            + self.dropped_parse_error
            + self.dropped_missing_id
        )

    def __iadd__(self, other: "ParseStats") -> "ParseStats":
        self.parsed += other.parsed
        self.dropped_zero_coords += other.dropped_zero_coords
        self.dropped_parse_error += other.dropped_parse_error
        self.dropped_missing_id += other.dropped_missing_id
        return self

    def summary(self) -> str:
        return (
            f"parsed={self.parsed}, dropped={self.dropped_total} "
            f"(zero_coords={self.dropped_zero_coords}, "
            f"parse_error={self.dropped_parse_error}, "
            f"missing_id={self.dropped_missing_id})"
        )
