"""A small, single-pass data-quality gate for the silver frame.

Expectations mirror Great Expectations' vocabulary (``not_null``, ``unique``,
``in_set``, ``between``) but evaluate as one Spark aggregation, so the whole suite
costs a single job rather than one scan per rule. ``error`` failures abort the
pipeline; ``warn`` failures are logged and let the run continue.

This is intentionally a focused, deterministic engine rather than a full GE
integration — it needs no extra services, is trivially unit-tested, and keeps the
DQ contract in code next to the transform it guards.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ERROR = "error"
WARN = "warn"


class DataQualityError(RuntimeError):
    """Raised when one or more ``error``-severity expectations fail."""


@dataclass(frozen=True)
class Expectation:
    kind: str  # not_null | unique | in_set | between
    column: str
    severity: str = ERROR
    lo: float | None = None
    hi: float | None = None
    allowed: tuple[object, ...] | None = None

    @property
    def name(self) -> str:
        if self.kind == "between":
            return f"expect_{self.column}_between_{self.lo}_and_{self.hi}"
        if self.kind == "in_set":
            return f"expect_{self.column}_in_set"
        return f"expect_{self.column}_{self.kind}"

    def _failure_count(self) -> Column:
        col = F.col(self.column)
        if self.kind == "not_null":
            return F.sum(col.isNull().cast("long"))
        if self.kind == "unique":
            return F.count(col) - F.countDistinct(col)
        if self.kind == "between":
            bad = col.isNotNull() & ((col < self.lo) | (col > self.hi))
            return F.sum(bad.cast("long"))
        if self.kind == "in_set":
            bad = col.isNotNull() & ~col.isin(list(self.allowed or ()))
            return F.sum(bad.cast("long"))
        raise ValueError(f"unknown expectation kind: {self.kind}")


@dataclass(frozen=True)
class ExpectationResult:
    expectation: Expectation
    failures: int

    @property
    def passed(self) -> bool:
        return self.failures == 0


@dataclass
class SuiteResult:
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def failed(self) -> list[ExpectationResult]:
        return [r for r in self.results if not r.passed]

    @property
    def failed_errors(self) -> list[ExpectationResult]:
        return [r for r in self.failed if r.expectation.severity == ERROR]

    def raise_for_errors(self) -> None:
        errors = self.failed_errors
        if errors:
            detail = ", ".join(f"{r.expectation.name}={r.failures}" for r in errors)
            raise DataQualityError(f"{len(errors)} error expectation(s) failed: {detail}")

    def summary(self) -> str:
        lines = []
        for r in self.results:
            status = "PASS" if r.passed else f"FAIL[{r.expectation.severity}]"
            lines.append(f"  {status:12} {r.expectation.name} (failures={r.failures})")
        return "\n".join(lines)


def run_suite(df: DataFrame, expectations: list[Expectation]) -> SuiteResult:
    """Evaluate every expectation in a single aggregation pass."""
    if not expectations:
        return SuiteResult([])
    aggs = [e._failure_count().alias(f"m{i}") for i, e in enumerate(expectations)]
    row = df.agg(*aggs).collect()[0]
    return SuiteResult(
        [ExpectationResult(e, int(row[f"m{i}"] or 0)) for i, e in enumerate(expectations)]
    )


# The silver contract for GDELT events. Ranges follow the GDELT 2.0 codebook.
SILVER_EXPECTATIONS: list[Expectation] = [
    Expectation("not_null", "global_event_id", ERROR),
    Expectation("unique", "global_event_id", ERROR),
    Expectation("not_null", "sql_date", ERROR),
    Expectation("in_set", "quad_class", ERROR, allowed=(1, 2, 3, 4)),
    Expectation("between", "goldstein_scale", WARN, lo=-10.0, hi=10.0),
    Expectation("between", "avg_tone", WARN, lo=-100.0, hi=100.0),
    Expectation("between", "num_articles", WARN, lo=0, hi=1_000_000),
    Expectation("between", "action_geo_lat", WARN, lo=-90.0, hi=90.0),
    Expectation("between", "action_geo_long", WARN, lo=-180.0, hi=180.0),
]
