"""Core metrics from the star schema match an independent pandas calculation."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from credit_risk_agent.metrics import CORE_METRICS, METRICS, compute_metrics
from tests.oracle import core_metrics, prepare


def test_metric_sql_reads_only_marts() -> None:
    for name, sql in METRICS.items():
        lowered = sql.lower()
        assert "marts." in lowered, name
        assert "staging" not in lowered, name
        assert "read_csv" not in lowered, name
        assert "pragma" not in lowered, name


def test_core_metrics_match_pandas(
    connection: duckdb.DuckDBPyConnection,
    fixture_csv: Path,
) -> None:
    kept, rejection_counts = prepare(fixture_csv)
    expected = core_metrics(kept)
    actual = compute_metrics(connection, CORE_METRICS)

    assert actual["loan_count"] == expected["loan_count"]
    assert actual["loan_count"] == float(len(kept))
    for name in (
        "funded_volume",
        "weighted_avg_interest_rate",
        "default_rate_matured",
        "charge_off_rate",
    ):
        assert actual[name] == pytest.approx(expected[name], rel=1e-9, abs=1e-6), name

    warehouse_rejections = dict(
        connection.execute(
            "SELECT reject_reason, row_count FROM marts.load_rejection_summary"
        ).fetchall()
    )
    assert {reason: int(count) for reason, count in warehouse_rejections.items()} == {
        reason: int(count) for reason, count in rejection_counts.items()
    }


def test_null_interest_rate_is_left_out_of_the_weighted_average(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    filtered, treating_null_as_zero = connection.execute(
        """
        SELECT
            SUM(int_rate * funded_amnt) FILTER (WHERE int_rate IS NOT NULL)
                / SUM(funded_amnt) FILTER (WHERE int_rate IS NOT NULL),
            SUM(COALESCE(int_rate, 0) * funded_amnt) / SUM(funded_amnt)
        FROM marts.fact_loan
        """
    ).fetchone()
    assert filtered != treating_null_as_zero
    assert compute_metrics(connection, ("weighted_avg_interest_rate",))[
        "weighted_avg_interest_rate"
    ] == pytest.approx(filtered)


def test_additional_metrics_execute(connection: duckdb.DuckDBPyConnection) -> None:
    values = compute_metrics(connection)
    for name in (
        "avg_interest_rate",
        "delinquency_rate_active",
        "delinquency_rate_balance",
        "recovery_rate",
        "lgd_realised",
        "net_loss_rate",
    ):
        assert values[name] is not None, name
