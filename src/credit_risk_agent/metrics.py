"""Core credit-risk metrics, computed only from the marts star schema.

Rates are ratios in decimal form (0.05 = 5%). ``weighted_avg_interest_rate``
stays in percentage points, matching ``fact_loan.int_rate`` (13.56 means 13.56%).
``expected_loss_proxy`` is documented in the data dictionary and is not
implemented here; it is a multi-step semantic metric for a later phase.
"""

from __future__ import annotations

from typing import Any

import duckdb

# The five metrics the Phase 1 acceptance tests reconcile against pandas.
CORE_METRICS = (
    "loan_count",
    "funded_volume",
    "weighted_avg_interest_rate",
    "default_rate_matured",
    "charge_off_rate",
)

METRICS: dict[str, str] = {
    "loan_count": """
        SELECT COUNT(*)::DOUBLE
        FROM marts.fact_loan
    """,
    "funded_volume": """
        SELECT COALESCE(SUM(funded_amnt), 0)::DOUBLE
        FROM marts.fact_loan
    """,
    "avg_interest_rate": """
        SELECT AVG(int_rate)
        FROM marts.fact_loan
    """,
    "weighted_avg_interest_rate": """
        SELECT SUM(int_rate * funded_amnt) FILTER (WHERE int_rate IS NOT NULL)
             / NULLIF(SUM(funded_amnt) FILTER (WHERE int_rate IS NOT NULL), 0)
        FROM marts.fact_loan
    """,
    "default_rate_matured": """
        SELECT SUM(is_default)::DOUBLE / NULLIF(COUNT(*), 0)
        FROM marts.fact_loan
        WHERE is_matured = 1
    """,
    "charge_off_rate": """
        SELECT SUM(exposure_at_default) / NULLIF(SUM(funded_amnt), 0)
        FROM marts.fact_loan
    """,
    "delinquency_rate_active": """
        SELECT SUM(f.is_delinquent)::DOUBLE / NULLIF(COUNT(*), 0)
        FROM marts.fact_loan AS f
        JOIN marts.dim_loan_status AS s ON f.status_key = s.status_key
        WHERE s.status_group IN ('Performing', 'Delinquent')
    """,
    "delinquency_rate_balance": """
        SELECT SUM(f.out_prncp) FILTER (WHERE f.is_delinquent = 1)
             / NULLIF(SUM(f.out_prncp), 0)
        FROM marts.fact_loan AS f
        JOIN marts.dim_loan_status AS s ON f.status_key = s.status_key
        WHERE s.status_group IN ('Performing', 'Delinquent')
    """,
    "recovery_rate": """
        SELECT SUM(recoveries) / NULLIF(SUM(exposure_at_default), 0)
        FROM marts.fact_loan
    """,
    "lgd_realised": """
        SELECT 1
             - (SUM(recoveries) - SUM(collection_recovery_fee))
               / NULLIF(SUM(exposure_at_default), 0)
        FROM marts.fact_loan
    """,
    "net_loss_rate": """
        SELECT SUM(net_loss) / NULLIF(SUM(funded_amnt), 0)
        FROM marts.fact_loan
    """,
}


def compute_metrics(
    connection: duckdb.DuckDBPyConnection,
    names: tuple[str, ...] | None = None,
) -> dict[str, float | None]:
    """Run each metric query. Missing results (empty matured set, zero EAD) are None."""
    selected = names or tuple(METRICS)
    unknown = [name for name in selected if name not in METRICS]
    if unknown:
        raise KeyError(f"Unknown metric(s): {', '.join(unknown)}")
    values: dict[str, float | None] = {}
    for name in selected:
        raw: Any = connection.execute(METRICS[name]).fetchone()[0]
        values[name] = None if raw is None else float(raw)
    return values
