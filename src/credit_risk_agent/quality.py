"""Primary-key, foreign-key, and row-count checks for the marts star schema."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

# (table, column). Every key is also required to be non-null.
PRIMARY_KEYS = (
    ("dim_date", "date_key"),
    ("dim_loan_product", "product_key"),
    ("dim_borrower_profile", "borrower_profile_key"),
    ("dim_geography", "geography_key"),
    ("dim_purpose", "purpose_key"),
    ("dim_loan_status", "status_key"),
    ("fact_loan", "loan_key"),
)

# (table, column, parent_table, parent_column, nulls_allowed)
FOREIGN_KEYS = (
    ("fact_loan", "issue_date_key", "dim_date", "date_key", False),
    ("fact_loan", "last_payment_date_key", "dim_date", "date_key", True),
    ("fact_loan", "product_key", "dim_loan_product", "product_key", False),
    ("fact_loan", "borrower_profile_key", "dim_borrower_profile", "borrower_profile_key", False),
    ("fact_loan", "geography_key", "dim_geography", "geography_key", False),
    ("fact_loan", "purpose_key", "dim_purpose", "purpose_key", False),
    ("fact_loan", "status_key", "dim_loan_status", "status_key", False),
)

NOT_NULL_COLUMNS = (
    ("fact_loan", "source_loan_id"),
    ("fact_loan", "funded_amnt"),
    ("fact_loan", "is_default"),
    ("fact_loan", "is_delinquent"),
    ("fact_loan", "is_matured"),
    ("fact_loan", "exposure_at_default"),
    ("fact_loan", "net_loss"),
    ("dim_loan_product", "term_months"),
    ("dim_loan_product", "grade"),
    ("dim_loan_product", "sub_grade"),
    ("dim_loan_status", "loan_status"),
    ("dim_loan_status", "status_group"),
    ("dim_geography", "zip3"),
    ("dim_geography", "state_code"),
    ("dim_geography", "state_name"),
    ("dim_geography", "census_region"),
    ("dim_geography", "census_division"),
    ("dim_purpose", "purpose_code"),
    ("dim_purpose", "purpose_label"),
    ("dim_purpose", "purpose_group"),
)


@dataclass(frozen=True)
class QualityIssue:
    check: str
    detail: str

    def __str__(self) -> str:
        return f"{self.check}: {self.detail}"


def _count(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    value = connection.execute(sql).fetchone()[0]
    return int(value)


def run_quality_checks(connection: duckdb.DuckDBPyConnection) -> list[QualityIssue]:
    """Return every failed check. An empty list means the warehouse passed."""
    issues: list[QualityIssue] = []

    for table, column in PRIMARY_KEYS:
        duplicate_rows = _count(
            connection,
            f"""
            SELECT COUNT(*) FROM (
                SELECT {column}
                FROM marts.{table}
                GROUP BY {column}
                HAVING COUNT(*) > 1
            )
            """,
        )
        if duplicate_rows:
            issues.append(
                QualityIssue(
                    "primary_key_unique",
                    f"marts.{table}.{column} has {duplicate_rows} duplicated key(s)",
                )
            )
        nulls = _count(
            connection,
            f"SELECT COUNT(*) FROM marts.{table} WHERE {column} IS NULL",
        )
        if nulls:
            issues.append(
                QualityIssue(
                    "primary_key_not_null",
                    f"marts.{table}.{column} has {nulls} null value(s)",
                )
            )

    duplicate_ids = _count(
        connection,
        """
        SELECT COUNT(*) FROM (
            SELECT source_loan_id
            FROM marts.fact_loan
            GROUP BY source_loan_id
            HAVING COUNT(*) > 1
        )
        """,
    )
    if duplicate_ids:
        issues.append(
            QualityIssue(
                "natural_key_unique",
                f"marts.fact_loan.source_loan_id has {duplicate_ids} duplicated id(s)",
            )
        )

    for table, column in NOT_NULL_COLUMNS:
        nulls = _count(
            connection,
            f"SELECT COUNT(*) FROM marts.{table} WHERE {column} IS NULL",
        )
        if nulls:
            issues.append(
                QualityIssue(
                    "not_null",
                    f"marts.{table}.{column} has {nulls} null value(s)",
                )
            )

    for table, column, parent, parent_column, nulls_allowed in FOREIGN_KEYS:
        null_clause = f"AND child.{column} IS NOT NULL" if nulls_allowed else ""
        orphans = _count(
            connection,
            f"""
            SELECT COUNT(*)
            FROM marts.{table} AS child
            LEFT JOIN marts.{parent} AS parent
              ON child.{column} = parent.{parent_column}
            WHERE parent.{parent_column} IS NULL
              {null_clause}
            """,
        )
        if orphans:
            issues.append(
                QualityIssue(
                    "foreign_key",
                    (
                        f"marts.{table}.{column} has {orphans} value(s) "
                        f"with no marts.{parent}.{parent_column}"
                    ),
                )
            )
        if not nulls_allowed:
            nulls = _count(
                connection,
                f"SELECT COUNT(*) FROM marts.{table} WHERE {column} IS NULL",
            )
            if nulls:
                issues.append(
                    QualityIssue(
                        "foreign_key_not_null",
                        f"marts.{table}.{column} has {nulls} null value(s)",
                    )
                )

    flag_issues = _count(
        connection,
        """
        SELECT COUNT(*)
        FROM marts.fact_loan
        WHERE is_default NOT IN (0, 1)
           OR is_delinquent NOT IN (0, 1)
           OR is_matured NOT IN (0, 1)
        """,
    )
    if flag_issues:
        issues.append(
            QualityIssue("flag_domain", f"{flag_issues} fact row(s) have flags outside 0/1")
        )

    bad_exposure = _count(
        connection,
        """
        SELECT COUNT(*)
        FROM marts.fact_loan
        WHERE (
            is_default = 0
            AND (exposure_at_default <> 0 OR net_loss <> 0)
        ) OR (
            is_default = 1
            AND (
                exposure_at_default <> funded_amnt - COALESCE(total_rec_prncp, 0)
                OR net_loss <> exposure_at_default
                    - COALESCE(recoveries, 0)
                    + COALESCE(collection_recovery_fee, 0)
            )
        )
        """,
    )
    if bad_exposure:
        issues.append(
            QualityIssue(
                "loss_formula",
                f"{bad_exposure} fact row(s) break the exposure / net-loss formula",
            )
        )

    non_month_issues = _count(
        connection,
        """
        SELECT COUNT(*)
        FROM marts.fact_loan AS fact
        JOIN marts.dim_date AS issued ON fact.issue_date_key = issued.date_key
        WHERE issued.is_month_start = FALSE
        """,
    )
    if non_month_issues:
        issues.append(
            QualityIssue(
                "issue_month_start",
                f"{non_month_issues} loan(s) have an issue date that is not the first of a month",
            )
        )

    audit = connection.execute(
        """
        SELECT source_rows, fact_rows, rejected_rows
        FROM marts.pipeline_audit
        """
    ).fetchone()
    if audit is None:
        issues.append(QualityIssue("row_count", "marts.pipeline_audit has no row"))
    else:
        source_rows, fact_rows, rejected_rows = (int(value) for value in audit)
        actual_fact_rows = _count(connection, "SELECT COUNT(*) FROM marts.fact_loan")
        if actual_fact_rows != fact_rows:
            issues.append(
                QualityIssue(
                    "row_count",
                    f"pipeline_audit.fact_rows={fact_rows} but fact_loan has {actual_fact_rows}",
                )
            )
        if fact_rows + rejected_rows != source_rows:
            issues.append(
                QualityIssue(
                    "row_count",
                    (
                        "fact rows plus rejected rows "
                        f"({fact_rows} + {rejected_rows}) do not equal source rows ({source_rows})"
                    ),
                )
            )
        rejection_rows = _count(connection, "SELECT COUNT(*) FROM staging.load_rejections")
        if rejection_rows != rejected_rows:
            issues.append(
                QualityIssue(
                    "row_count",
                    (
                        f"pipeline_audit.rejected_rows={rejected_rows} but "
                        f"staging.load_rejections has {rejection_rows}"
                    ),
                )
            )

    return issues


def format_issues(issues: list[QualityIssue]) -> str:
    if not issues:
        return "Quality checks passed."
    lines = [f"{len(issues)} quality check(s) failed:"]
    lines.extend(f"- {issue}" for issue in issues)
    return "\n".join(lines)
