"""Star-schema constraints, documented filters, and a few hand-checked loans."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from credit_risk_agent.config import DROPPED_FREE_TEXT_COLUMNS, REQUIRED_COLUMNS
from credit_risk_agent.pipeline import BuildReport, build_warehouse
from credit_risk_agent.quality import run_quality_checks

EXPECTED_REJECTIONS = {
    "missing_loan_id": 1,
    "duplicate_loan_id": 1,
    "missing_issue_date": 1,
    "missing_loan_status": 1,
    "missing_funded_amnt": 1,
    "invalid_term": 1,
    "negative_funded_amnt": 1,
}


def _one(connection: duckdb.DuckDBPyConnection, sql: str, params: list[object]) -> dict[str, object]:
    row = connection.execute(sql, params).fetchone()
    assert row is not None
    columns = [item[0] for item in connection.description]
    return dict(zip(columns, row, strict=True))


def test_fixture_uses_real_column_names_and_includes_free_text(fixture_csv: Path) -> None:
    with fixture_csv.open(newline="", encoding="utf-8") as handle:
        header = next(csv.reader(handle))
    for column in REQUIRED_COLUMNS:
        assert column in header
    for column in DROPPED_FREE_TEXT_COLUMNS:
        assert column in header
    text = fixture_csv.read_text(encoding="utf-8")
    assert "EmpTitleSecret" in text
    assert "personal note" in text
    assert "TitleSecret" in text
    assert "example.invalid" in text


def test_quality_checks_pass(connection: duckdb.DuckDBPyConnection) -> None:
    assert run_quality_checks(connection) == []


def test_row_accounting(build_report: BuildReport) -> None:
    assert build_report.fact_rows == 28
    assert build_report.rejected_rows == 7
    assert build_report.source_rows == 35
    assert build_report.rejections_by_reason == EXPECTED_REJECTIONS
    assert build_report.fact_rows + build_report.rejected_rows == build_report.source_rows


def test_duplicate_keeps_the_first_file_row(connection: duckdb.DuckDBPyConnection) -> None:
    row = _one(
        connection,
        """
        SELECT funded_amnt, int_rate
        FROM marts.fact_loan
        WHERE source_loan_id = ?
        """,
        ["L-DUP"],
    )
    assert row["funded_amnt"] == 1000
    assert row["int_rate"] == 10


def test_loss_and_maturity_flags(connection: duckdb.DuckDBPyConnection) -> None:
    charged = _one(
        connection,
        """
        SELECT is_default, is_delinquent, is_matured, exposure_at_default, net_loss, int_rate
        FROM marts.fact_loan WHERE source_loan_id = ?
        """,
        ["L-CO-1"],
    )
    assert charged["is_default"] == 1
    assert charged["is_delinquent"] == 0
    assert charged["is_matured"] == 1
    assert charged["exposure_at_default"] == 10000
    assert charged["net_loss"] == 9550

    policy = _one(
        connection,
        """
        SELECT fact.is_default, fact.is_matured, fact.exposure_at_default, fact.net_loss,
               status.credit_policy_flag, status.status_group
        FROM marts.fact_loan AS fact
        JOIN marts.dim_loan_status AS status ON fact.status_key = status.status_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-POLICY-CO"],
    )
    assert policy["is_default"] == 1
    assert policy["credit_policy_flag"] is True
    assert policy["status_group"] == "Default"
    assert policy["exposure_at_default"] == 2000
    assert policy["net_loss"] == 1910
    employer = _one(
        connection,
        """
        SELECT borrower.emp_length, borrower.fico_band
        FROM marts.fact_loan AS fact
        JOIN marts.dim_borrower_profile AS borrower
          ON fact.borrower_profile_key = borrower.borrower_profile_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-POLICY-CO"],
    )
    assert employer["emp_length"] == "Unknown"
    assert employer["fico_band"] == "<660"

    paid_policy = _one(
        connection,
        """
        SELECT fact.is_default, status.credit_policy_flag, status.status_group, status.is_terminal
        FROM marts.fact_loan AS fact
        JOIN marts.dim_loan_status AS status ON fact.status_key = status.status_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-POLICY-PAID"],
    )
    assert paid_policy["is_default"] == 0
    assert paid_policy["credit_policy_flag"] is True
    assert paid_policy["status_group"] == "Closed-Paid"
    assert paid_policy["is_terminal"] is True

    assert _one(
        connection,
        "SELECT is_matured, is_default, exposure_at_default FROM marts.fact_loan WHERE source_loan_id = ?",
        ["L-MATURE-EDGE"],
    )["is_matured"] == 1
    assert _one(
        connection,
        "SELECT is_matured FROM marts.fact_loan WHERE source_loan_id = ?",
        ["L-IMMATURE-EDGE"],
    )["is_matured"] == 0
    assert _one(
        connection,
        "SELECT is_matured, is_default FROM marts.fact_loan WHERE source_loan_id = ?",
        ["L-TERM60-MATURE"],
    )["is_matured"] == 1
    assert _one(
        connection,
        "SELECT is_matured FROM marts.fact_loan WHERE source_loan_id = ?",
        ["L-TERM60-IMMATURE"],
    )["is_matured"] == 0

    late = _one(
        connection,
        """
        SELECT fact.is_delinquent, fact.is_default, status.status_group
        FROM marts.fact_loan AS fact
        JOIN marts.dim_loan_status AS status ON fact.status_key = status.status_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-LATE"],
    )
    assert late["is_delinquent"] == 1
    assert late["is_default"] == 0
    assert late["status_group"] == "Delinquent"

    unknown = _one(
        connection,
        """
        SELECT fact.is_default, fact.is_delinquent, fact.is_matured, status.status_group, status.is_terminal
        FROM marts.fact_loan AS fact
        JOIN marts.dim_loan_status AS status ON fact.status_key = status.status_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-UNKNOWN-STATUS"],
    )
    assert unknown["status_group"] == "Unknown"
    assert unknown["is_default"] == 0
    assert unknown["is_delinquent"] == 0
    assert unknown["is_terminal"] is False
    assert unknown["is_matured"] == 1


def test_percent_rate_grade_and_null_last_payment(connection: duckdb.DuckDBPyConnection) -> None:
    percent = _one(
        connection,
        "SELECT int_rate FROM marts.fact_loan WHERE source_loan_id = ?",
        ["L-PERCENT-RATE"],
    )
    assert percent["int_rate"] == 15.25

    derived = _one(
        connection,
        """
        SELECT product.grade, product.sub_grade, product.grade_rank
        FROM marts.fact_loan AS fact
        JOIN marts.dim_loan_product AS product ON fact.product_key = product.product_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-GRADE-DERIVED"],
    )
    assert derived["grade"] == "D"
    assert derived["sub_grade"] == "D4"
    assert derived["grade_rank"] == 4

    issued = _one(
        connection,
        "SELECT last_payment_date_key, is_matured FROM marts.fact_loan WHERE source_loan_id = ?",
        ["L-ISSUED"],
    )
    assert issued["last_payment_date_key"] is None
    assert issued["is_matured"] == 0

    blank_rate = _one(
        connection,
        "SELECT int_rate FROM marts.fact_loan WHERE source_loan_id = ?",
        ["L-NO-RATE"],
    )
    assert blank_rate["int_rate"] is None


def test_borrower_geography_and_purpose(connection: duckdb.DuckDBPyConnection) -> None:
    paid = _one(
        connection,
        """
        SELECT borrower.fico_band, borrower.income_band, borrower.dti_band, borrower.emp_length,
               borrower.home_ownership, borrower.verification_status, borrower.application_type,
               fact.fico_mid,
               geography.state_code, geography.census_region, geography.census_division,
               purpose.purpose_code, purpose.purpose_group
        FROM marts.fact_loan AS fact
        JOIN marts.dim_borrower_profile AS borrower
          ON fact.borrower_profile_key = borrower.borrower_profile_key
        JOIN marts.dim_geography AS geography ON fact.geography_key = geography.geography_key
        JOIN marts.dim_purpose AS purpose ON fact.purpose_key = purpose.purpose_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-PAID-1"],
    )
    assert paid["fico_mid"] == 692
    assert paid["fico_band"] == "680-699"
    assert paid["income_band"] == "50-74k"
    assert paid["dti_band"] == "10-19"
    assert paid["emp_length"] == "10+ years"
    assert paid["home_ownership"] == "MORTGAGE"
    assert paid["state_code"] == "CA"
    assert paid["census_region"] == "West"
    assert paid["census_division"] == "Pacific"
    assert paid["purpose_group"] == "Debt"

    unknown_state = _one(
        connection,
        """
        SELECT geography.state_code, geography.state_name, geography.census_region,
               geography.census_division, geography.zip3
        FROM marts.fact_loan AS fact
        JOIN marts.dim_geography AS geography ON fact.geography_key = geography.geography_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-UNKNOWN-STATE"],
    )
    assert unknown_state["state_code"] == "ZZ"
    assert unknown_state["state_name"] == "Unknown"
    assert unknown_state["census_region"] == "Unknown"
    assert unknown_state["zip3"] == "000"

    military = _one(
        connection,
        """
        SELECT geography.census_region, geography.census_division
        FROM marts.fact_loan AS fact
        JOIN marts.dim_geography AS geography ON fact.geography_key = geography.geography_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-MILITARY"],
    )
    assert military["census_region"] == "Armed Forces"
    assert military["census_division"] == "Overseas Military"

    lowered = _one(
        connection,
        """
        SELECT geography.state_code, geography.zip3, borrower.application_type, borrower.home_ownership,
               borrower.dti_band, borrower.fico_band, borrower.income_band, purpose.purpose_group
        FROM marts.fact_loan AS fact
        JOIN marts.dim_geography AS geography ON fact.geography_key = geography.geography_key
        JOIN marts.dim_borrower_profile AS borrower
          ON fact.borrower_profile_key = borrower.borrower_profile_key
        JOIN marts.dim_purpose AS purpose ON fact.purpose_key = purpose.purpose_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-LOWER-STATE"],
    )
    assert lowered["state_code"] == "CA"
    assert lowered["zip3"] == "941"
    assert lowered["application_type"] == "Joint App"
    assert lowered["home_ownership"] == "ANY"
    assert lowered["dti_band"] == "10-19"
    assert lowered["fico_band"] == "700-719"
    assert lowered["income_band"] == "75-99k"
    assert lowered["purpose_group"] == "Business"

    blank = _one(
        connection,
        """
        SELECT borrower.fico_band, borrower.income_band, borrower.dti_band, borrower.emp_length,
               borrower.home_ownership, borrower.application_type,
               purpose.purpose_code, purpose.purpose_label, purpose.purpose_group,
               geography.zip3
        FROM marts.fact_loan AS fact
        JOIN marts.dim_borrower_profile AS borrower
          ON fact.borrower_profile_key = borrower.borrower_profile_key
        JOIN marts.dim_purpose AS purpose ON fact.purpose_key = purpose.purpose_key
        JOIN marts.dim_geography AS geography ON fact.geography_key = geography.geography_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-BLANK-PURPOSE"],
    )
    assert blank["fico_band"] == "Unknown"
    assert blank["income_band"] == "Unknown"
    assert blank["dti_band"] == "Unknown"
    assert blank["emp_length"] == "Unknown"
    assert blank["home_ownership"] == "Unknown"
    assert blank["application_type"] == "Unknown"
    assert blank["purpose_code"] == "unknown"
    assert blank["purpose_label"] == "Unknown"
    assert blank["purpose_group"] == "Other"
    assert blank["zip3"] == "UNK"

    joint = _one(
        connection,
        """
        SELECT borrower.application_type, borrower.dti_band, borrower.income_band
        FROM marts.fact_loan AS fact
        JOIN marts.dim_borrower_profile AS borrower
          ON fact.borrower_profile_key = borrower.borrower_profile_key
        WHERE fact.source_loan_id = ?
        """,
        ["L-JOINT-PAID"],
    )
    assert joint["application_type"] == "Joint App"
    assert joint["dti_band"] == "20-29"
    assert joint["income_band"] == "30-49k"


def test_date_dimension_covers_snapshot_and_history(connection: duckdb.DuckDBPyConnection) -> None:
    january = _one(
        connection,
        """
        SELECT date_key, "date", year, quarter, month, year_month, month_name, is_month_start
        FROM marts.dim_date
        WHERE date_key = 20150101
        """,
        [],
    )
    assert january["year"] == 2015
    assert january["quarter"] == 1
    assert january["month"] == 1
    assert january["year_month"] == "2015-01"
    assert january["month_name"] == "January"
    assert january["is_month_start"] is True

    mid_month = _one(
        connection,
        'SELECT is_month_start FROM marts.dim_date WHERE "date" = DATE \'2015-01-02\'',
        [],
    )
    assert mid_month["is_month_start"] is False

    span = _one(
        connection,
        'SELECT min("date") AS min_date, max("date") AS max_date, count(*) AS days FROM marts.dim_date',
        [],
    )
    assert str(span["min_date"]) == "2007-06-01"
    assert str(span["max_date"]) == "2019-03-31"
    assert span["days"] > 365 * 10


def test_rejected_ids_are_absent_and_keys_are_stable(tmp_path: Path, fixture_csv: Path) -> None:
    first = tmp_path / "one.duckdb"
    second = tmp_path / "two.duckdb"
    build_warehouse(fixture_csv, first)
    build_warehouse(fixture_csv, second)
    with duckdb.connect(str(first), read_only=True) as left, duckdb.connect(
        str(second), read_only=True
    ) as right:
        absent = left.execute(
            """
            SELECT COUNT(*)
            FROM marts.fact_loan
            WHERE source_loan_id IN ('L-NO-ISSUE', 'L-NO-STATUS', 'L-NO-FUNDED', 'L-BAD-TERM', 'L-NEG-FUNDED')
            """
        ).fetchone()[0]
        assert absent == 0
        left_keys = left.execute(
            "SELECT source_loan_id, loan_key FROM marts.fact_loan ORDER BY source_loan_id"
        ).fetchall()
        right_keys = right.execute(
            "SELECT source_loan_id, loan_key FROM marts.fact_loan ORDER BY source_loan_id"
        ).fetchall()
        assert left_keys == right_keys


def test_quoted_newline_in_a_dropped_column_does_not_shift_the_next_loan(tmp_path: Path) -> None:
    """The real extract stores free text with commas and newlines. Those fields are not loaded."""
    source = tmp_path / "quoted.csv"
    header = [
        "id",
        "loan_amnt",
        "funded_amnt",
        "term",
        "int_rate",
        "installment",
        "grade",
        "sub_grade",
        "emp_title",
        "emp_length",
        "home_ownership",
        "annual_inc",
        "verification_status",
        "issue_d",
        "loan_status",
        "url",
        "desc",
        "purpose",
        "title",
        "zip_code",
        "addr_state",
        "dti",
        "fico_range_low",
        "fico_range_high",
        "total_pymnt",
        "total_rec_prncp",
        "total_rec_int",
        "recoveries",
        "collection_recovery_fee",
        "out_prncp",
        "last_pymnt_d",
        "application_type",
    ]
    first = {column: "" for column in header}
    first.update(
        {
            "id": "L-QUOTED",
            "loan_amnt": "1000",
            "funded_amnt": "1000",
            "term": " 36 months",
            "int_rate": "10",
            "grade": "A",
            "sub_grade": "A1",
            "emp_title": "EmpTitleSecret",
            "issue_d": "Jan-2015",
            "loan_status": "Fully Paid",
            "desc": "personal note\nstill the note, with a comma",
            "purpose": "debt_consolidation",
            "title": "TitleSecret",
            "url": "https://example.invalid/loan",
            "zip_code": "941xx",
            "addr_state": "CA",
            "total_rec_prncp": "1000",
            "application_type": "Individual",
        }
    )
    second = {column: "" for column in header}
    second.update(
        {
            "id": "L-AFTER",
            "loan_amnt": "42",
            "funded_amnt": "42",
            "term": " 36 months",
            "int_rate": "9",
            "grade": "B",
            "sub_grade": "B1",
            "issue_d": "Jan-2015",
            "loan_status": "Fully Paid",
            "purpose": "car",
            "zip_code": "100xx",
            "addr_state": "NY",
            "total_rec_prncp": "42",
            "application_type": "Individual",
        }
    )
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows([first, second])

    warehouse = tmp_path / "quoted.duckdb"
    report = build_warehouse(source, warehouse)
    assert report.fact_rows == 2
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        funded = connection.execute(
            "SELECT funded_amnt FROM marts.fact_loan WHERE source_loan_id = 'L-AFTER'"
        ).fetchone()[0]
        assert funded == 42
        leaked = connection.execute(
            """
            SELECT COUNT(*)
            FROM marts.fact_loan
            WHERE CAST(source_loan_id AS VARCHAR) ILIKE '%personal note%'
            """
        ).fetchone()[0]
        assert leaked == 0


def test_missing_column_fails(tmp_path: Path) -> None:
    from credit_risk_agent.pipeline import PipelineError

    source = tmp_path / "missing.csv"
    source.write_text("id,loan_amnt\n1,10\n", encoding="utf-8")
    try:
        build_warehouse(source, tmp_path / "should_not_exist.duckdb")
    except PipelineError as exc:
        assert "issue_d" in str(exc) or "required" in str(exc).lower() or "Failed to read" in str(exc)
    else:
        raise AssertionError("build should fail when required columns are missing")
    assert not (tmp_path / "should_not_exist.duckdb").exists()
