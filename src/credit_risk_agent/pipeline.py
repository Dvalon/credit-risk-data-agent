"""Build the LendingClub accepted-loans star schema in a local DuckDB file.

The transform is staging (typed, filtered loans) then marts (dimensions and
``fact_loan``). Free-text columns are never selected from the source CSV.
"""

from __future__ import annotations

import csv
import gzip
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from credit_risk_agent.config import (
    ACCEPTED_CSV_NAME,
    DEFAULT_SOURCE,
    DEFAULT_WAREHOUSE,
    EXPECTED_ACCEPTED_COLUMNS,
    EXPECTED_ACCEPTED_DATA_ROWS,
    LOAN_STATUS_CSV,
    PURPOSE_GROUPS_CSV,
    RAW_DIR,
    REQUIRED_COLUMNS,
    SNAPSHOT_DATE,
    US_STATES_CSV,
)
from credit_risk_agent.download import (
    credential_source,
    download_accepted,
    load_env,
    missing_source_message,
)
from credit_risk_agent.quality import format_issues, run_quality_checks

# CSV scan is single-threaded so "first row wins" on a duplicate id follows file order.
_READ_CSV = """
read_csv(
    ?,
    header = true,
    all_varchar = true,
    parallel = false,
    nullstr = [''],
    quote = '"',
    escape = '"',
    strict_mode = true,
    maximum_line_size = 10000000
)
"""


class PipelineError(RuntimeError):
    """The warehouse could not be built or failed its quality checks."""


class SourceNotFoundError(PipelineError):
    """The accepted-loans extract is not on disk and could not be downloaded."""


@dataclass(frozen=True)
class BuildReport:
    warehouse_path: Path
    source_name: str
    snapshot_date: date
    source_rows: int
    fact_rows: int
    rejected_rows: int
    rejections_by_reason: dict[str, int]

    def summary(self) -> str:
        reasons = ", ".join(
            f"{reason}={count}" for reason, count in sorted(self.rejections_by_reason.items())
        )
        reason_text = reasons or "none"
        return (
            f"Warehouse: {self.warehouse_path}\n"
            f"Source: {self.source_name}\n"
            f"Snapshot: {self.snapshot_date.isoformat()}\n"
            f"Rows: source={self.source_rows} fact={self.fact_rows} "
            f"rejected={self.rejected_rows} ({reason_text})"
        )


def resolve_source(explicit: Path | None, *, allow_download: bool) -> Path:
    """Pick the CSV to load, downloading the Kaggle extract when that is possible."""
    if explicit is not None:
        source = explicit.expanduser().resolve()
        if not source.is_file():
            raise SourceNotFoundError(f"Source file not found: {source}")
        return source

    load_env()
    canonical = DEFAULT_SOURCE
    gzip_path = RAW_DIR / f"{ACCEPTED_CSV_NAME}.gz"
    if canonical.is_file():
        return canonical
    if gzip_path.is_file():
        return gzip_path
    if allow_download and credential_source() is not None:
        downloaded = download_accepted()
        if downloaded.is_file():
            return downloaded
    raise SourceNotFoundError(missing_source_message())


def build_warehouse(
    source: Path,
    warehouse_path: Path,
    snapshot: date = SNAPSHOT_DATE,
    *,
    run_checks: bool = True,
) -> BuildReport:
    """Rebuild ``warehouse_path`` from ``source``. The previous file is replaced only on success."""
    source = source.expanduser().resolve()
    if not source.is_file():
        raise SourceNotFoundError(f"Source file not found: {source}")
    warehouse_path = warehouse_path.expanduser().resolve()
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    _warn_if_unexpected_extract(source)

    temporary = Path(str(warehouse_path) + ".building")
    _remove_database(temporary)
    connection = duckdb.connect(str(temporary))
    try:
        connection.execute("SET preserve_insertion_order = true")
        report = _build(connection, source, snapshot)
        if run_checks:
            issues = run_quality_checks(connection)
            if issues:
                raise PipelineError(format_issues(issues))
        connection.execute("CHECKPOINT")
    except Exception:
        connection.close()
        _remove_database(temporary)
        raise
    connection.close()

    _remove_database(warehouse_path)
    temporary.replace(warehouse_path)
    building_wal = Path(str(temporary) + ".wal")
    if building_wal.exists():
        building_wal.unlink()
    return BuildReport(
        warehouse_path=warehouse_path,
        source_name=report.source_name,
        snapshot_date=report.snapshot_date,
        source_rows=report.source_rows,
        fact_rows=report.fact_rows,
        rejected_rows=report.rejected_rows,
        rejections_by_reason=report.rejections_by_reason,
    )


def _build(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    snapshot: date,
) -> BuildReport:
    connection.execute("CREATE SCHEMA IF NOT EXISTS staging")
    connection.execute("CREATE SCHEMA IF NOT EXISTS marts")
    connection.execute(
        "CREATE TEMP TABLE params AS SELECT ?::DATE AS snapshot_date",
        [snapshot.isoformat()],
    )
    _load_seeds(connection)
    try:
        connection.execute(
            f"""
            CREATE TEMP TABLE parsed AS
            SELECT
                row_number() OVER () AS source_row_number,
                NULLIF(trim(id), '') AS source_loan_id,
                try_cast(loan_amnt AS DOUBLE) AS loan_amnt,
                try_cast(funded_amnt AS DOUBLE) AS funded_amnt,
                try_cast(
                    NULLIF(regexp_extract(COALESCE(term, ''), '([0-9]+)', 1), '')
                    AS INTEGER
                ) AS term_months,
                try_cast(
                    NULLIF(replace(replace(trim(int_rate), '%', ''), ' ', ''), '')
                    AS DOUBLE
                ) AS int_rate,
                try_cast(installment AS DOUBLE) AS installment,
                CASE
                    WHEN upper(trim(grade)) IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')
                        THEN upper(trim(grade))
                    WHEN substr(upper(trim(sub_grade)), 1, 1) IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')
                        THEN substr(upper(trim(sub_grade)), 1, 1)
                    ELSE 'Unknown'
                END AS grade,
                CASE
                    WHEN sub_grade IS NULL OR trim(sub_grade) = '' THEN 'Unknown'
                    ELSE upper(trim(sub_grade))
                END AS sub_grade,
                CASE
                    WHEN emp_length IS NULL OR trim(emp_length) = '' THEN 'Unknown'
                    WHEN lower(trim(emp_length)) IN ('n/a', 'na', 'none', 'null') THEN 'Unknown'
                    WHEN lower(trim(emp_length)) IN ('< 1 year', '<1 year', '< 1 years')
                        THEN '< 1 year'
                    WHEN lower(trim(emp_length)) IN ('10+ years', '10+ year', '10+years')
                        THEN '10+ years'
                    ELSE trim(emp_length)
                END AS emp_length,
                CASE
                    WHEN home_ownership IS NULL OR trim(home_ownership) = '' THEN 'Unknown'
                    ELSE upper(trim(home_ownership))
                END AS home_ownership,
                try_cast(annual_inc AS DOUBLE) AS annual_inc,
                CASE
                    WHEN verification_status IS NULL OR trim(verification_status) = '' THEN 'Unknown'
                    WHEN lower(trim(verification_status)) = 'verified' THEN 'Verified'
                    WHEN lower(trim(verification_status)) = 'source verified' THEN 'Source Verified'
                    WHEN lower(trim(verification_status)) = 'not verified' THEN 'Not Verified'
                    ELSE trim(verification_status)
                END AS verification_status,
                try_strptime(trim(issue_d), '%b-%Y')::DATE AS issue_date,
                NULLIF(trim(loan_status), '') AS loan_status,
                CASE
                    WHEN purpose IS NULL OR trim(purpose) = '' THEN 'unknown'
                    ELSE lower(trim(purpose))
                END AS purpose_code,
                CASE
                    WHEN regexp_extract(COALESCE(zip_code, ''), '([0-9]{{3}})', 1) = '' THEN 'UNK'
                    ELSE regexp_extract(zip_code, '([0-9]{{3}})', 1)
                END AS zip3,
                CASE
                    WHEN addr_state IS NULL OR trim(addr_state) = '' THEN 'UNK'
                    ELSE upper(trim(addr_state))
                END AS state_code,
                try_cast(dti AS DOUBLE) AS dti,
                try_cast(fico_range_low AS DOUBLE) AS fico_range_low,
                try_cast(fico_range_high AS DOUBLE) AS fico_range_high,
                try_cast(total_pymnt AS DOUBLE) AS total_pymnt,
                try_cast(total_rec_prncp AS DOUBLE) AS total_rec_prncp,
                try_cast(total_rec_int AS DOUBLE) AS total_rec_int,
                try_cast(recoveries AS DOUBLE) AS recoveries,
                try_cast(collection_recovery_fee AS DOUBLE) AS collection_recovery_fee,
                try_cast(out_prncp AS DOUBLE) AS out_prncp,
                try_strptime(trim(last_pymnt_d), '%b-%Y')::DATE AS last_payment_date,
                CASE
                    WHEN application_type IS NULL OR trim(application_type) = '' THEN 'Unknown'
                    WHEN lower(trim(application_type)) = 'individual' THEN 'Individual'
                    WHEN lower(replace(trim(application_type), '_', ' ')) IN ('joint app', 'joint')
                        THEN 'Joint App'
                    ELSE trim(application_type)
                END AS application_type
            FROM {_READ_CSV}
            """,
            [str(source)],
        )
    except duckdb.Error as exc:
        raise PipelineError(
            "Failed to read the source CSV. The accepted-loans file must include these "
            f"columns: {', '.join(REQUIRED_COLUMNS)}.\nDuckDB reported: {exc}"
        ) from exc

    connection.execute(
        """
        CREATE TEMP TABLE classified AS
        SELECT
            *,
            CASE
                WHEN source_loan_id IS NULL THEN 'missing_loan_id'
                WHEN issue_date IS NULL THEN 'missing_issue_date'
                WHEN loan_status IS NULL THEN 'missing_loan_status'
                WHEN funded_amnt IS NULL THEN 'missing_funded_amnt'
                WHEN funded_amnt < 0 THEN 'negative_funded_amnt'
                WHEN term_months IS NULL OR term_months NOT IN (36, 60) THEN 'invalid_term'
                ELSE NULL
            END AS reject_reason
        FROM parsed
        """
    )
    connection.execute(
        """
        CREATE TABLE staging.load_rejections (
            source_row_number BIGINT PRIMARY KEY,
            source_loan_id VARCHAR,
            reject_reason VARCHAR NOT NULL
        )
        """
    )
    connection.execute(
        """
        INSERT INTO staging.load_rejections
        SELECT source_row_number, source_loan_id, reject_reason
        FROM classified
        WHERE reject_reason IS NOT NULL
        UNION ALL
        SELECT source_row_number, source_loan_id, 'duplicate_loan_id'
        FROM (
            SELECT
                source_row_number,
                source_loan_id,
                row_number() OVER (
                    PARTITION BY source_loan_id
                    ORDER BY source_row_number
                ) AS rn
            FROM classified
            WHERE reject_reason IS NULL
        )
        WHERE rn > 1
        """
    )
    connection.execute(
        """
        CREATE TEMP TABLE loan AS
        SELECT
            eligible.*,
            CASE
                WHEN fico_mid IS NULL THEN 'Unknown'
                WHEN fico_mid < 660 THEN '<660'
                WHEN fico_mid < 680 THEN '660-679'
                WHEN fico_mid < 700 THEN '680-699'
                WHEN fico_mid < 720 THEN '700-719'
                WHEN fico_mid < 740 THEN '720-739'
                WHEN fico_mid < 760 THEN '740-759'
                WHEN fico_mid < 780 THEN '760-779'
                WHEN fico_mid < 800 THEN '780-799'
                ELSE '800+'
            END AS fico_band,
            CASE
                WHEN annual_inc IS NULL OR annual_inc < 0 THEN 'Unknown'
                WHEN annual_inc < 30000 THEN '<30k'
                WHEN annual_inc < 50000 THEN '30-49k'
                WHEN annual_inc < 75000 THEN '50-74k'
                WHEN annual_inc < 100000 THEN '75-99k'
                WHEN annual_inc < 150000 THEN '100-149k'
                WHEN annual_inc < 200000 THEN '150-199k'
                ELSE '200k+'
            END AS income_band,
            CASE
                WHEN dti IS NULL OR dti < 0 THEN 'Unknown'
                WHEN dti < 10 THEN '<10'
                WHEN dti < 20 THEN '10-19'
                WHEN dti < 30 THEN '20-29'
                WHEN dti < 40 THEN '30-39'
                ELSE '40+'
            END AS dti_band,
            COALESCE(status_map.status_group, 'Unknown') AS status_group,
            COALESCE(status_map.is_terminal, FALSE) AS is_terminal,
            COALESCE(status_map.credit_policy_flag, FALSE) AS credit_policy_flag,
            COALESCE(status_map.is_default, 0) AS is_default,
            COALESCE(status_map.is_delinquent, 0) AS is_delinquent
        FROM (
            SELECT
                classified.*,
                CASE
                    WHEN fico_range_low IS NOT NULL AND fico_range_high IS NOT NULL
                        THEN CAST(round((fico_range_low + fico_range_high) / 2.0) AS INTEGER)
                    WHEN fico_range_low IS NOT NULL THEN CAST(round(fico_range_low) AS INTEGER)
                    WHEN fico_range_high IS NOT NULL THEN CAST(round(fico_range_high) AS INTEGER)
                    ELSE NULL
                END AS fico_mid
            FROM classified
            WHERE reject_reason IS NULL
              AND source_row_number NOT IN (
                  SELECT source_row_number FROM staging.load_rejections
              )
        ) AS eligible
        LEFT JOIN status_map
          ON eligible.loan_status = status_map.loan_status
        """
    )

    kept = connection.execute("SELECT COUNT(*) FROM loan").fetchone()[0]
    if int(kept) == 0:
        raise PipelineError(
            "No loans remained after filtering. Refusing to build an empty fact table."
        )

    _build_dimensions(connection)
    _build_fact(connection)
    return _write_audit(connection, source.name, snapshot)


def _load_seeds(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        "CREATE TEMP TABLE seed_state AS SELECT * FROM read_csv(?, header = true, all_varchar = true)",
        [str(US_STATES_CSV)],
    )
    connection.execute(
        "CREATE TEMP TABLE seed_purpose AS SELECT * FROM read_csv(?, header = true, all_varchar = true)",
        [str(PURPOSE_GROUPS_CSV)],
    )
    connection.execute(
        """
        CREATE TEMP TABLE status_map AS
        SELECT
            loan_status,
            status_group,
            COALESCE(try_cast(is_terminal AS BOOLEAN), FALSE) AS is_terminal,
            COALESCE(try_cast(credit_policy_flag AS BOOLEAN), FALSE) AS credit_policy_flag,
            COALESCE(try_cast(is_default AS INTEGER), 0) AS is_default,
            COALESCE(try_cast(is_delinquent AS INTEGER), 0) AS is_delinquent
        FROM read_csv(?, header = true, all_varchar = true)
        """,
        [str(LOAN_STATUS_CSV)],
    )


def _build_dimensions(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE marts.dim_date (
            date_key INTEGER PRIMARY KEY,
            "date" DATE NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year_month VARCHAR NOT NULL,
            month_name VARCHAR NOT NULL,
            is_month_start BOOLEAN NOT NULL
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.dim_date
        WITH bounds AS (
            SELECT
                min(observed) AS min_d,
                greatest(max(observed), (SELECT snapshot_date FROM params)) AS max_d
            FROM (
                SELECT issue_date AS observed FROM loan
                UNION ALL
                SELECT last_payment_date FROM loan WHERE last_payment_date IS NOT NULL
            )
        ),
        days AS (
            SELECT unnest(generate_series(
                (SELECT min_d FROM bounds),
                (SELECT max_d FROM bounds),
                INTERVAL 1 DAY
            ))::DATE AS calendar_date
        )
        SELECT
            CAST(strftime(calendar_date, '%Y%m%d') AS INTEGER) AS date_key,
            calendar_date AS "date",
            year(calendar_date) AS year,
            quarter(calendar_date) AS quarter,
            month(calendar_date) AS month,
            strftime(calendar_date, '%Y-%m') AS year_month,
            monthname(calendar_date) AS month_name,
            day(calendar_date) = 1 AS is_month_start
        FROM days
        """
    )
    connection.execute(
        """
        CREATE TABLE marts.dim_loan_product (
            product_key INTEGER PRIMARY KEY,
            term_months INTEGER NOT NULL,
            grade VARCHAR NOT NULL,
            sub_grade VARCHAR NOT NULL,
            grade_rank INTEGER,
            UNIQUE (term_months, grade, sub_grade)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.dim_loan_product
        SELECT
            CAST(row_number() OVER (ORDER BY term_months, grade, sub_grade) AS INTEGER),
            term_months,
            grade,
            sub_grade,
            CASE grade
                WHEN 'A' THEN 1
                WHEN 'B' THEN 2
                WHEN 'C' THEN 3
                WHEN 'D' THEN 4
                WHEN 'E' THEN 5
                WHEN 'F' THEN 6
                WHEN 'G' THEN 7
                ELSE NULL
            END AS grade_rank
        FROM (
            SELECT DISTINCT term_months, grade, sub_grade FROM loan
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE marts.dim_borrower_profile (
            borrower_profile_key INTEGER PRIMARY KEY,
            fico_band VARCHAR NOT NULL,
            income_band VARCHAR NOT NULL,
            dti_band VARCHAR NOT NULL,
            emp_length VARCHAR NOT NULL,
            home_ownership VARCHAR NOT NULL,
            verification_status VARCHAR NOT NULL,
            application_type VARCHAR NOT NULL,
            UNIQUE (
                fico_band, income_band, dti_band, emp_length,
                home_ownership, verification_status, application_type
            )
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.dim_borrower_profile
        SELECT
            CAST(row_number() OVER (
                ORDER BY fico_band, income_band, dti_band, emp_length,
                         home_ownership, verification_status, application_type
            ) AS INTEGER),
            fico_band,
            income_band,
            dti_band,
            emp_length,
            home_ownership,
            verification_status,
            application_type
        FROM (
            SELECT DISTINCT
                fico_band, income_band, dti_band, emp_length,
                home_ownership, verification_status, application_type
            FROM loan
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE marts.dim_geography (
            geography_key INTEGER PRIMARY KEY,
            zip3 VARCHAR NOT NULL,
            state_code VARCHAR NOT NULL,
            state_name VARCHAR NOT NULL,
            census_region VARCHAR NOT NULL,
            census_division VARCHAR NOT NULL,
            UNIQUE (zip3, state_code)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.dim_geography
        SELECT
            CAST(row_number() OVER (ORDER BY state_code, zip3) AS INTEGER),
            zip3,
            state_code,
            state_name,
            census_region,
            census_division
        FROM (
            SELECT DISTINCT
                loan.zip3,
                loan.state_code,
                COALESCE(seed_state.state_name, 'Unknown') AS state_name,
                COALESCE(seed_state.census_region, 'Unknown') AS census_region,
                COALESCE(seed_state.census_division, 'Unknown') AS census_division
            FROM loan
            LEFT JOIN seed_state ON loan.state_code = seed_state.state_code
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE marts.dim_purpose (
            purpose_key INTEGER PRIMARY KEY,
            purpose_code VARCHAR NOT NULL UNIQUE,
            purpose_label VARCHAR NOT NULL,
            purpose_group VARCHAR NOT NULL
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.dim_purpose
        SELECT
            CAST(row_number() OVER (ORDER BY codes.purpose_code) AS INTEGER),
            codes.purpose_code,
            CASE
                WHEN codes.purpose_code = 'unknown' THEN 'Unknown'
                ELSE COALESCE(seed_purpose.purpose_label, replace(codes.purpose_code, '_', ' '))
            END AS purpose_label,
            COALESCE(seed_purpose.purpose_group, 'Other') AS purpose_group
        FROM (
            SELECT DISTINCT purpose_code FROM loan
        ) AS codes
        LEFT JOIN seed_purpose ON codes.purpose_code = seed_purpose.purpose_code
        """
    )
    connection.execute(
        """
        CREATE TABLE marts.dim_loan_status (
            status_key INTEGER PRIMARY KEY,
            loan_status VARCHAR NOT NULL UNIQUE,
            status_group VARCHAR NOT NULL,
            is_terminal BOOLEAN NOT NULL,
            credit_policy_flag BOOLEAN NOT NULL
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.dim_loan_status
        SELECT
            CAST(row_number() OVER (ORDER BY loan_status) AS INTEGER),
            loan_status,
            status_group,
            is_terminal,
            credit_policy_flag
        FROM (
            SELECT DISTINCT loan_status, status_group, is_terminal, credit_policy_flag
            FROM loan
        )
        """
    )


def _build_fact(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE marts.fact_loan (
            loan_key BIGINT PRIMARY KEY,
            source_loan_id VARCHAR NOT NULL UNIQUE,
            issue_date_key INTEGER NOT NULL,
            last_payment_date_key INTEGER,
            product_key INTEGER NOT NULL,
            borrower_profile_key INTEGER NOT NULL,
            geography_key INTEGER NOT NULL,
            purpose_key INTEGER NOT NULL,
            status_key INTEGER NOT NULL,
            loan_amnt DOUBLE,
            funded_amnt DOUBLE NOT NULL,
            int_rate DOUBLE,
            installment DOUBLE,
            annual_inc DOUBLE,
            dti DOUBLE,
            fico_mid INTEGER,
            total_pymnt DOUBLE,
            total_rec_prncp DOUBLE,
            total_rec_int DOUBLE,
            recoveries DOUBLE,
            collection_recovery_fee DOUBLE,
            out_prncp DOUBLE,
            is_default INTEGER NOT NULL,
            is_delinquent INTEGER NOT NULL,
            is_matured INTEGER NOT NULL,
            exposure_at_default DOUBLE NOT NULL,
            net_loss DOUBLE NOT NULL,
            FOREIGN KEY (issue_date_key) REFERENCES marts.dim_date (date_key),
            FOREIGN KEY (last_payment_date_key) REFERENCES marts.dim_date (date_key),
            FOREIGN KEY (product_key) REFERENCES marts.dim_loan_product (product_key),
            FOREIGN KEY (borrower_profile_key)
                REFERENCES marts.dim_borrower_profile (borrower_profile_key),
            FOREIGN KEY (geography_key) REFERENCES marts.dim_geography (geography_key),
            FOREIGN KEY (purpose_key) REFERENCES marts.dim_purpose (purpose_key),
            FOREIGN KEY (status_key) REFERENCES marts.dim_loan_status (status_key)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.fact_loan
        SELECT
            CAST(row_number() OVER (ORDER BY loan.source_loan_id) AS BIGINT) AS loan_key,
            loan.source_loan_id,
            CAST(strftime(loan.issue_date, '%Y%m%d') AS INTEGER) AS issue_date_key,
            CASE
                WHEN loan.last_payment_date IS NULL THEN NULL
                ELSE CAST(strftime(loan.last_payment_date, '%Y%m%d') AS INTEGER)
            END AS last_payment_date_key,
            product.product_key,
            borrower.borrower_profile_key,
            geography.geography_key,
            purpose.purpose_key,
            status.status_key,
            loan.loan_amnt,
            loan.funded_amnt,
            loan.int_rate,
            loan.installment,
            loan.annual_inc,
            loan.dti,
            loan.fico_mid,
            loan.total_pymnt,
            loan.total_rec_prncp,
            loan.total_rec_int,
            loan.recoveries,
            loan.collection_recovery_fee,
            loan.out_prncp,
            loan.is_default,
            loan.is_delinquent,
            CASE
                WHEN (loan.issue_date + loan.term_months * INTERVAL 1 MONTH)::DATE
                     <= (SELECT snapshot_date FROM params)
                THEN 1 ELSE 0
            END AS is_matured,
            CASE
                WHEN loan.is_default = 1
                THEN loan.funded_amnt - COALESCE(loan.total_rec_prncp, 0)
                ELSE 0
            END AS exposure_at_default,
            CASE
                WHEN loan.is_default = 1
                THEN (loan.funded_amnt - COALESCE(loan.total_rec_prncp, 0))
                     - COALESCE(loan.recoveries, 0)
                     + COALESCE(loan.collection_recovery_fee, 0)
                ELSE 0
            END AS net_loss
        FROM loan
        JOIN marts.dim_loan_product AS product
          ON loan.term_months = product.term_months
         AND loan.grade = product.grade
         AND loan.sub_grade = product.sub_grade
        JOIN marts.dim_borrower_profile AS borrower
          ON loan.fico_band = borrower.fico_band
         AND loan.income_band = borrower.income_band
         AND loan.dti_band = borrower.dti_band
         AND loan.emp_length = borrower.emp_length
         AND loan.home_ownership = borrower.home_ownership
         AND loan.verification_status = borrower.verification_status
         AND loan.application_type = borrower.application_type
        JOIN marts.dim_geography AS geography
          ON loan.zip3 = geography.zip3
         AND loan.state_code = geography.state_code
        JOIN marts.dim_purpose AS purpose
          ON loan.purpose_code = purpose.purpose_code
        JOIN marts.dim_loan_status AS status
          ON loan.loan_status = status.loan_status
         AND loan.status_group = status.status_group
         AND loan.is_terminal = status.is_terminal
         AND loan.credit_policy_flag = status.credit_policy_flag
        """
    )
    loan_rows = int(connection.execute("SELECT COUNT(*) FROM loan").fetchone()[0])
    fact_rows = int(connection.execute("SELECT COUNT(*) FROM marts.fact_loan").fetchone()[0])
    if loan_rows != fact_rows:
        raise PipelineError(
            f"Dimension join dropped or duplicated loans (staged {loan_rows}, fact {fact_rows})."
        )


def _write_audit(
    connection: duckdb.DuckDBPyConnection,
    source_name: str,
    snapshot: date,
) -> BuildReport:
    source_rows = int(connection.execute("SELECT COUNT(*) FROM parsed").fetchone()[0])
    fact_rows = int(connection.execute("SELECT COUNT(*) FROM marts.fact_loan").fetchone()[0])
    rejected_rows = int(
        connection.execute("SELECT COUNT(*) FROM staging.load_rejections").fetchone()[0]
    )
    if source_rows != fact_rows + rejected_rows:
        raise PipelineError(
            "Row accounting failed: "
            f"source {source_rows} != fact {fact_rows} + rejected {rejected_rows}."
        )
    reason_rows = connection.execute(
        """
        SELECT reject_reason, COUNT(*)
        FROM staging.load_rejections
        GROUP BY reject_reason
        ORDER BY reject_reason
        """
    ).fetchall()
    reasons = {str(reason): int(count) for reason, count in reason_rows}
    connection.execute(
        """
        CREATE TABLE marts.pipeline_audit (
            snapshot_date DATE NOT NULL,
            source_file_name VARCHAR NOT NULL,
            source_rows BIGINT NOT NULL,
            fact_rows BIGINT NOT NULL,
            rejected_rows BIGINT NOT NULL,
            built_at TIMESTAMP NOT NULL
        )
        """
    )
    connection.execute(
        """
        INSERT INTO marts.pipeline_audit
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [snapshot.isoformat(), source_name, source_rows, fact_rows, rejected_rows, datetime.now()],
    )
    connection.execute(
        """
        CREATE TABLE marts.load_rejection_summary AS
        SELECT reject_reason, COUNT(*)::BIGINT AS row_count
        FROM staging.load_rejections
        GROUP BY reject_reason
        ORDER BY reject_reason
        """
    )
    return BuildReport(
        warehouse_path=Path(),
        source_name=source_name,
        snapshot_date=snapshot,
        source_rows=source_rows,
        fact_rows=fact_rows,
        rejected_rows=rejected_rows,
        rejections_by_reason=reasons,
    )


def _warn_if_unexpected_extract(source: Path) -> None:
    if source.name != ACCEPTED_CSV_NAME:
        return
    columns = _header_column_count(source)
    if columns != EXPECTED_ACCEPTED_COLUMNS:
        print(
            "WARNING: "
            f"{source.name} has {columns} header columns; the Kaggle extract this pipeline "
            f"was written for has {EXPECTED_ACCEPTED_COLUMNS}. Continuing."
        )


def warn_if_unexpected_row_count(source_name: str, source_rows: int) -> None:
    if source_name != ACCEPTED_CSV_NAME:
        return
    if source_rows != EXPECTED_ACCEPTED_DATA_ROWS:
        print(
            "WARNING: "
            f"{source_name} has {source_rows} data rows; the Kaggle extract this pipeline "
            f"was written for has {EXPECTED_ACCEPTED_DATA_ROWS}. Continuing."
        )


def _header_column_count(path: Path) -> int:
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        header = handle.readline()
    return len(next(csv.reader([header])))


def _remove_database(path: Path) -> None:
    for candidate in (path, Path(str(path) + ".wal")):
        if candidate.exists():
            candidate.unlink()
