"""Paths, source columns, and the fixed snapshot date for the Phase 1 warehouse."""

from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
REFERENCE_DIR = REPO_ROOT / "reference"

ACCEPTED_CSV_NAME = "accepted_2007_to_2018Q4.csv"
ACCEPTED_GZ_NAME = "accepted_2007_to_2018Q4.csv.gz"
DEFAULT_SOURCE = RAW_DIR / ACCEPTED_CSV_NAME
DEFAULT_WAREHOUSE = DATA_DIR / "warehouse.duckdb"
SAMPLE_WAREHOUSE = DATA_DIR / "sample_warehouse.duckdb"
FIXTURE_CSV = REPO_ROOT / "tests" / "fixtures" / "accepted_sample.csv"

KAGGLE_DATASET = "wordsforthewise/lending-club"
KAGGLE_ACCEPTED_FILE = ACCEPTED_GZ_NAME

# Originations in the Kaggle extract run through 2018Q4. The file was published
# in early 2019. A fixed as-of date keeps matured-loan metrics reproducible.
SNAPSHOT_DATE = date(2019, 3, 31)

# Identity of the wordsforthewise extract this loader was written against.
# A mismatch prints a warning; it does not fail the build.
EXPECTED_ACCEPTED_DATA_ROWS = 2_260_701
EXPECTED_ACCEPTED_COLUMNS = 151

# Columns read from the accepted-loans extract. Free-text fields are absent
# on purpose: desc, emp_title, title, and url are never loaded.
REQUIRED_COLUMNS = (
    "id",
    "loan_amnt",
    "funded_amnt",
    "term",
    "int_rate",
    "installment",
    "grade",
    "sub_grade",
    "emp_length",
    "home_ownership",
    "annual_inc",
    "verification_status",
    "issue_d",
    "loan_status",
    "purpose",
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
)

DROPPED_FREE_TEXT_COLUMNS = ("desc", "emp_title", "title", "url")

US_STATES_CSV = REFERENCE_DIR / "us_states.csv"
PURPOSE_GROUPS_CSV = REFERENCE_DIR / "purpose_groups.csv"
LOAN_STATUS_CSV = REFERENCE_DIR / "loan_status.csv"
