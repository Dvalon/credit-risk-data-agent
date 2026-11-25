"""Independent pandas computation of the Phase 1 filters and core metrics.

This module intentionally does not import the warehouse pipeline. It re-reads
the raw CSV and applies the rules documented in ``docs/data_dictionary.md``.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd

# Documented as-of date. Duplicated here so a change in the pipeline constant
# cannot silently move the oracle.
SNAPSHOT_DATE = date(2019, 3, 31)

DEFAULT_STATUSES = {
    "Charged Off",
    "Default",
    "Does not meet the credit policy. Status:Charged Off",
}

_DIGITS = re.compile(r"(\d+)")


def _blank(value: object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "") or pd.isna(value)


def _parse_month(value: object) -> date | None:
    if _blank(value):
        return None
    try:
        return datetime.strptime(str(value).strip(), "%b-%Y").date()
    except ValueError:
        return None


def _parse_amount(value: object) -> float | None:
    if _blank(value):
        return None
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _parse_rate(value: object) -> float | None:
    if _blank(value):
        return None
    text = str(value).strip().replace("%", "").replace(" ", "")
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_term(value: object) -> int | None:
    if _blank(value):
        return None
    match = _DIGITS.search(str(value))
    if match is None:
        return None
    return int(match.group(1))


def _add_months(value: date, months: int) -> date:
    index = value.month - 1 + months
    year = value.year + index // 12
    month = index % 12 + 1
    return date(year, month, value.day)


def _reject_reason(row: pd.Series) -> str | None:
    if _blank(row["source_loan_id"]):
        return "missing_loan_id"
    if row["issue_date"] is None or pd.isna(row["issue_date"]):
        return "missing_issue_date"
    if _blank(row["loan_status_clean"]):
        return "missing_loan_status"
    if row["funded_amnt_num"] is None or pd.isna(row["funded_amnt_num"]):
        return "missing_funded_amnt"
    if row["funded_amnt_num"] < 0:
        return "negative_funded_amnt"
    term = row["term_months"]
    if term is None or pd.isna(term) or int(term) not in (36, 60):
        return "invalid_term"
    return None


def prepare(csv_path: Path) -> tuple[pd.DataFrame, pd.Series]:
    """Return kept loans and a count of rejection reasons."""
    raw = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    frame = raw.copy()
    frame["source_row_number"] = range(1, len(frame) + 1)
    loan_ids = frame["id"].str.strip()
    frame["source_loan_id"] = loan_ids.where(loan_ids != "", pd.NA)
    frame["issue_date"] = frame["issue_d"].map(_parse_month)
    statuses = frame["loan_status"].str.strip()
    frame["loan_status_clean"] = statuses.where(statuses != "", pd.NA)
    frame["funded_amnt_num"] = frame["funded_amnt"].map(_parse_amount)
    frame["term_months"] = frame["term"].map(_parse_term)
    frame["int_rate_num"] = frame["int_rate"].map(_parse_rate)
    frame["total_rec_prncp_num"] = frame["total_rec_prncp"].map(_parse_amount)
    frame["reject_reason"] = frame.apply(_reject_reason, axis=1)

    invalid = frame.loc[frame["reject_reason"].notna(), "reject_reason"]
    valid = frame.loc[frame["reject_reason"].isna()].sort_values("source_row_number").copy()
    valid["dup_rank"] = valid.groupby("source_loan_id", dropna=False).cumcount()
    duplicates = pd.Series(["duplicate_loan_id"] * int((valid["dup_rank"] > 0).sum()))
    kept = valid.loc[valid["dup_rank"] == 0].copy()

    kept["is_default"] = kept["loan_status_clean"].isin(DEFAULT_STATUSES).astype(int)
    kept["is_matured"] = [
        int(_add_months(issued, int(term)) <= SNAPSHOT_DATE)
        for issued, term in zip(kept["issue_date"], kept["term_months"], strict=True)
    ]
    principal = kept["total_rec_prncp_num"].fillna(0.0)
    kept["exposure_at_default"] = 0.0
    default_rows = kept["is_default"] == 1
    kept.loc[default_rows, "exposure_at_default"] = (
        kept.loc[default_rows, "funded_amnt_num"] - principal.loc[default_rows]
    )
    reasons = pd.concat([invalid, duplicates], ignore_index=True)
    return kept, reasons.value_counts()


def core_metrics(kept: pd.DataFrame) -> dict[str, float]:
    matured = kept.loc[kept["is_matured"] == 1]
    rated = kept.loc[kept["int_rate_num"].notna()]
    funded = kept["funded_amnt_num"].sum()
    return {
        "loan_count": float(len(kept)),
        "funded_volume": float(funded),
        "weighted_avg_interest_rate": float(
            (rated["int_rate_num"] * rated["funded_amnt_num"]).sum() / rated["funded_amnt_num"].sum()
        ),
        "default_rate_matured": float(matured["is_default"].sum() / len(matured)),
        "charge_off_rate": float(kept["exposure_at_default"].sum() / funded),
    }
