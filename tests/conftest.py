"""Build the synthetic warehouse once per test session."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from credit_risk_agent.config import FIXTURE_CSV
from credit_risk_agent.pipeline import BuildReport, build_warehouse


@pytest.fixture(scope="session")
def fixture_csv() -> Path:
    return FIXTURE_CSV


@pytest.fixture(scope="session")
def built(tmp_path_factory: pytest.TempPathFactory) -> tuple[BuildReport, Path]:
    warehouse = tmp_path_factory.mktemp("warehouse") / "warehouse.duckdb"
    report = build_warehouse(FIXTURE_CSV, warehouse)
    return report, warehouse


@pytest.fixture(scope="session")
def build_report(built: tuple[BuildReport, Path]) -> BuildReport:
    return built[0]


@pytest.fixture(scope="session")
def warehouse_path(built: tuple[BuildReport, Path]) -> Path:
    return built[1]


@pytest.fixture
def connection(warehouse_path: Path):
    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        yield con
    finally:
        con.close()
