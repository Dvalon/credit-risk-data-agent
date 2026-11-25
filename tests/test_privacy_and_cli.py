"""Free-text exclusion, gitignore boundaries, and the no-credentials message."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from credit_risk_agent import download
from credit_risk_agent.config import DROPPED_FREE_TEXT_COLUMNS
from credit_risk_agent.pipeline import SourceNotFoundError, resolve_source

CANARIES = ("EmpTitleSecret", "personal note", "TitleSecret", "example.invalid")


def test_free_text_columns_and_values_are_absent(connection: duckdb.DuckDBPyConnection) -> None:
    columns = connection.execute(
        """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema IN ('staging', 'marts')
        """
    ).fetchall()
    names = {column for _, _, column in columns}
    assert names.isdisjoint(DROPPED_FREE_TEXT_COLUMNS)
    assert "member_id" not in names

    for schema, table, column in columns:
        hits = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM "{schema}"."{table}"
            WHERE CAST("{column}" AS VARCHAR) ILIKE '%personal note%'
               OR CAST("{column}" AS VARCHAR) ILIKE '%EmpTitleSecret%'
               OR CAST("{column}" AS VARCHAR) ILIKE '%TitleSecret%'
               OR CAST("{column}" AS VARCHAR) ILIKE '%example.invalid%'
            """
        ).fetchone()[0]
        assert hits == 0, f"{schema}.{table}.{column} contains a free-text canary"


def test_fixture_canaries_would_have_been_detectable(fixture_csv: Path) -> None:
    text = fixture_csv.read_text(encoding="utf-8")
    for canary in CANARIES:
        assert canary in text


def test_git_does_not_track_data_or_secrets() -> None:
    tracked = subprocess.check_output(["git", "ls-files"], text=True).splitlines()
    for path in tracked:
        assert not path.startswith("data/"), path
        assert not path.endswith(".duckdb"), path
        assert path != ".env", path
        assert "kaggle.json" not in path
        assert not path.endswith(".env") or path.endswith(".env.example")


def test_credential_detection_ignores_empty_values(tmp_path: Path) -> None:
    assert download.credential_source(environ={}, home=tmp_path) is None
    assert (
        download.credential_source(environ={"KAGGLE_API_TOKEN": "KGAT_example"}, home=tmp_path)
        == "KAGGLE_API_TOKEN"
    )
    assert (
        download.credential_source(
            environ={"KAGGLE_USERNAME": "someone", "KAGGLE_KEY": "legacy-key"},
            home=tmp_path,
        )
        == "KAGGLE_USERNAME and KAGGLE_KEY"
    )
    assert (
        download.credential_source(
            environ={"KAGGLE_USERNAME": "someone", "KAGGLE_KEY": "KGAT_example"},
            home=tmp_path,
        )
        is None
    )
    token_file = tmp_path / ".kaggle" / "access_token"
    token_file.parent.mkdir()
    token_file.write_text("KGAT_example\n", encoding="utf-8")
    assert download.credential_source(environ={}, home=tmp_path) == "~/.kaggle/access_token"


def test_kgat_key_is_promoted_to_api_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAGGLE_KEY", "KGAT_example")
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    download.load_env()
    assert os.environ["KAGGLE_API_TOKEN"] == "KGAT_example"
    assert "KAGGLE_KEY" not in os.environ


def test_resolve_source_explains_how_to_proceed_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(download, "RAW_DIR", tmp_path)
    monkeypatch.setattr("credit_risk_agent.pipeline.DEFAULT_SOURCE", tmp_path / "missing.csv")
    monkeypatch.setattr("credit_risk_agent.pipeline.RAW_DIR", tmp_path)
    monkeypatch.setattr("credit_risk_agent.pipeline.credential_source", lambda: None)
    with pytest.raises(SourceNotFoundError) as caught:
        resolve_source(None, allow_download=True)
    message = str(caught.value)
    assert "KAGGLE_API_TOKEN" in message
    assert "make build-sample" in message
    assert "make download" in message
