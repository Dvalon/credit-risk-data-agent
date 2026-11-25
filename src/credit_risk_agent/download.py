"""Download the LendingClub accepted-loans file with the Kaggle CLI.

Credentials are read from the environment or from a gitignored ``.env`` file.
Nothing in this module puts a secret on the command line.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

from credit_risk_agent.config import (
    ACCEPTED_CSV_NAME,
    KAGGLE_ACCEPTED_FILE,
    KAGGLE_DATASET,
    RAW_DIR,
    REPO_ROOT,
)

_TOKEN_RE = re.compile(r"KGAT_[A-Za-z0-9_\-]+")


class DownloadError(RuntimeError):
    """The Kaggle download did not produce the accepted-loans file."""


def load_env() -> None:
    """Load ``.env`` without overriding variables that are already set."""
    load_dotenv(REPO_ROOT / ".env", override=False)
    key = os.environ.get("KAGGLE_KEY", "").strip()
    if key.upper().startswith("KGAT_"):
        # Current tokens belong in KAGGLE_API_TOKEN. Leaving them in KAGGLE_KEY
        # makes the Kaggle CLI reject the session.
        os.environ.setdefault("KAGGLE_API_TOKEN", key)
        os.environ.pop("KAGGLE_KEY", None)


def credential_source(
    environ: dict[str, str] | None = None,
    home: Path | None = None,
) -> str | None:
    """Return a short label for the credential that is present, or None."""
    env = os.environ if environ is None else environ
    if env.get("KAGGLE_API_TOKEN", "").strip():
        return "KAGGLE_API_TOKEN"
    username = env.get("KAGGLE_USERNAME", "").strip()
    key = env.get("KAGGLE_KEY", "").strip()
    if username and key and not key.upper().startswith("KGAT_"):
        return "KAGGLE_USERNAME and KAGGLE_KEY"
    root = Path.home() if home is None else home
    access_token = root / ".kaggle" / "access_token"
    if access_token.is_file() and access_token.stat().st_size > 0:
        return "~/.kaggle/access_token"
    legacy = root / ".kaggle" / "kaggle.json"
    if legacy.is_file() and legacy.stat().st_size > 0:
        return "~/.kaggle/kaggle.json"
    return None


def missing_source_message() -> str:
    return (
        "Kaggle credentials are not configured, and the accepted-loans file was not found at\n"
        f"  {RAW_DIR / ACCEPTED_CSV_NAME}\n"
        "\n"
        "The full-data build is pending credentials. Set KAGGLE_API_TOKEN, or "
        "KAGGLE_USERNAME and KAGGLE_KEY, in .env (see .env.example), then run:\n"
        "  make download\n"
        "  make build\n"
        "\n"
        "To build and test the pipeline on the committed synthetic fixture (no Kaggle account):\n"
        "  make build-sample\n"
        "  make test"
    )


def _kaggle_executable() -> Path:
    candidate = Path(sys.executable).resolve().parent / "kaggle"
    if candidate.is_file():
        return candidate
    raise DownloadError(
        "The kaggle CLI is not installed in this environment. From the repo root, run `uv sync`."
    )


def _redact(text: str) -> str:
    return _TOKEN_RE.sub("KGAT_[redacted]", text)


def download_accepted(destination: Path | None = None) -> Path:
    """Download and unpack the accepted-loans CSV. Returns the CSV path."""
    load_env()
    if credential_source() is None:
        raise DownloadError(missing_source_message())

    raw_dir = destination or RAW_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)
    command = [
        str(_kaggle_executable()),
        "datasets",
        "download",
        KAGGLE_DATASET,
        "-f",
        KAGGLE_ACCEPTED_FILE,
        "-p",
        str(raw_dir),
        "--unzip",
        "-o",
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    if completed.returncode != 0:
        detail = _redact((completed.stderr or completed.stdout or "").strip())
        raise DownloadError(
            "Kaggle download failed"
            + (f":\n{detail}" if detail else f" with exit code {completed.returncode}.")
        )

    csv_path = _find_accepted_csv(raw_dir)
    if csv_path is None:
        raise DownloadError(
            f"Kaggle finished but {ACCEPTED_CSV_NAME} was not found under {raw_dir}."
        )
    return csv_path


def _find_accepted_csv(raw_dir: Path) -> Path | None:
    direct = raw_dir / ACCEPTED_CSV_NAME
    if direct.is_file():
        return direct
    matches = sorted(raw_dir.rglob(ACCEPTED_CSV_NAME))
    if matches:
        return matches[0]
    gzip_matches = sorted(raw_dir.rglob(ACCEPTED_CSV_NAME + ".gz"))
    if not gzip_matches:
        return None
    import gzip
    import shutil

    target = raw_dir / ACCEPTED_CSV_NAME
    with gzip.open(gzip_matches[0], "rb") as source, target.open("wb") as output:
        shutil.copyfileobj(source, output)
    return target
