# Credit risk data agent

Phase 1 of a natural-language-to-SQL agent for loan and credit-risk questions.
This phase loads LendingClub accepted loans (2007–2018Q4) into a local DuckDB
star schema. There is no LLM, API, or chat UI yet.

The model is one row per issued loan (`marts.fact_loan`) with date, product,
borrower-profile, geography, purpose, and loan-status dimensions. Metric
definitions and the column dictionary are in [docs/data_dictionary.md](docs/data_dictionary.md).

## Status of the full extract

The loader is written for the Kaggle file `accepted_2007_to_2018Q4.csv`
([wordsforthewise/lending-club](https://www.kaggle.com/datasets/wordsforthewise/lending-club),
about 2.26 million rows). **That file is not in this environment, and this
phase was not run on it.** CI and the default tests use a synthetic fixture
that matches the real column names and value formats (term text, `Mon-YYYY`
dates, `941xx` zip codes). The fixture is invented; it is not a sample of the
Kaggle extract.

After credentials are in `.env`, the same command path builds the full warehouse:

```bash
make download
make build
```

`make build` prints a warning if that canonical file does not have 151 columns
or 2,260,701 data rows, then continues.

## Setup

Python 3.12 and [uv](https://docs.astral.sh/uv/) are required.

```bash
uv sync --all-groups
```

Copy the credential template and fill in one of the two options. `.env` is
gitignored.

```bash
cp .env.example .env
```

- `KAGGLE_API_TOKEN` — a current token from https://www.kaggle.com/settings/api (it starts with `KGAT_`).
- or `KAGGLE_USERNAME` and `KAGGLE_KEY` — a legacy API key from the same page.

A token placed in `KAGGLE_KEY` is moved to `KAGGLE_API_TOKEN` before the Kaggle
CLI runs. Secrets are not passed as command-line arguments.

## Commands

| Command | What it does |
|---|---|
| `make download` | Download only the accepted-loans file into `data/raw/` |
| `make build` | Rebuild `data/warehouse.duckdb` from that file. Downloads first if the file is missing and credentials exist. |
| `make build-sample` | Rebuild `data/sample_warehouse.duckdb` from `tests/fixtures/accepted_sample.csv` |
| `make test` | Run the quality checks and the pandas metric tie-out on the fixture |
| `make check` | Build the sample warehouse and run the quality checks again |
| `make clean` | Delete local DuckDB files. Does not delete `data/raw/` |

`make build` replaces the warehouse only after the load and the quality checks
succeed. Override the inputs when you need to: `make build SOURCE=path/to.csv WAREHOUSE=path/to.duckdb`.

## What the tests prove

On the synthetic fixture, pytest checks:

- primary keys are unique and non-null
- foreign keys are present, except `last_payment_date_key`, which may be null
- fact rows plus rejected rows equal source rows, by the documented filter
- five metrics from `marts.fact_loan` match an independent pandas calculation on the raw CSV: loan count, funded volume, weighted average interest rate, default rate on matured loans, and charge-off rate
- free-text canaries in the fixture (`emp_title`, `desc`, `title`, `url`) do not appear in the warehouse

## Layout

```text
src/credit_risk_agent/    loader, star-schema build, quality checks, metrics
reference/                state, purpose, and loan-status maps
tests/fixtures/           synthetic accepted-loans CSV
docs/data_dictionary.md   columns, filters, metrics, ER diagram
data/                     gitignored raw file and DuckDB warehouses
```

Raw and built loan-level files stay out of git (`.gitignore` covers `data/` and `*.duckdb`). The Kaggle upload is marked CC0 by the publisher; this repo still does not redistribute the extract. Public write-ups should show aggregates, not individual loans.

## Phase 1 modelling choices

The spec's dbt project is not in this phase. SQL still lands in `staging` and then `marts`, and pytest covers the uniqueness, null, relationship, and metric checks dbt would have run. That keeps one toolchain for CI. A later phase can point the NL-to-SQL allowlist at `marts`.

Other differences, with the reasoning, are listed at the end of the data dictionary. The short version: no rejected-application fact, a fixed snapshot of 2019-03-31 for `is_matured`, explicit `Unknown` dimension members, and a nullable last-payment date instead of a dummy date.

## Licence

Code in this repository is MIT. See [LICENSE](LICENSE).
