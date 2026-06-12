# AGENTS.md

## Repo Purpose

`finbot-catalog` is a lightweight metadata catalog builder for the Finbot project.
It scans `FINBOT_DATA_ROOT`, inspects dataset metadata JSON and Parquet files, and
writes compact catalog outputs for downstream dashboard, feature, and modeling packages.

## Boundaries

- Keep raw datasets out of this repo.
- Do not copy or store data files in the repo.
- Write generated catalog outputs to `FINBOT_DATA_ROOT/catalog`.
- Keep this package dependency-light: Python, pandas, pyarrow, pytest.
- Do not add dashboards, modeling, feature engineering, orchestration, Dagster, DuckDB,
  Streamlit, FastAPI, databases, or services here.

## Expected Commands

Run locally:

```bash
python scripts/build_catalog.py
```

Run tests:

```bash
pytest
```

Run with an explicit data root:

```bash
python scripts/build_catalog.py --data-root /path/to/finbot/data
```

Run with Docker:

```bash
docker build -t finbot-catalog .
docker run --rm -v /path/to/finbot/data:/data finbot-catalog
```

## Development Notes

- Default data root resolution should remain: shell `FINBOT_DATA_ROOT`, then current working directory `.env`, then `./data`.
- Docker should mount the host data root to `/data` and use `FINBOT_DATA_ROOT=/data`.
- Tests must use temporary directories and fake metadata/Parquet files.
- Avoid reading or writing the real shared data root in tests.
- Keep catalog records simple, tabular, and easy for pandas consumers to read.
- Preserve both outputs:
  - `dataset_catalog.parquet`
  - `dataset_catalog.json`
