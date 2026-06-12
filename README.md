# finbot-catalog

`finbot-catalog` builds a lightweight metadata catalog for datasets produced by Finbot data-producing packages such as `finbot-data` and `finbot-features`.
It scans `FINBOT_DATA_ROOT`, reads sidecar `*.metadata.json` files and matching Parquet
files, then writes compact catalog outputs for downstream dashboard, modeling, and analysis
packages.

## What This Does

- Finds dataset metadata and Parquet files under the shared Finbot data root.
- Records dataset names, groups, paths, provider metadata, row counts, symbol counts, date
  ranges, Parquet columns, and schema details.
- Assigns a simple health status: `fresh`, `stale`, `missing`, `partial`, `failed`, or
  `unknown`.
- Writes:
  - `FINBOT_DATA_ROOT/catalog/dataset_catalog.parquet`
  - `FINBOT_DATA_ROOT/catalog/dataset_catalog.json`

## What This Does Not Do

- It does not own or copy raw datasets.
- It does not build features, train models, run dashboards, or orchestrate pipelines.
- It does not introduce databases, DuckDB, Dagster, Streamlit, FastAPI, or services.

## Expected Layout

```text
finbot/
├── data/
│   ├── market/
│   ├── reference/
│   ├── features/
│   ├── labels/
│   ├── models/
│   └── catalog/
└── repos/
    ├── finbot-data/
    ├── finbot-catalog/
    ├── finbot-features/
    ├── finbot-models/
    └── finbot-dashboard/
```

Known datasets include:

- `market/daily_bars/historical.parquet`
- `ratios/ratios.parquet`
- `reference/tickers_all.parquet`
- `reference/tickers.parquet`
- `reference/ticker_details.parquet`
- `reference/related_tickers.parquet`
- `features/equity_price_features.parquet`
- `features/equity_relative_features.parquet`
- `labels/equity_forward_return_labels.parquet`

Each dataset should have a sidecar metadata file such as
`historical.metadata.json` next to `historical.parquet`.

## Local Usage

Install the package into the local virtualenv:

```bash
source .venv/bin/activate
pip install -e ".[dev]"
```

Run with the default data root from `FINBOT_DATA_ROOT`, read first from the shell environment and then from `.env` in the current working directory:

```bash
python scripts/build_catalog.py
```

Or pass an explicit data root:

```bash
python scripts/build_catalog.py --data-root /Users/ilan/workspace/finbot/data
```

If `FINBOT_DATA_ROOT` is not set in either place, the command falls back to `./data` for local development.

You can also override the output directory:

```bash
python scripts/build_catalog.py --data-root /path/to/data --output-dir /path/to/data/catalog
```

## Docker Usage

Build the image:

```bash
docker build -t finbot-catalog .
```

Run it with the host data root mounted to `/data`:

```bash
docker run --rm -v /Users/ilan/workspace/finbot/data:/data finbot-catalog
```

The container uses `FINBOT_DATA_ROOT=/data`.

## Tests

```bash
pytest
```

Tests create temporary fake metadata and Parquet files. They do not use or modify the shared
Finbot data root.

## Consuming The Catalog

Downstream packages should read the catalog outputs from the shared data root rather than scanning
raw datasets themselves:

```python
import pandas as pd

catalog = pd.read_parquet("/path/to/finbot/data/catalog/dataset_catalog.parquet")
fresh_market = catalog[
    (catalog["dataset_group"] == "market") & (catalog["status"] == "fresh")
]
```

The JSON output is useful for simple tools or dashboards that prefer a portable, inspectable
format.
