# finbot-catalog

`finbot-catalog` builds a lightweight metadata catalog for datasets produced by Finbot data-producing packages such as `finbot-data` and `finbot-features`.
It scans `FINBOT_DATA_ROOT`, reads sidecar `*.metadata.json` files and matching Parquet
files, then writes compact catalog outputs for downstream dashboard, modeling, and analysis
packages.

Catalog discovery intentionally excludes `FINBOT_DATA_ROOT/research` so exploratory outputs
from `finbot-research` do not appear in dashboard-facing catalog records. Promoted datasets
should be written outside `research`, such as under `features`, `labels`, or `signals`.

## What This Does

- Finds dataset metadata and Parquet files under the shared Finbot data root.
- Registers expected core datasets so missing files are visible in the catalog.
- Excludes exploratory research outputs under `FINBOT_DATA_ROOT/research`.
- Records dataset names, groups, paths, provider metadata, row counts, entity counts, date
  ranges, Parquet columns, and schema details.
- Records current-state health fields such as write mode, completeness profile, latest date
  coverage, provider update bounds, duplicate key counts, and missing required columns.
- Assigns a simple health status: `fresh`, `stale`, `missing`, `partial`, `failed`, or
  `unknown`.
- Writes:
  - `FINBOT_DATA_ROOT/catalog/dataset_catalog.parquet`
  - `FINBOT_DATA_ROOT/catalog/dataset_catalog.json`
  - `FINBOT_DATA_ROOT/catalog/ops_catalog.parquet`
  - `FINBOT_DATA_ROOT/catalog/ops_catalog.json`

## What This Does Not Do

- It does not own or copy raw datasets.
- It does not build features, train models, run dashboards, or orchestrate pipelines.
- It does not catalog exploratory research outputs under `research/`.
- It does not introduce databases, DuckDB, Dagster, Streamlit, FastAPI, or services.

## Expected Layout

```text
finbot/
├── data/
│   ├── market/
│   ├── reference/
│   ├── features/
│   ├── labels/
│   ├── signals/
│   ├── research/      # intentionally excluded from catalog discovery
│   ├── models/
│   └── catalog/
└── repos/
    ├── finbot-data/
    ├── finbot-catalog/
    ├── finbot-features/
    ├── finbot-models/
    └── finbot-dashboard/
```

Registered core datasets:

- `market/daily_bars/historical.parquet`
- `fundamentals/sf1.parquet`
- `fundamentals/daily_valuation_metrics.parquet`
- `reference/tickers.parquet`

Additional discovered datasets may include:

- `ratios/ratios.parquet`
- `reference/ticker_details.parquet`
- `reference/related_tickers.parquet`
- `features/equity_price_features.parquet`
- `features/equity_relative_features.parquet`
- `labels/equity_forward_return_labels.parquet`
- `signals/price_strength/scorecard_v1_current.parquet`

Each dataset should have a sidecar metadata file such as
`historical.metadata.json` next to `historical.parquet`.
Durable dataset metadata should use the current-state fields written by Finbot data producers,
including `dataset_name`, `dataset_group`, `generated_at_utc`, `write_mode`,
`completeness_profile`, `row_count`, date ranges, coverage fields, provider update bounds,
duplicate key counts, and missing required columns.

Catalog discovery classifies records as:

- `registered_present`: registered metadata and parquet exist.
- `registered_missing`: a registered dataset has neither metadata nor parquet.
- `discovered_unregistered`: metadata and parquet were found for an unregistered dataset.
- `metadata_only`: metadata exists without the parquet file.
- `parquet_only`: parquet exists without metadata.

The `ops_catalog` outputs are current-state records only. They do not store job-run history.

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

For dataset health views, read `catalog/ops_catalog.parquet`:

```python
ops = pd.read_parquet("/path/to/finbot/data/catalog/ops_catalog.parquet")
stale_or_missing = ops[ops["status"].isin(["stale", "missing", "partial", "failed"])]
```
