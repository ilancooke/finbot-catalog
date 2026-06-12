from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from finbot_catalog.catalog import build_catalog, default_data_root


NOW = datetime(2026, 5, 8, 12, 0, tzinfo=UTC)


def test_default_data_root_reads_dotenv(tmp_path: Path, monkeypatch) -> None:
    dotenv_data_root = tmp_path / "shared_data"
    (tmp_path / ".env").write_text(f"FINBOT_DATA_ROOT={dotenv_data_root}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("FINBOT_DATA_ROOT", raising=False)

    assert default_data_root() == dotenv_data_root


def test_default_data_root_prefers_environment_over_dotenv(tmp_path: Path, monkeypatch) -> None:
    dotenv_data_root = tmp_path / "dotenv_data"
    env_data_root = tmp_path / "env_data"
    (tmp_path / ".env").write_text(f"FINBOT_DATA_ROOT={dotenv_data_root}\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("FINBOT_DATA_ROOT", str(env_data_root))

    assert default_data_root() == env_data_root


def test_successful_catalog_build(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "market" / "daily_bars"
    _write_dataset(
        dataset_dir,
        "historical",
        metadata={
            "provider": "massive",
            "collected_at_utc": "2026-05-08T10:00:00Z",
            "rows": 2,
            "symbols": 2,
            "data_min_date": "2026-05-06",
            "data_max_date": "2026-05-07",
            "parquet_file": "historical.parquet",
        },
        dataframe=pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "date": ["2026-05-07", "2026-05-07"],
                "close": [190.0, 420.0],
            }
        ),
    )

    result = build_catalog(tmp_path, now=NOW)

    assert result.dataframe.shape[0] == 1
    row = result.dataframe.iloc[0].to_dict()
    assert row["dataset_name"] == "market.daily_bars.historical"
    assert row["dataset_group"] == "market"
    assert row["metadata_exists"] is True
    assert row["parquet_exists"] is True
    assert row["provider"] == "massive"
    assert row["row_count"] == 2
    assert row["symbol_count"] == 2
    assert row["data_min_date"] == "2026-05-06"
    assert row["data_max_date"] == "2026-05-07"
    assert row["status"] == "fresh"
    assert json.loads(row["parquet_columns"]) == ["symbol", "date", "close"]


def test_catalog_includes_ratios_dataset(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "ratios",
        "ratios",
        metadata={
            "provider": "massive",
            "dataset": "ratios",
            "mode": "replace",
            "collected_at_utc": "2026-05-08T10:00:00Z",
            "rows": 2,
            "tickers": 2,
            "data_min_date": "2026-05-06",
            "data_max_date": "2026-05-07",
            "parquet_file": "ratios.parquet",
        },
        dataframe=pd.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "date": ["2026-05-07", "2026-05-07"],
                "price_to_earnings": [34.44, 24.96],
            }
        ),
    )

    result = build_catalog(tmp_path, now=NOW)

    row = result.dataframe.iloc[0].to_dict()
    assert row["dataset_name"] == "ratios.ratios"
    assert row["dataset_group"] == "ratios"
    assert row["provider"] == "massive"
    assert row["row_count"] == 2
    assert row["symbol_count"] == 2
    assert row["data_min_date"] == "2026-05-06"
    assert row["data_max_date"] == "2026-05-07"
    assert row["status"] == "fresh"
    assert json.loads(row["parquet_columns"]) == ["ticker", "date", "price_to_earnings"]


def test_missing_metadata(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "reference"
    dataset_dir.mkdir(parents=True)
    _write_parquet(dataset_dir / "tickers.parquet", pd.DataFrame({"ticker": ["AAPL"]}))

    result = build_catalog(tmp_path, now=NOW)

    row = result.dataframe.iloc[0]
    assert row["dataset_name"] == "reference.tickers"
    assert bool(row["metadata_exists"]) is False
    assert bool(row["parquet_exists"]) is True
    assert row["status"] == "missing"
    assert "metadata file is missing" in row["status_reason"]


def test_missing_parquet(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "reference"
    dataset_dir.mkdir(parents=True)
    _write_metadata(
        dataset_dir / "ticker_details.metadata.json",
        {
            "provider": "massive",
            "collected_at_utc": "2026-05-08T10:00:00Z",
            "rows": 10,
            "tickers": 10,
            "parquet_file": "ticker_details.parquet",
        },
    )

    result = build_catalog(tmp_path, now=NOW)

    row = result.dataframe.iloc[0]
    assert row["dataset_name"] == "reference.ticker_details"
    assert bool(row["metadata_exists"]) is True
    assert bool(row["parquet_exists"]) is False
    assert row["status"] == "missing"
    assert "parquet file is missing" in row["status_reason"]


def test_stale_and_fresh_logic(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "reference",
        "fresh",
        metadata={
            "collected_at_utc": "2026-05-07T12:00:00Z",
            "rows": 1,
            "parquet_file": "fresh.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["AAPL"]}),
    )
    _write_dataset(
        tmp_path / "reference",
        "stale",
        metadata={
            "collected_at_utc": "2026-04-01T12:00:00Z",
            "rows": 1,
            "parquet_file": "stale.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["MSFT"]}),
    )

    result = build_catalog(tmp_path, now=NOW, freshness_days=7)

    statuses = dict(zip(result.dataframe["dataset_name"], result.dataframe["status"]))
    assert statuses["reference.fresh"] == "fresh"
    assert statuses["reference.stale"] == "stale"


def test_partial_and_failed_logic(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "reference",
        "partial_dataset",
        metadata={
            "collected_at_utc": "2026-05-08T10:00:00Z",
            "partial": True,
            "pending_tickers": 5,
            "parquet_file": "partial_dataset.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["AAPL"]}),
    )
    _write_dataset(
        tmp_path / "reference",
        "failed_dataset",
        metadata={
            "collected_at_utc": "2026-05-08T10:00:00Z",
            "failed_tickers": ["MSFT"],
            "parquet_file": "failed_dataset.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["MSFT"]}),
    )

    result = build_catalog(tmp_path, now=NOW)

    statuses = dict(zip(result.dataframe["dataset_name"], result.dataframe["status"]))
    assert statuses["reference.partial_dataset"] == "partial"
    assert statuses["reference.failed_dataset"] == "failed"


def test_output_files_written_successfully(tmp_path: Path) -> None:
    output_dir = tmp_path / "catalog_out"
    _write_dataset(
        tmp_path / "reference",
        "tickers",
        metadata={
            "collected_date_utc": "2026-05-08",
            "rows": 1,
            "tickers": 1,
            "parquet_file": "tickers.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["AAPL"]}),
    )

    result = build_catalog(tmp_path, output_dir=output_dir, now=NOW)

    assert result.parquet_path.exists()
    assert result.json_path.exists()
    parquet = pd.read_parquet(result.parquet_path)
    loaded_json = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert parquet.shape[0] == 1
    assert len(loaded_json) == 1
    assert loaded_json[0]["dataset_name"] == "reference.tickers"


def _write_dataset(
    directory: Path,
    name: str,
    *,
    metadata: dict,
    dataframe: pd.DataFrame,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    _write_parquet(directory / f"{name}.parquet", dataframe)
    _write_metadata(directory / f"{name}.metadata.json", metadata)


def _write_parquet(path: Path, dataframe: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(path, index=False)


def _write_metadata(path: Path, metadata: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metadata), encoding="utf-8")
