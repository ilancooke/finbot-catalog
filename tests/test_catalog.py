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
            "dataset_name": "market.daily_bars.historical",
            "dataset_group": "market",
            "provider": "sharadar",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "write_mode": "incremental_merge",
            "completeness_profile": "trading_day_symbol_panel",
            "primary_key": ["symbol", "date"],
            "date_column": "date",
            "entity_column": "symbol",
            "row_count": 2,
            "symbol_count": 2,
            "min_date": "2026-05-06",
            "max_date": "2026-05-07",
            "latest_date": "2026-05-07",
            "latest_date_coverage_count": 2,
            "latest_date_coverage_pct": 1.0,
            "provider_min_lastupdated": "2026-05-08",
            "provider_max_lastupdated": "2026-05-08",
            "duplicate_key_count": 0,
            "missing_required_columns": [],
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

    result = build_catalog(tmp_path, now=NOW, registry=())

    assert result.dataframe.shape[0] == 1
    row = result.dataframe.iloc[0].to_dict()
    assert row["dataset_name"] == "market.daily_bars.historical"
    assert row["dataset_group"] == "market"
    assert row["discovery_status"] == "discovered_unregistered"
    assert row["is_registered"] is False
    assert row["metadata_exists"] is True
    assert row["parquet_exists"] is True
    assert row["provider"] == "sharadar"
    assert row["write_mode"] == "incremental_merge"
    assert row["completeness_profile"] == "trading_day_symbol_panel"
    assert row["row_count"] == 2
    assert row["entity_count"] == 2
    assert row["min_date"] == "2026-05-06"
    assert row["max_date"] == "2026-05-07"
    assert row["latest_date"] == "2026-05-07"
    assert row["latest_date_coverage_pct"] == 1.0
    assert row["duplicate_key_count"] == 0
    assert row["status"] == "fresh"
    assert json.loads(row["parquet_columns"]) == ["symbol", "date", "close"]
    assert json.loads(row["primary_key"]) == ["symbol", "date"]
    assert json.loads(row["missing_required_columns"]) == []


def test_default_registry_marks_expected_datasets(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "reference",
        "tickers",
        metadata={
            "dataset_name": "reference.tickers",
            "dataset_group": "reference",
            "provider": "sharadar",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "write_mode": "replace_snapshot",
            "completeness_profile": "provider_snapshot",
            "primary_key": ["ticker"],
            "entity_column": "ticker",
            "row_count": 1,
            "ticker_count": 1,
            "duplicate_key_count": 0,
            "missing_required_columns": [],
            "parquet_file": "tickers.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["AAPL"]}),
    )

    result = build_catalog(tmp_path, now=NOW)

    statuses = dict(zip(result.dataframe["dataset_name"], result.dataframe["discovery_status"]))
    assert statuses["reference.tickers"] == "registered_present"
    assert statuses["market.daily_bars.historical"] == "registered_missing"
    assert statuses["fundamentals.sf1"] == "registered_missing"
    assert statuses["fundamentals.daily_valuation_metrics"] == "registered_missing"
    tickers = result.dataframe[result.dataframe["dataset_name"] == "reference.tickers"].iloc[0].to_dict()
    assert tickers["is_registered"] is True
    assert tickers["owner_repo"] == "finbot-data"
    assert tickers["producer_job"] == "download_ticker_universe"
    assert tickers["status"] == "fresh"

    ops = result.ops_dataframe
    assert ops.shape[0] == 4
    assert set(ops["record_type"]) == {"dataset_state"}
    assert "reference.tickers" in ops["dataset_name"].tolist()


def test_catalog_includes_ratios_dataset(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "ratios",
        "ratios",
        metadata={
            "provider": "massive",
            "dataset_name": "ratios.ratios",
            "dataset_group": "ratios",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "write_mode": "replace_snapshot",
            "completeness_profile": "daily_ticker_metrics",
            "entity_column": "ticker",
            "row_count": 2,
            "ticker_count": 2,
            "min_date": "2026-05-06",
            "max_date": "2026-05-07",
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

    result = build_catalog(tmp_path, now=NOW, registry=())

    row = result.dataframe.iloc[0].to_dict()
    assert row["dataset_name"] == "ratios.ratios"
    assert row["dataset_group"] == "ratios"
    assert row["provider"] == "massive"
    assert row["row_count"] == 2
    assert row["entity_count"] == 2
    assert row["min_date"] == "2026-05-06"
    assert row["max_date"] == "2026-05-07"
    assert row["status"] == "fresh"
    assert json.loads(row["parquet_columns"]) == ["ticker", "date", "price_to_earnings"]


def test_catalog_excludes_research_outputs(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "research" / "price_strength_scorecard_v1",
        "equity_price_strength_scorecard_v1_current",
        metadata={
            "dataset_name": "research.price_strength_scorecard_v1.equity_price_strength_scorecard_v1_current",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "row_count": 1,
        },
        dataframe=pd.DataFrame({"symbol": ["AAPL"], "date": ["2026-05-07"]}),
    )
    _write_dataset(
        tmp_path / "features",
        "equity_price_features",
        metadata={
            "dataset_name": "features.equity_price_features",
            "dataset_group": "features",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "row_count": 1,
        },
        dataframe=pd.DataFrame({"symbol": ["AAPL"], "date": ["2026-05-07"]}),
    )

    result = build_catalog(tmp_path, now=NOW, registry=())

    assert result.dataframe["dataset_name"].tolist() == ["features.equity_price_features"]


def test_catalog_includes_signals_and_generated_timestamp(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "signals" / "price_strength",
        "scorecard_v1_current",
        metadata={
            "dataset_name": "signals.price_strength.scorecard_v1_current",
            "dataset_group": "signals",
            "dataset_type": "research_derived_signal_snapshot",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "row_count": 2,
            "symbol_count": 2,
            "entity_column": "symbol",
        },
        dataframe=pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "date": ["2026-05-07", "2026-05-07"],
                "price_strength_score_v1": [3, 1],
            }
        ),
    )

    result = build_catalog(tmp_path, now=NOW, registry=())

    row = result.dataframe.iloc[0].to_dict()
    assert row["dataset_name"] == "signals.price_strength.scorecard_v1_current"
    assert row["dataset_group"] == "signals"
    assert row["generated_at_utc"] == "2026-05-08T10:00:00Z"
    assert row["row_count"] == 2
    assert row["entity_count"] == 2
    assert row["status"] == "fresh"
    assert json.loads(row["parquet_columns"]) == ["symbol", "date", "price_strength_score_v1"]


def test_missing_metadata(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "reference"
    dataset_dir.mkdir(parents=True)
    _write_parquet(dataset_dir / "tickers.parquet", pd.DataFrame({"ticker": ["AAPL"]}))

    result = build_catalog(tmp_path, now=NOW, registry=())

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
            "dataset_name": "reference.ticker_details",
            "dataset_group": "reference",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "row_count": 10,
            "ticker_count": 10,
            "entity_column": "ticker",
            "parquet_file": "ticker_details.parquet",
        },
    )

    result = build_catalog(tmp_path, now=NOW, registry=())

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
            "dataset_name": "reference.fresh",
            "generated_at_utc": "2026-05-07T12:00:00Z",
            "row_count": 1,
            "parquet_file": "fresh.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["AAPL"]}),
    )
    _write_dataset(
        tmp_path / "reference",
        "stale",
        metadata={
            "dataset_name": "reference.stale",
            "generated_at_utc": "2026-04-01T12:00:00Z",
            "row_count": 1,
            "parquet_file": "stale.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["MSFT"]}),
    )

    result = build_catalog(tmp_path, now=NOW, freshness_days=7, registry=())

    statuses = dict(zip(result.dataframe["dataset_name"], result.dataframe["status"]))
    assert statuses["reference.fresh"] == "fresh"
    assert statuses["reference.stale"] == "stale"


def test_partial_and_failed_logic(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path / "reference",
        "partial_dataset",
        metadata={
            "dataset_name": "reference.partial_dataset",
            "generated_at_utc": "2026-05-08T10:00:00Z",
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
            "dataset_name": "reference.failed_dataset",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "failed_tickers": ["MSFT"],
            "parquet_file": "failed_dataset.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["MSFT"]}),
    )

    result = build_catalog(tmp_path, now=NOW, registry=())

    statuses = dict(zip(result.dataframe["dataset_name"], result.dataframe["status"]))
    assert statuses["reference.partial_dataset"] == "partial"
    assert statuses["reference.failed_dataset"] == "failed"


def test_output_files_written_successfully(tmp_path: Path) -> None:
    output_dir = tmp_path / "catalog_out"
    _write_dataset(
        tmp_path / "reference",
        "tickers",
        metadata={
            "dataset_name": "reference.tickers",
            "dataset_group": "reference",
            "generated_at_utc": "2026-05-08T10:00:00Z",
            "row_count": 1,
            "ticker_count": 1,
            "entity_column": "ticker",
            "parquet_file": "tickers.parquet",
        },
        dataframe=pd.DataFrame({"ticker": ["AAPL"]}),
    )

    result = build_catalog(tmp_path, output_dir=output_dir, now=NOW, registry=())

    assert result.parquet_path.exists()
    assert result.json_path.exists()
    assert result.ops_parquet_path.exists()
    assert result.ops_json_path.exists()
    parquet = pd.read_parquet(result.parquet_path)
    ops_parquet = pd.read_parquet(result.ops_parquet_path)
    loaded_json = json.loads(result.json_path.read_text(encoding="utf-8"))
    loaded_ops_json = json.loads(result.ops_json_path.read_text(encoding="utf-8"))
    assert parquet.shape[0] == 1
    assert ops_parquet.shape[0] == 1
    assert len(loaded_json) == 1
    assert len(loaded_ops_json) == 1
    assert loaded_json[0]["dataset_name"] == "reference.tickers"
    assert loaded_ops_json[0]["record_type"] == "dataset_state"


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
