from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


CATALOG_PARQUET_NAME = "dataset_catalog.parquet"
CATALOG_JSON_NAME = "dataset_catalog.json"
OPS_CATALOG_PARQUET_NAME = "ops_catalog.parquet"
OPS_CATALOG_JSON_NAME = "ops_catalog.json"
DEFAULT_FRESHNESS_DAYS = 7
DEFAULT_DOTENV_FILE = ".env"


@dataclass(frozen=True)
class CatalogResult:
    dataframe: pd.DataFrame
    parquet_path: Path
    json_path: Path
    ops_dataframe: pd.DataFrame
    ops_parquet_path: Path
    ops_json_path: Path


@dataclass(frozen=True)
class DatasetRegistration:
    dataset_name: str
    dataset_group: str
    parquet_path: str
    metadata_path: str
    owner_repo: str
    producer_job: str
    write_mode: str
    completeness_profile: str
    primary_key: tuple[str, ...]
    date_column: str | None = None
    entity_column: str | None = None


DEFAULT_DATASET_REGISTRY: tuple[DatasetRegistration, ...] = (
    DatasetRegistration(
        dataset_name="reference.tickers",
        dataset_group="reference",
        parquet_path="reference/tickers.parquet",
        metadata_path="reference/tickers.metadata.json",
        owner_repo="finbot-data",
        producer_job="download_ticker_universe",
        write_mode="replace_snapshot",
        completeness_profile="provider_snapshot",
        primary_key=("ticker",),
        entity_column="ticker",
    ),
    DatasetRegistration(
        dataset_name="market.daily_bars.historical",
        dataset_group="market",
        parquet_path="market/daily_bars/historical.parquet",
        metadata_path="market/daily_bars/historical.metadata.json",
        owner_repo="finbot-data",
        producer_job="download_historical_prices/update_historical_prices",
        write_mode="incremental_merge",
        completeness_profile="trading_day_symbol_panel",
        primary_key=("symbol", "date"),
        date_column="date",
        entity_column="symbol",
    ),
    DatasetRegistration(
        dataset_name="fundamentals.sf1",
        dataset_group="fundamentals",
        parquet_path="fundamentals/sf1.parquet",
        metadata_path="fundamentals/sf1.metadata.json",
        owner_repo="finbot-data",
        producer_job="download_fundamentals/update_fundamentals",
        write_mode="incremental_merge",
        completeness_profile="fundamental_filings",
        primary_key=("ticker", "dimension", "datekey", "reportperiod"),
        date_column="datekey",
        entity_column="ticker",
    ),
    DatasetRegistration(
        dataset_name="fundamentals.daily_valuation_metrics",
        dataset_group="fundamentals",
        parquet_path="fundamentals/daily_valuation_metrics.parquet",
        metadata_path="fundamentals/daily_valuation_metrics.metadata.json",
        owner_repo="finbot-data",
        producer_job="download_daily_valuation_metrics/update_daily_valuation_metrics",
        write_mode="incremental_merge",
        completeness_profile="daily_ticker_metrics",
        primary_key=("ticker", "date"),
        date_column="date",
        entity_column="ticker",
    ),
)


def build_catalog(
    data_root: str | Path,
    output_dir: str | Path | None = None,
    *,
    now: datetime | None = None,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    registry: tuple[DatasetRegistration, ...] = DEFAULT_DATASET_REGISTRY,
) -> CatalogResult:
    """Scan a Finbot data root and write dataset catalog outputs."""

    data_root_path = Path(data_root).expanduser().resolve()
    output_dir_path = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else data_root_path / "catalog"
    )
    built_at = _normalize_datetime(now) or datetime.now(UTC)

    records = discover_datasets(
        data_root_path,
        built_at=built_at,
        freshness_days=freshness_days,
        exclude_dirs=[output_dir_path],
        registry=registry,
    )
    ops_records = build_ops_records(records, built_at=built_at)
    dataframe = pd.DataFrame(records, columns=_catalog_columns())
    ops_dataframe = pd.DataFrame(ops_records, columns=_ops_catalog_columns())
    output_dir_path.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir_path / CATALOG_PARQUET_NAME
    json_path = output_dir_path / CATALOG_JSON_NAME
    ops_parquet_path = output_dir_path / OPS_CATALOG_PARQUET_NAME
    ops_json_path = output_dir_path / OPS_CATALOG_JSON_NAME
    dataframe.to_parquet(parquet_path, index=False)
    ops_dataframe.to_parquet(ops_parquet_path, index=False)
    json_path.write_text(
        json.dumps(records, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    ops_json_path.write_text(
        json.dumps(ops_records, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return CatalogResult(
        dataframe=dataframe,
        parquet_path=parquet_path,
        json_path=json_path,
        ops_dataframe=ops_dataframe,
        ops_parquet_path=ops_parquet_path,
        ops_json_path=ops_json_path,
    )


def discover_datasets(
    data_root: Path,
    *,
    built_at: datetime,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    exclude_dirs: list[Path] | None = None,
    registry: tuple[DatasetRegistration, ...] = DEFAULT_DATASET_REGISTRY,
) -> list[dict[str, Any]]:
    """Return catalog records without writing output files."""

    data_root = data_root.resolve()
    ignored_dirs = [data_root / "catalog", data_root / "research"]
    if exclude_dirs:
        ignored_dirs.extend(exclude_dirs)
    metadata_files = sorted(
        path
        for path in data_root.rglob("*.metadata.json")
        if not _is_under_any(path, ignored_dirs)
    )
    parquet_files = sorted(
        path
        for path in data_root.rglob("*.parquet")
        if not _is_under_any(path, ignored_dirs)
    )

    records: list[dict[str, Any]] = []
    seen_metadata: set[Path] = set()
    seen_parquets: set[Path] = set()
    registry_by_metadata = {
        (data_root / item.metadata_path).resolve(): item
        for item in registry
    }
    registry_by_parquet = {
        (data_root / item.parquet_path).resolve(): item
        for item in registry
    }

    for item in registry:
        metadata_path = data_root / item.metadata_path
        parquet_path = data_root / item.parquet_path
        metadata, metadata_error = _load_metadata(metadata_path)
        seen_metadata.add(metadata_path.resolve())
        seen_parquets.add(parquet_path.resolve())
        records.append(
            _build_record(
                data_root=data_root,
                metadata_path=metadata_path,
                parquet_path=parquet_path,
                metadata=metadata,
                metadata_error=metadata_error,
                built_at=built_at,
                freshness_days=freshness_days,
                registration=item,
                discovery_status=_discovery_status(
                    registered=True,
                    metadata_exists=metadata_path.exists(),
                    parquet_exists=parquet_path.exists(),
                ),
            )
        )

    for metadata_path in metadata_files:
        if metadata_path.resolve() in seen_metadata:
            continue
        metadata, metadata_error = _load_metadata(metadata_path)
        parquet_path = _resolve_parquet_path(metadata_path, metadata)
        if parquet_path.exists():
            seen_parquets.add(parquet_path.resolve())
        registration = registry_by_metadata.get(metadata_path.resolve()) or registry_by_parquet.get(parquet_path.resolve())
        records.append(
            _build_record(
                data_root=data_root,
                metadata_path=metadata_path,
                parquet_path=parquet_path,
                metadata=metadata,
                metadata_error=metadata_error,
                built_at=built_at,
                freshness_days=freshness_days,
                registration=registration,
                discovery_status=_discovery_status(
                    registered=registration is not None,
                    metadata_exists=metadata_path.exists(),
                    parquet_exists=parquet_path.exists(),
                ),
            )
        )

    for parquet_path in parquet_files:
        resolved = parquet_path.resolve()
        if resolved in seen_parquets:
            continue
        sidecar_path = parquet_path.with_name(f"{parquet_path.stem}.metadata.json")
        if sidecar_path.exists():
            continue
        registration = registry_by_parquet.get(resolved)
        records.append(
            _build_record(
                data_root=data_root,
                metadata_path=sidecar_path,
                parquet_path=parquet_path,
                metadata={},
                metadata_error=None,
                built_at=built_at,
                freshness_days=freshness_days,
                registration=registration,
                discovery_status=_discovery_status(
                    registered=registration is not None,
                    metadata_exists=False,
                    parquet_exists=True,
                ),
            )
        )

    records.sort(key=lambda row: row["dataset_name"])
    return records


def default_data_root() -> Path:
    """Return FINBOT_DATA_ROOT from the environment, .env, or ./data."""

    return Path(get_env("FINBOT_DATA_ROOT", "./data"))


def get_env(key: str, default: str = "", dotenv_path: Path | None = None) -> str:
    value = os.getenv(key)
    if value:
        return value

    dotenv_value = parse_dotenv_value(key, dotenv_path=dotenv_path)
    if dotenv_value:
        return dotenv_value

    return default


def parse_dotenv_value(key: str, dotenv_path: Path | None = None) -> str | None:
    dotenv_path = dotenv_path or Path.cwd() / DEFAULT_DOTENV_FILE
    if not dotenv_path.exists():
        return None

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        env_key, env_value = line.split("=", 1)
        if env_key.strip() == key:
            return env_value.strip().strip('"').strip("'")
    return None


def _build_record(
    *,
    data_root: Path,
    metadata_path: Path,
    parquet_path: Path,
    metadata: dict[str, Any],
    metadata_error: str | None,
    built_at: datetime,
    freshness_days: int,
    registration: DatasetRegistration | None = None,
    discovery_status: str | None = None,
) -> dict[str, Any]:
    metadata_exists = metadata_path.exists()
    parquet_exists = parquet_path.exists()
    parquet_info = _read_parquet_info(parquet_path) if parquet_exists else {}
    status, reason = _status_for(
        metadata_exists=metadata_exists,
        parquet_exists=parquet_exists,
        metadata=metadata,
        metadata_error=metadata_error,
        parquet_error=parquet_info.get("error"),
        built_at=built_at,
        freshness_days=freshness_days,
    )
    dataset_name = _string_or_none(metadata.get("dataset_name")) or (registration.dataset_name if registration else None)
    dataset_group = _string_or_none(metadata.get("dataset_group")) or (registration.dataset_group if registration else None)
    if dataset_name is None:
        dataset_name = _dataset_name(data_root, parquet_path, metadata_path)
    if dataset_group is None:
        dataset_group = _dataset_group(data_root, parquet_path, metadata_path)
    entity_column = _string_or_none(metadata.get("entity_column")) or (registration.entity_column if registration else None)
    entity_count = _entity_count(metadata, entity_column)
    details = _details_json(metadata, registration=registration)

    return {
        "dataset_name": dataset_name,
        "dataset_group": dataset_group,
        "discovery_status": discovery_status
        or _discovery_status(
            registered=registration is not None,
            metadata_exists=metadata_exists,
            parquet_exists=parquet_exists,
        ),
        "is_registered": registration is not None,
        "owner_repo": registration.owner_repo if registration else None,
        "producer_job": registration.producer_job if registration else None,
        "metadata_path": _relative_posix(data_root, metadata_path),
        "parquet_path": _relative_posix(data_root, parquet_path),
        "metadata_exists": metadata_exists,
        "parquet_exists": parquet_exists,
        "provider": _string_or_none(metadata.get("provider")),
        "generated_at_utc": _string_or_none(metadata.get("generated_at_utc")),
        "write_mode": _string_or_none(metadata.get("write_mode")) or (registration.write_mode if registration else None),
        "completeness_profile": _string_or_none(metadata.get("completeness_profile"))
        or (registration.completeness_profile if registration else None),
        "primary_key": json.dumps(_json_list(metadata.get("primary_key"), registration.primary_key if registration else None)),
        "date_column": _string_or_none(metadata.get("date_column")) or (registration.date_column if registration else None),
        "entity_column": entity_column,
        "row_count": _first_int(metadata.get("row_count"), parquet_info.get("row_count")),
        "entity_count": entity_count,
        "min_date": _string_or_none(metadata.get("min_date")),
        "max_date": _string_or_none(metadata.get("max_date")),
        "latest_date": _string_or_none(metadata.get("latest_date")),
        "latest_date_coverage_count": _as_int(metadata.get("latest_date_coverage_count")),
        "latest_date_coverage_pct": _as_float(metadata.get("latest_date_coverage_pct")),
        "provider_min_lastupdated": _string_or_none(metadata.get("provider_min_lastupdated")),
        "provider_max_lastupdated": _string_or_none(metadata.get("provider_max_lastupdated")),
        "duplicate_key_count": _as_int(metadata.get("duplicate_key_count")),
        "missing_required_columns": json.dumps(_json_list(metadata.get("missing_required_columns"))),
        "parquet_columns": json.dumps(parquet_info.get("columns", [])),
        "parquet_schema": json.dumps(parquet_info.get("schema", []), sort_keys=True),
        "status": status,
        "status_reason": reason,
        "catalog_built_at": built_at.isoformat().replace("+00:00", "Z"),
        "details_json": details,
    }


def _load_metadata(metadata_path: Path) -> tuple[dict[str, Any], str | None]:
    if not metadata_path.exists():
        return {}, None
    try:
        value = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, f"metadata JSON is invalid: {exc.msg}"
    if not isinstance(value, dict):
        return {}, "metadata JSON must be an object"
    return value, None


def _resolve_parquet_path(metadata_path: Path, metadata: dict[str, Any]) -> Path:
    parquet_file = metadata.get("parquet_file")
    if isinstance(parquet_file, str) and parquet_file.strip():
        candidate = Path(parquet_file)
        if candidate.is_absolute():
            return candidate
        return metadata_path.parent / candidate
    stem = metadata_path.name.removesuffix(".metadata.json")
    return metadata_path.parent / f"{stem}.parquet"


def _read_parquet_info(parquet_path: Path) -> dict[str, Any]:
    try:
        parquet_file = pq.ParquetFile(parquet_path)
        schema = parquet_file.schema_arrow
    except Exception as exc:  # pragma: no cover - exact pyarrow exceptions vary
        return {
            "row_count": None,
            "columns": [],
            "schema": [],
            "error": f"could not read parquet metadata: {exc}",
        }

    return {
        "row_count": parquet_file.metadata.num_rows if parquet_file.metadata else None,
        "columns": list(schema.names),
        "schema": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
            }
            for field in schema
        ],
    }


def _status_for(
    *,
    metadata_exists: bool,
    parquet_exists: bool,
    metadata: dict[str, Any],
    metadata_error: str | None,
    parquet_error: str | None,
    built_at: datetime,
    freshness_days: int,
) -> tuple[str, str]:
    if not metadata_exists and not parquet_exists:
        return "missing", "metadata and parquet files are missing"
    if not metadata_exists:
        return "missing", "metadata file is missing"
    if not parquet_exists:
        return "missing", "parquet file is missing"
    if metadata_error:
        return "failed", metadata_error
    if parquet_error:
        return "failed", parquet_error
    if _metadata_indicates_failure(metadata):
        return "failed", "metadata indicates one or more collection failures"
    if _metadata_indicates_partial(metadata):
        return "partial", "metadata indicates the dataset is partial or has pending work"

    collected_at = _metadata_collection_time(metadata)
    if collected_at is None:
        return "unknown", "metadata does not include a collection timestamp"

    age_seconds = (built_at - collected_at).total_seconds()
    if age_seconds < 0:
        return "fresh", "metadata collection timestamp is in the future"

    age_days = age_seconds / 86_400
    if age_days <= freshness_days:
        return "fresh", f"generated {age_days:.1f} days ago"
    return "stale", f"generated {age_days:.1f} days ago"


def build_ops_records(records: list[dict[str, Any]], *, built_at: datetime) -> list[dict[str, Any]]:
    observed_at = built_at.isoformat().replace("+00:00", "Z")
    ops_records = []
    for record in records:
        ops_records.append(
            {
                "record_type": "dataset_state",
                "dataset_name": record["dataset_name"],
                "dataset_group": record["dataset_group"],
                "status": record["status"],
                "status_reason": record["status_reason"],
                "discovery_status": record["discovery_status"],
                "is_registered": record["is_registered"],
                "owner_repo": record["owner_repo"],
                "producer_job": record["producer_job"],
                "write_mode": record["write_mode"],
                "completeness_profile": record["completeness_profile"],
                "row_count": record["row_count"],
                "entity_count": record["entity_count"],
                "min_date": record["min_date"],
                "max_date": record["max_date"],
                "latest_date": record["latest_date"],
                "latest_date_coverage_count": record["latest_date_coverage_count"],
                "latest_date_coverage_pct": record["latest_date_coverage_pct"],
                "provider_min_lastupdated": record["provider_min_lastupdated"],
                "provider_max_lastupdated": record["provider_max_lastupdated"],
                "duplicate_key_count": record["duplicate_key_count"],
                "missing_required_columns": record["missing_required_columns"],
                "generated_at_utc": record["generated_at_utc"],
                "observed_at_utc": observed_at,
                "details_json": record["details_json"],
            }
        )
    return ops_records


def _metadata_indicates_failure(metadata: dict[str, Any]) -> bool:
    if metadata.get("failed") is True:
        return True
    if str(metadata.get("status", "")).lower() == "failed":
        return True
    if _has_items(metadata.get("failures")) or _has_items(metadata.get("errors")):
        return True
    return _has_items(metadata.get("failed_tickers"))


def _metadata_indicates_partial(metadata: dict[str, Any]) -> bool:
    if metadata.get("partial") is True:
        return True
    if _positive_int(metadata.get("pending_tickers")):
        return True
    if _positive_int(metadata.get("missing_count")):
        return True
    return _has_items(metadata.get("missing_tickers"))


def _metadata_collection_time(metadata: dict[str, Any]) -> datetime | None:
    return _parse_datetime(metadata.get("generated_at_utc"))


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _dataset_name(data_root: Path, parquet_path: Path, metadata_path: Path) -> str:
    source = parquet_path if parquet_path.exists() else metadata_path
    try:
        relative = source.relative_to(data_root)
    except ValueError:
        relative = source.name
        return str(relative).removesuffix(".metadata.json").removesuffix(".parquet")
    if relative.name.endswith(".metadata.json"):
        return str(relative.with_name(relative.name.removesuffix(".metadata.json"))).replace(
            "/",
            ".",
        )
    return str(relative.with_suffix("")).replace("/", ".")


def _dataset_group(data_root: Path, parquet_path: Path, metadata_path: Path) -> str | None:
    source = parquet_path if parquet_path.exists() else metadata_path
    try:
        relative = source.relative_to(data_root)
    except ValueError:
        return None
    return relative.parts[0] if relative.parts else None


def _relative_posix(data_root: Path, path: Path) -> str:
    try:
        return path.relative_to(data_root).as_posix()
    except ValueError:
        return path.as_posix()


def _catalog_columns() -> list[str]:
    return [
        "dataset_name",
        "dataset_group",
        "discovery_status",
        "is_registered",
        "owner_repo",
        "producer_job",
        "metadata_path",
        "parquet_path",
        "metadata_exists",
        "parquet_exists",
        "provider",
        "generated_at_utc",
        "write_mode",
        "completeness_profile",
        "primary_key",
        "date_column",
        "entity_column",
        "row_count",
        "entity_count",
        "min_date",
        "max_date",
        "latest_date",
        "latest_date_coverage_count",
        "latest_date_coverage_pct",
        "provider_min_lastupdated",
        "provider_max_lastupdated",
        "duplicate_key_count",
        "missing_required_columns",
        "parquet_columns",
        "parquet_schema",
        "status",
        "status_reason",
        "catalog_built_at",
        "details_json",
    ]


def _ops_catalog_columns() -> list[str]:
    return [
        "record_type",
        "dataset_name",
        "dataset_group",
        "status",
        "status_reason",
        "discovery_status",
        "is_registered",
        "owner_repo",
        "producer_job",
        "write_mode",
        "completeness_profile",
        "row_count",
        "entity_count",
        "min_date",
        "max_date",
        "latest_date",
        "latest_date_coverage_count",
        "latest_date_coverage_pct",
        "provider_min_lastupdated",
        "provider_max_lastupdated",
        "duplicate_key_count",
        "missing_required_columns",
        "generated_at_utc",
        "observed_at_utc",
        "details_json",
    ]


def _first_int(*values: Any) -> int | None:
    for value in values:
        parsed = _as_int(value)
        if parsed is not None:
            return parsed
    return None


def _discovery_status(*, registered: bool, metadata_exists: bool, parquet_exists: bool) -> str:
    if registered and metadata_exists and parquet_exists:
        return "registered_present"
    if registered and not metadata_exists and not parquet_exists:
        return "registered_missing"
    if metadata_exists and not parquet_exists:
        return "metadata_only"
    if parquet_exists and not metadata_exists:
        return "parquet_only"
    if not registered:
        return "discovered_unregistered"
    return "registered_missing"


def _entity_count(metadata: dict[str, Any], entity_column: str | None) -> int | None:
    if entity_column is None:
        return None
    return _first_int(
        metadata.get(f"{entity_column}_count"),
        metadata.get("entity_count"),
    )


def _details_json(metadata: dict[str, Any], *, registration: DatasetRegistration | None) -> str:
    details: dict[str, Any] = {}
    for key in (
        "input_tickers",
        "update_raw_rows",
        "update_rows",
        "update_filter",
        "lastupdated_gte",
        "ticker_universe_file",
        "dimensions",
        "dimension_ticker_counts",
        "min_reportperiod",
        "max_reportperiod",
        "filter",
        "raw_input_file",
        "input_rows",
    ):
        if key in metadata:
            details[key] = metadata[key]
    if registration is not None:
        details["registered_parquet_path"] = registration.parquet_path
        details["registered_metadata_path"] = registration.metadata_path
    return json.dumps(details, sort_keys=True)


def _json_list(value: Any, fallback: tuple[str, ...] | None = None) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if fallback is not None:
        return list(fallback)
    return []


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _positive_int(value: Any) -> bool:
    parsed = _as_int(value)
    return parsed is not None and parsed > 0


def _has_items(value: Any) -> bool:
    if isinstance(value, dict):
        return bool(value)
    if isinstance(value, list | tuple | set):
        return len(value) > 0
    if isinstance(value, str):
        return bool(value.strip())
    return False


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def _is_under_any(path: Path, parents: list[Path]) -> bool:
    return any(_is_relative_to(path, parent) for parent in parents)
