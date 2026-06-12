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
DEFAULT_FRESHNESS_DAYS = 7
DEFAULT_DOTENV_FILE = ".env"


@dataclass(frozen=True)
class CatalogResult:
    dataframe: pd.DataFrame
    parquet_path: Path
    json_path: Path


def build_catalog(
    data_root: str | Path,
    output_dir: str | Path | None = None,
    *,
    now: datetime | None = None,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
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
    )
    dataframe = pd.DataFrame(records, columns=_catalog_columns())
    output_dir_path.mkdir(parents=True, exist_ok=True)

    parquet_path = output_dir_path / CATALOG_PARQUET_NAME
    json_path = output_dir_path / CATALOG_JSON_NAME
    dataframe.to_parquet(parquet_path, index=False)
    json_path.write_text(
        json.dumps(records, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return CatalogResult(
        dataframe=dataframe,
        parquet_path=parquet_path,
        json_path=json_path,
    )


def discover_datasets(
    data_root: Path,
    *,
    built_at: datetime,
    freshness_days: int = DEFAULT_FRESHNESS_DAYS,
    exclude_dirs: list[Path] | None = None,
) -> list[dict[str, Any]]:
    """Return catalog records without writing output files."""

    data_root = data_root.resolve()
    ignored_dirs = [data_root / "catalog"]
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
    seen_parquets: set[Path] = set()

    for metadata_path in metadata_files:
        metadata, metadata_error = _load_metadata(metadata_path)
        parquet_path = _resolve_parquet_path(metadata_path, metadata)
        if parquet_path.exists():
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
            )
        )

    for parquet_path in parquet_files:
        resolved = parquet_path.resolve()
        if resolved in seen_parquets:
            continue
        sidecar_path = parquet_path.with_name(f"{parquet_path.stem}.metadata.json")
        if sidecar_path.exists():
            continue
        records.append(
            _build_record(
                data_root=data_root,
                metadata_path=sidecar_path,
                parquet_path=parquet_path,
                metadata={},
                metadata_error=None,
                built_at=built_at,
                freshness_days=freshness_days,
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
) -> dict[str, Any]:
    metadata_exists = metadata_path.exists()
    parquet_exists = parquet_path.exists()
    parquet_info = _read_parquet_info(parquet_path) if parquet_exists else {}
    status, reason = _status_for(
        metadata_exists=metadata_exists,
        parquet_exists=parquet_exists,
        metadata=metadata,
        metadata_error=metadata_error,
        built_at=built_at,
        freshness_days=freshness_days,
    )

    return {
        "dataset_name": _dataset_name(data_root, parquet_path, metadata_path),
        "dataset_group": _dataset_group(data_root, parquet_path, metadata_path),
        "metadata_path": _relative_posix(data_root, metadata_path),
        "parquet_path": _relative_posix(data_root, parquet_path),
        "metadata_exists": metadata_exists,
        "parquet_exists": parquet_exists,
        "provider": _string_or_none(metadata.get("provider")),
        "collection_timestamp": _string_or_none(
            metadata.get("collected_at_utc") or metadata.get("collected_date_utc")
        ),
        "row_count": _first_int(
            metadata.get("rows"),
            metadata.get("row_count"),
            parquet_info.get("row_count"),
        ),
        "symbol_count": _first_int(
            metadata.get("symbols"),
            metadata.get("tickers"),
            metadata.get("symbol_count"),
            metadata.get("ticker_count"),
        ),
        "data_min_date": _string_or_none(
            metadata.get("data_min_date")
            or metadata.get("min_date")
            or metadata.get("start_date")
        ),
        "data_max_date": _string_or_none(
            metadata.get("data_max_date")
            or metadata.get("max_date")
            or metadata.get("end_date")
        ),
        "parquet_columns": json.dumps(parquet_info.get("columns", [])),
        "parquet_schema": json.dumps(parquet_info.get("schema", []), sort_keys=True),
        "status": status,
        "status_reason": reason,
        "catalog_built_at": built_at.isoformat().replace("+00:00", "Z"),
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
        return "fresh", f"collected {age_days:.1f} days ago"
    return "stale", f"collected {age_days:.1f} days ago"


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
    for key in ("collected_at_utc", "collection_timestamp", "created_at_utc"):
        value = metadata.get(key)
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed

    for key in ("collected_date_utc", "collection_date"):
        value = metadata.get(key)
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


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
        "metadata_path",
        "parquet_path",
        "metadata_exists",
        "parquet_exists",
        "provider",
        "collection_timestamp",
        "row_count",
        "symbol_count",
        "data_min_date",
        "data_max_date",
        "parquet_columns",
        "parquet_schema",
        "status",
        "status_reason",
        "catalog_built_at",
    ]


def _first_int(*values: Any) -> int | None:
    for value in values:
        parsed = _as_int(value)
        if parsed is not None:
            return parsed
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
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
