from __future__ import annotations

import argparse
from pathlib import Path

from finbot_catalog.catalog import build_catalog, default_data_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the Finbot dataset catalog.")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Data root to scan. Defaults to FINBOT_DATA_ROOT, then ./data.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to DATA_ROOT/catalog.",
    )
    args = parser.parse_args(argv)

    data_root = args.data_root or default_data_root()
    result = build_catalog(data_root=data_root, output_dir=args.output_dir)

    print(f"Built {len(result.dataframe)} dataset catalog records")
    print(f"Wrote {result.parquet_path}")
    print(f"Wrote {result.json_path}")
    print(f"Wrote {result.ops_parquet_path}")
    print(f"Wrote {result.ops_json_path}")
    return 0
