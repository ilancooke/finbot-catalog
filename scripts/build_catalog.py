#!/usr/bin/env python
"""Build the Finbot dataset catalog."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from finbot_catalog.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
