#!/usr/bin/env python3
"""Cross-platform entry point for a full raw and structured download."""

from __future__ import annotations

import sys
from pathlib import Path

from izbirkom_full_parser import main


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    defaults = [
        "--election-id", "587813923",
        "--from-date", "2026-09-18",
        "--to-date", "2026-09-20",
        "--stages", "reports,candidates",
        "--no-files",
        "--workers", "1",
        "--commission-workers", "16",
        "--delay", "0.04",
        "--output", str(root / "download" / "izbirkom_2026-09-18_20"),
    ]
    sys.argv = [sys.argv[0], *defaults, *sys.argv[1:]]
    main()
