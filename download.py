#!/usr/bin/env python3
"""Cross-platform entry point with the proven PC/laptop work split."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from izbirkom_full_parser import main


if __name__ == "__main__":
    profile_parser = argparse.ArgumentParser(add_help=False)
    profile_parser.add_argument(
        "--profile", choices=("single", "pc", "laptop"), default="single"
    )
    profile, remaining = profile_parser.parse_known_args()
    root = Path(__file__).resolve().parent
    defaults = [
        "--election-id", "587813923",
        "--from-date", "2026-09-18",
        "--to-date", "2026-09-20",
        "--stages", "reports,candidates",
        "--no-files",
        "--workers", "1",
        "--commission-workers", "16" if profile.profile == "laptop" else "24",
        "--delay", "0.04",
        "--output", str(root / "download" / profile.profile / "izbirkom_2026-09-18_20"),
    ]
    if profile.profile in ("pc", "laptop"):
        defaults.extend(("--commission-shards", "3"))
        for shard in ((0, 1) if profile.profile == "pc" else (2,)):
            defaults.extend(("--commission-shard", str(shard)))
    if profile.profile == "laptop":
        index = defaults.index("--stages")
        defaults[index + 1] = "reports"
    print(
        f"[ПРОФИЛЬ] {profile.profile}; результаты: "
        f"{root / 'download' / profile.profile / 'izbirkom_2026-09-18_20'}",
        flush=True,
    )
    sys.argv = [sys.argv[0], *defaults, *remaining]
    main()
