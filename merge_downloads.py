#!/usr/bin/env python3
"""Combine the proven two-machine profiles, then rebuild JSONL exports."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from izbirkom_full_parser import build_exports


def main() -> None:
    root = Path(__file__).resolve().parent
    election = "izbirkom_2026-09-18_20"
    parser = argparse.ArgumentParser(description="Объединить загрузки ПК и ноутбука")
    parser.add_argument("--pc-dir", type=Path, default=root / "download" / "pc" / election)
    parser.add_argument("--laptop-dir", type=Path, default=root / "download" / "laptop" / election)
    parser.add_argument("--output", type=Path, default=root / "download" / "merged" / election)
    args = parser.parse_args()
    pc, laptop, output = (p.resolve() for p in (args.pc_dir, args.laptop_dir, args.output))
    for source in (pc, laptop):
        if not (source / "raw" / "elections" / "587813923").is_dir():
            parser.error(f"Нет данных выборов в {source}")
        if output == source or source in output.parents:
            parser.error("Выходная папка должна быть отдельно от исходных")
    if output.exists():
        parser.error(f"Выходная папка уже существует: {output}. Выберите новую через --output")

    print("Копирую исходные данные ПК…", flush=True)
    shutil.copytree(pc, output, ignore=shutil.ignore_patterns("exports"))

    election_rel = Path("raw") / "elections" / "587813923"
    added = 0
    for group in ("commission_report_catalog", "commission_reports", "results", "deg_results"):
        source_dir = laptop / election_rel / group
        if not source_dir.exists():
            continue
        for source in source_dir.rglob("*"):
            if not source.is_file():
                continue
            target = output / election_rel / group / source.relative_to(source_dir)
            if target.exists():
                if target.read_bytes() != source.read_bytes():
                    raise RuntimeError(f"Разное содержимое одного ответа: {target}")
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            added += 1

    print(f"Добавлено файлов ноутбука: {added}. Собираю exports/…", flush=True)
    build_exports(output)
    print(f"Готово: {output}", flush=True)


if __name__ == "__main__":
    main()
