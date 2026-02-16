# scripts/build_site.py
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def list_targets() -> list[Path]:
    targets: list[Path] = [Path("docs") / "data"]
    legacy = Path("trainstats-dashboard") / "docs" / "data"
    if (Path("trainstats-dashboard") / "docs").is_dir():
        targets.append(legacy)
    return targets


def build_manifest(gold_dir: Path) -> dict:
    gold_files = sorted([p.name for p in gold_dir.glob("*.csv") if p.is_file()])
    return {
        "built_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "gold_files": gold_files,
    }


def copy_gold(gold_dir: Path, target_data_dir: Path) -> None:
    out_gold = target_data_dir / "gold"
    ensure_dir(out_gold)
    for p in gold_dir.glob("*.csv"):
        if p.is_file():
            shutil.copy2(p, out_gold / p.name)


def copy_root_files(target_data_dir: Path) -> None:
    ensure_dir(target_data_dir)
    for fname in ["stations_dim.csv", "capoluoghi_provincia.csv"]:
        src = Path("data") / fname
        if src.exists():
            shutil.copy2(src, target_data_dir / fname)


def write_manifest(target_data_dir: Path, manifest: dict) -> None:
    ensure_dir(target_data_dir)
    (target_data_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    out_gold = target_data_dir / "gold"
    ensure_dir(out_gold)
    (out_gold / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main() -> None:
    gold_dir = Path("data") / "gold"
    if not gold_dir.is_dir():
        raise SystemExit("missing data/gold, run scripts.build_gold first")

    manifest = build_manifest(gold_dir)

    for t in list_targets():
        copy_gold(gold_dir, t)
        copy_root_files(t)
        write_manifest(t, manifest)

    print(
        json.dumps(
            {
                "site_targets": [str(t) for t in list_targets()],
                "gold_dir": str(gold_dir),
                "manifest": manifest,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
