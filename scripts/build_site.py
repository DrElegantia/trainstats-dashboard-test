# scripts/build_site.py
from __future__ import annotations

import json
import shutil
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def build_manifest(gold_dir: Path) -> dict:
    gold_files = sorted([p.name for p in gold_dir.glob("*.csv") if p.is_file()])
    return {
        "built_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "gold_files": gold_files,
    }



HEAVY_DAILY_FILES = {
    "od_giorno_categoria.csv",
    "stazioni_giorno_categoria_nodo.csv",
    "stazioni_giorno_categoria_ruolo.csv",
    "hist_stazioni_giorno_categoria_ruolo.csv",
}


def _maybe_trim_daily_csv(src: Path, dst: Path, max_daily_days: int) -> None:
    """Trim heavy daily CSVs to the most recent N days for web payload size control."""
    if src.name not in HEAVY_DAILY_FILES:
        shutil.copy2(src, dst)
        print(f"Copied {src.name} -> {dst}")
        return

    try:
        df = pd.read_csv(src, low_memory=False)
        if "giorno" not in df.columns or max_daily_days <= 0:
            shutil.copy2(src, dst)
            print(f"Copied {src.name} (no trim applicable) -> {dst}")
            return

        dates = pd.to_datetime(df["giorno"], errors="coerce")
        max_date = dates.max()
        if pd.isna(max_date):
            shutil.copy2(src, dst)
            print(f"Copied {src.name} (invalid giorno) -> {dst}")
            return

        cutoff = max_date - pd.Timedelta(days=max_daily_days - 1)
        trimmed = df[dates >= cutoff].copy()
        trimmed.to_csv(dst, index=False, encoding="utf-8")
        print(
            f"Copied {src.name} -> {dst} (trimmed to {len(trimmed):,} rows, "
            f"last {max_daily_days} days from {cutoff.date()} to {max_date.date()})"
        )
    except Exception as e:
        shutil.copy2(src, dst)
        print(f"Copied {src.name} (trim failed: {e}) -> {dst}")


def copy_gold_files(gold_dir: Path, target_dir: Path, max_daily_days: int) -> None:
    """Copia i file CSV gold direttamente in docs/data/ (non in una sottocartella)."""
    ensure_dir(target_dir)
    for p in gold_dir.glob("*.csv"):
        if p.is_file():
            _maybe_trim_daily_csv(p, target_dir / p.name, max_daily_days=max_daily_days)


def copy_root_files(target_dir: Path) -> None:
    """Copia stations_dim.csv e altri file dalla root di data/"""
    ensure_dir(target_dir)
    
    # stations_dim.csv potrebbe essere in data/gold/ o in data/
    stations_paths = [
        Path("data") / "gold" / "stations_dim.csv",
        Path("data") / "stations_dim.csv"
    ]
    
    for src in stations_paths:
        if src.exists():
            shutil.copy2(src, target_dir / "stations_dim.csv")
            print(f"Copied stations_dim.csv from {src}")
            break
    
    # Capoluoghi provincia (supporta sia data/ che data/stations/)
    capoluoghi_paths = [
        Path("data") / "capoluoghi_provincia.csv",
        Path("data") / "stations" / "capoluoghi_provincia.csv",
    ]
    for capoluoghi in capoluoghi_paths:
        if capoluoghi.exists():
            shutil.copy2(capoluoghi, target_dir / "capoluoghi_provincia.csv")
            print(f"Copied capoluoghi_provincia.csv from {capoluoghi}")
            break


def write_manifest(target_dir: Path, manifest: dict) -> None:
    ensure_dir(target_dir)
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Wrote manifest to {manifest_path}")


def main() -> None:
    gold_dir = Path("data") / "gold"
    if not gold_dir.is_dir():
        raise SystemExit("missing data/gold, run scripts.build_gold first")

    manifest = build_manifest(gold_dir)
    
    # Target principale: docs/data
    target = Path("docs") / "data"
    
    max_daily_days = int(os.environ.get("SITE_MAX_DAILY_DAYS", "365"))
    print(f"Site payload daily window: {max_daily_days} days")

    copy_gold_files(gold_dir, target, max_daily_days=max_daily_days)
    copy_root_files(target)
    write_manifest(target, manifest)

    print(
        json.dumps(
            {
                "target": str(target),
                "gold_dir": str(gold_dir),
                "manifest": manifest,
            },
            ensure_ascii=False,
            indent=2
        )
    )


if __name__ == "__main__":
    main()
