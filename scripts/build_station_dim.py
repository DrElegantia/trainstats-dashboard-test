from __future__ import annotations

import os
from typing import Dict, Any

import pandas as pd

from .utils import ensure_dir, load_yaml


def load_gold_station_table() -> pd.DataFrame:
    p = os.path.join("data", "gold", "stazioni_mese_categoria_nodo.csv")
    if not os.path.exists(p):
        raise FileNotFoundError(f"missing gold table: {p}")
    return pd.read_csv(p, dtype=str)


def load_station_registry() -> pd.DataFrame:
    p = os.path.join("data", "stations", "stations.csv")
    if not os.path.exists(p):
        raise FileNotFoundError(f"missing station registry: {p}")
    df = pd.read_csv(p, dtype=str)

    rename_map = {}
    if "codice" in df.columns and "cod_stazione" not in df.columns:
        rename_map["codice"] = "cod_stazione"
    if "nome_norm" in df.columns and "nome_stazione" not in df.columns:
        rename_map["nome_norm"] = "nome_stazione"
    if rename_map:
        df = df.rename(columns=rename_map)

    if "cod_stazione" not in df.columns:
        raise ValueError("station registry must contain 'cod_stazione' (or 'codice')")

    df["cod_stazione"] = df["cod_stazione"].astype(str).str.strip()

    for c in ("lat", "lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def main() -> None:
    _cfg: Dict[str, Any] = load_yaml("config/pipeline.yml")

    gold = load_gold_station_table()
    reg = load_station_registry()

    if "cod_stazione" not in gold.columns or "nome_stazione" not in gold.columns:
        raise ValueError("gold table must contain 'cod_stazione' and 'nome_stazione' columns")

    seen = gold[["cod_stazione", "nome_stazione"]].drop_duplicates().copy()
    seen["cod_stazione"] = seen["cod_stazione"].astype(str).str.strip()

    joined = seen.merge(reg, on="cod_stazione", how="left", suffixes=("", "_reg"))

    out_dir = os.path.join("site", "data")
    ensure_dir(out_dir)
    joined.to_csv(os.path.join(out_dir, "stations_dim.csv"), index=False)

    missing_cols = ["cod_stazione", "nome_stazione"]
    missing_mask = joined["lat"].isna() | joined["lon"].isna() if {"lat", "lon"}.issubset(joined.columns) else pd.Series(True, index=joined.index)
    missing = joined.loc[missing_mask, missing_cols].drop_duplicates()

    missing_path = os.path.join("data", "stations", "stations_unknown.csv")
    ensure_dir(os.path.dirname(missing_path))
    missing.to_csv(missing_path, index=False)

    print({"stations_dim_rows": int(len(joined)), "stations_missing_coords": int(len(missing))})


if __name__ == "__main__":
    main()
