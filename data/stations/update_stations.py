from __future__ import annotations

import os
from datetime import date
from typing import Dict, Tuple

import pandas as pd

from scripts.utils import ensure_dir, load_yaml, normalize_station_name, today_in_tz


def load_known() -> pd.DataFrame:
    p = os.path.join("stations", "stations.csv")
    if not os.path.exists(p):
        return pd.DataFrame(columns=["codice", "nome_norm", "lat", "lon"])
    return pd.read_csv(p, dtype={"codice": str, "nome_norm": str})


def load_unknown() -> pd.DataFrame:
    p = os.path.join("stations", "stations_unknown.csv")
    if not os.path.exists(p):
        return pd.DataFrame(columns=["codice", "nome_norm", "first_seen_date", "last_seen_date", "count_seen"])
    return pd.read_csv(p, dtype=str)


def list_silver_months() -> list[str]:
    root = os.path.join("data", "silver")
    out: list[str] = []
    if not os.path.exists(root):
        return out
    for y in os.listdir(root):
        yp = os.path.join(root, y)
        if not os.path.isdir(yp):
            continue
        for fn in os.listdir(yp):
            if fn.endswith(".parquet"):
                out.append(os.path.join(yp, fn))
    return sorted(out)


def main() -> None:
    cfg = load_yaml("config/pipeline.yml")
    tz_name = cfg["project"]["timezone"]
    today = today_in_tz(tz_name).isoformat()

    known = load_known()
    unknown = load_unknown()

    known_codes = set(known["codice"].astype(str).tolist())

    files = list_silver_months()
    if not files:
        print("no silver found, nothing to update")
        return

    df = pd.concat([pd.read_parquet(p, columns=["Codice stazione partenza", "Nome stazione partenza", "Codice stazione arrivo", "Nome stazione arrivo", "data_riferimento"]) for p in files], ignore_index=True)

    pairs = []
    pairs.append(df[["Codice stazione partenza", "Nome stazione partenza", "data_riferimento"]].rename(columns={"Codice stazione partenza": "codice", "Nome stazione partenza": "nome"}))
    pairs.append(df[["Codice stazione arrivo", "Nome stazione arrivo", "data_riferimento"]].rename(columns={"Codice stazione arrivo": "codice", "Nome stazione arrivo": "nome"}))
    st = pd.concat(pairs, ignore_index=True)

    st["codice"] = st["codice"].astype(str).str.strip()
    st["nome_norm"] = st["nome"].map(normalize_station_name)
    st["data_riferimento"] = st["data_riferimento"].astype(str)

    st = st[(st["codice"] != "") & st["codice"].notna()]
    st_new = st[~st["codice"].isin(known_codes)].copy()
    if st_new.empty:
        print("no new station codes found")
        return

    agg = st_new.groupby(["codice", "nome_norm"], dropna=False).agg(
        first_seen_date=("data_riferimento", "min"),
        last_seen_date=("data_riferimento", "max"),
        count_seen=("data_riferimento", "count"),
    ).reset_index()

    if unknown.empty:
        updated = agg
    else:
        u = unknown.copy()
        u["count_seen"] = pd.to_numeric(u["count_seen"], errors="coerce").fillna(0).astype(int)
        agg["count_seen"] = pd.to_numeric(agg["count_seen"], errors="coerce").fillna(0).astype(int)

        merged = pd.merge(u, agg, on=["codice", "nome_norm"], how="outer", suffixes=("_old", "_new"))
        merged["first_seen_date"] = merged[["first_seen_date_old", "first_seen_date_new"]].min(axis=1)
        merged["last_seen_date"] = merged[["last_seen_date_old", "last_seen_date_new"]].max(axis=1)
        merged["count_seen"] = merged["count_seen_old"].fillna(0).astype(int) + merged["count_seen_new"].fillna(0).astype(int)
        updated = merged[["codice", "nome_norm", "first_seen_date", "last_seen_date", "count_seen"]]

    ensure_dir("stations")
    updated.to_csv(os.path.join("stations", "stations_unknown.csv"), index=False)
    print({"unknown_stations_updated_rows": int(len(updated))})


if __name__ == "__main__":
    main()

