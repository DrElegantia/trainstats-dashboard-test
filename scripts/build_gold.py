# build_gold.py
from __future__ import annotations

import os
from typing import Dict, Any, List

import pandas as pd

from .utils import ensure_dir, load_yaml


def _read_silver(start: str, end: str) -> pd.DataFrame:
    """
    Atteso in silver almeno:
      data_ora_partenza (datetime), categoria (str),
      cod_partenza (str), cod_arrivo (str),
      cancellato (bool/int), soppresso (bool/int), parzialmente_cancellato (bool/int), info_mancante (bool/int),
      ritardo_arrivo_min (float/int) e/o ritardo_partenza_min (float/int) e/o anticipo_min
    """
    p = os.path.join("data", "silver", f"treni_{start}_{end}.parquet")
    if not os.path.exists(p):
        raise FileNotFoundError(f"missing silver parquet: {p}")
    return pd.read_parquet(p)


def _to_date_str(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")


def _to_month_str(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m")


def _to_hour_int(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.hour.astype("Int64")


def _bucketize_delay(minutes: pd.Series, buckets: List[int], labels: List[str]) -> pd.Series:
    """
    minutes: minuti di ritardo (può essere <0 per anticipo)
    buckets: soglie in minuti, es: [0,5,10,15,30,60,999999]
    labels: etichette per ciascun intervallo, lunghezza = len(buckets)-1
    """
    m = pd.to_numeric(minutes, errors="coerce").fillna(0.0)
    # ritardo negativo lo consideriamo nel primo bucket (in orario o anticipo)
    m = m.clip(lower=0)
    cuts = pd.cut(m, bins=buckets, labels=labels, right=False, include_lowest=True)
    return cuts.astype(str)


def _build_kpi_base(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara colonne KPI base a livello corsa.
    """
    out = df.copy()

    # colonna temporale riferimento
    if "data_ora_partenza" not in out.columns:
        raise ValueError("silver must contain 'data_ora_partenza'")

    out["giorno"] = _to_date_str(out["data_ora_partenza"])
    out["mese"] = _to_month_str(out["data_ora_partenza"])
    out["ora"] = _to_hour_int(out["data_ora_partenza"])

    # normalizza categoria
    if "categoria" not in out.columns:
        out["categoria"] = "all"
    out["categoria"] = out["categoria"].astype(str).str.strip().replace({"": "all"})

    # normalizza codici stazione
    for c in ("cod_partenza", "cod_arrivo"):
        if c not in out.columns:
            out[c] = ""
        out[c] = out[c].astype(str).str.strip()

    # flag corsa osservata (righe esistenti)
    out["corse_osservate"] = 1

    def _flag(name: str) -> pd.Series:
        if name not in out.columns:
            return pd.Series(0, index=out.index, dtype="int64")
        return pd.to_numeric(out[name], errors="coerce").fillna(0).astype("int64").clip(lower=0, upper=1)

    out["cancellate"] = _flag("cancellato")
    out["soppresse"] = _flag("soppresso")
    out["parzialmente_cancellate"] = _flag("parzialmente_cancellato")
    out["info_mancante"] = _flag("info_mancante")

    # effettuate = osservate - cancellate - soppresse (parzialmente cancellate restano effettuate)
    out["effettuate"] = (out["corse_osservate"] - out["cancellate"] - out["soppresse"]).clip(lower=0)

    # ritardi e anticipi
    if "ritardo_arrivo_min" in out.columns:
        delay = pd.to_numeric(out["ritardo_arrivo_min"], errors="coerce")
    elif "ritardo_partenza_min" in out.columns:
        delay = pd.to_numeric(out["ritardo_partenza_min"], errors="coerce")
    else:
        delay = pd.Series(0.0, index=out.index)

    delay = delay.fillna(0.0)

    out["in_orario"] = (delay == 0).astype("int64")
    out["in_ritardo"] = (delay > 0).astype("int64")
    out["in_anticipo"] = (delay < 0).astype("int64")

    # soglie ritardo (consideriamo solo valori >0)
    dpos = delay.clip(lower=0)
    out["oltre_5"] = (dpos >= 5).astype("int64")
    out["oltre_10"] = (dpos >= 10).astype("int64")
    out["oltre_15"] = (dpos >= 15).astype("int64")
    out["oltre_30"] = (dpos >= 30).astype("int64")
    out["oltre_60"] = (dpos >= 60).astype("int64")

    out["minuti_ritardo_tot"] = dpos.fillna(0.0)
    out["minuti_anticipo_tot"] = (-delay.clip(upper=0)).fillna(0.0)
    out["minuti_netti_tot"] = delay.fillna(0.0)

    out["ritardo_arrivo_min"] = delay

    return out


def _agg_core(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    agg_cols_sum = [
        "corse_osservate",
        "effettuate",
        "cancellate",
        "soppresse",
        "parzialmente_cancellate",
        "info_mancante",
        "in_orario",
        "in_ritardo",
        "in_anticipo",
        "oltre_5",
        "oltre_10",
        "oltre_15",
        "oltre_30",
        "oltre_60",
        "minuti_ritardo_tot",
        "minuti_anticipo_tot",
        "minuti_netti_tot",
    ]

    base = df[group_cols + agg_cols_sum + ["ritardo_arrivo_min"]].copy()
    g = base.groupby(group_cols, dropna=False, as_index=False)

    out = g[agg_cols_sum].sum()

    # percentili ritardo sui soli treni in ritardo
    def _q(s: pd.Series, q: float) -> float:
        s2 = pd.to_numeric(s, errors="coerce").dropna()
        s2 = s2[s2 > 0]
        if s2.empty:
            return 0.0
        return float(s2.quantile(q))

    q50 = g["ritardo_arrivo_min"].apply(lambda s: _q(s, 0.50)).rename(columns={"ritardo_arrivo_min": "p50_ritardo"})
    q90 = g["ritardo_arrivo_min"].apply(lambda s: _q(s, 0.90)).rename(columns={"ritardo_arrivo_min": "p90_ritardo"})
    q95 = g["ritardo_arrivo_min"].apply(lambda s: _q(s, 0.95)).rename(columns={"ritardo_arrivo_min": "p95_ritardo"})

    out = out.merge(q50, on=group_cols, how="left").merge(q90, on=group_cols, how="left").merge(q95, on=group_cols, how="left")
    out["p50_ritardo"] = out["p50_ritardo"].fillna(0.0)
    out["p90_ritardo"] = out["p90_ritardo"].fillna(0.0)
    out["p95_ritardo"] = out["p95_ritardo"].fillna(0.0)

    # media ritardo solo sui treni in ritardo
    def _mean_pos(s: pd.Series) -> float:
        s2 = pd.to_numeric(s, errors="coerce").dropna()
        s2 = s2[s2 > 0]
        if s2.empty:
            return 0.0
        return float(s2.mean())

    mean_delay = g["ritardo_arrivo_min"].apply(_mean_pos).rename(columns={"ritardo_arrivo_min": "media_ritardo"})
    out = out.merge(mean_delay, on=group_cols, how="left")
    out["media_ritardo"] = out["media_ritardo"].fillna(0.0)

    return out


def build_gold_tables(df_silver: pd.DataFrame, cfg: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Produce tutte le tabelle gold richieste dal sito.
    """
    mcfg = cfg.get("manifest", {})
    delay_cfg = mcfg.get("delay_buckets_minutes", {})
    buckets = delay_cfg.get("buckets", [0, 5, 10, 15, 30, 60, 999999])
    labels = delay_cfg.get("labels", ["0-4", "5-9", "10-14", "15-29", "30-59", "60+"])
    if len(labels) != len(buckets) - 1:
        labels = [str(i) for i in range(len(buckets) - 1)]

    base = _build_kpi_base(df_silver)

    tables: Dict[str, pd.DataFrame] = {}

    # KPI mensile per categoria
    tables["kpi_mese_categoria"] = _agg_core(base, ["mese", "categoria"]).sort_values(["mese", "categoria"]).reset_index(drop=True)

    # KPI giornaliero per categoria
    tables["kpi_giorno_categoria"] = _agg_core(base, ["giorno", "categoria"]).sort_values(["giorno", "categoria"]).reset_index(drop=True)

    # KPI giornaliero per ora per categoria
    tables["kpi_giorno_ora_categoria"] = _agg_core(base, ["giorno", "ora", "categoria"]).sort_values(["giorno", "ora", "categoria"]).reset_index(drop=True)

    # OD mensile per categoria
    tables["od_mese_categoria"] = _agg_core(base, ["mese", "categoria", "cod_partenza", "cod_arrivo"]).sort_values(
        ["mese", "categoria", "cod_partenza", "cod_arrivo"]
    ).reset_index(drop=True)

    # OD giornaliero per categoria
    tables["od_giorno_categoria"] = _agg_core(base, ["giorno", "categoria", "cod_partenza", "cod_arrivo"]).sort_values(
        ["giorno", "categoria", "cod_partenza", "cod_arrivo"]
    ).reset_index(drop=True)

    # Stazioni mensile per categoria (nodo = partenza + arrivo)
    dep = _agg_core(base, ["mese", "categoria", "cod_partenza"]).rename(columns={"cod_partenza": "cod_stazione"})
    arr = _agg_core(base, ["mese", "categoria", "cod_arrivo"]).rename(columns={"cod_arrivo": "cod_stazione"})
    st_month = pd.concat([dep, arr], ignore_index=True)
    st_month = st_month.groupby(["mese", "categoria", "cod_stazione"], as_index=False).sum(numeric_only=True)
    st_month["ruolo"] = "nodo"
    tables["stazioni_mese_categoria_nodo"] = st_month.sort_values(["mese", "categoria", "cod_stazione"]).reset_index(drop=True)

    # Stazioni giornaliero per categoria (nodo)
    depd = _agg_core(base, ["giorno", "categoria", "cod_partenza"]).rename(columns={"cod_partenza": "cod_stazione"})
    arrd = _agg_core(base, ["giorno", "categoria", "cod_arrivo"]).rename(columns={"cod_arrivo": "cod_stazione"})
    st_day = pd.concat([depd, arrd], ignore_index=True)
    st_day = st_day.groupby(["giorno", "categoria", "cod_stazione"], as_index=False).sum(numeric_only=True)
    st_day["ruolo"] = "nodo"
    tables["stazioni_giorno_categoria_nodo"] = st_day.sort_values(["giorno", "categoria", "cod_stazione"]).reset_index(drop=True)

    # Istogramma ritardo arrivo per mese e categoria
    base["bucket_ritardo_arrivo"] = _bucketize_delay(base["ritardo_arrivo_min"], buckets, labels)
    hist_month = (
        base.groupby(["mese", "categoria", "bucket_ritardo_arrivo"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["mese", "categoria", "bucket_ritardo_arrivo"])
        .reset_index(drop=True)
    )
    tables["hist_mese_categoria"] = hist_month

    # Istogramma giornaliero
    hist_day = (
        base.groupby(["giorno", "categoria", "bucket_ritardo_arrivo"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["giorno", "categoria", "bucket_ritardo_arrivo"])
        .reset_index(drop=True)
    )
    tables["hist_giorno_categoria"] = hist_day

    # Istogramma giornaliero per ora
    hist_day_hour = (
        base.groupby(["giorno", "ora", "categoria", "bucket_ritardo_arrivo"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["giorno", "ora", "categoria", "bucket_ritardo_arrivo"])
        .reset_index(drop=True)
    )
    tables["hist_giorno_ora_categoria"] = hist_day_hour

    return tables


def gold_keys() -> Dict[str, List[str]]:
    """
    Chiavi di deduplica per ogni tabella, usate nel salvataggio incrementale.
    """
    return {
        "kpi_mese_categoria": ["mese", "categoria"],
        "kpi_giorno_categoria": ["giorno", "categoria"],
        "kpi_giorno_ora_categoria": ["giorno", "ora", "categoria"],
        "od_mese_categoria": ["mese", "categoria", "cod_partenza", "cod_arrivo"],
        "od_giorno_categoria": ["giorno", "categoria", "cod_partenza", "cod_arrivo"],
        "stazioni_mese_categoria_nodo": ["mese", "categoria", "cod_stazione"],
        "stazioni_giorno_categoria_nodo": ["giorno", "categoria", "cod_stazione"],
        "hist_mese_categoria": ["mese", "categoria", "bucket_ritardo_arrivo"],
        "hist_giorno_categoria": ["giorno", "categoria", "bucket_ritardo_arrivo"],
        "hist_giorno_ora_categoria": ["giorno", "ora", "categoria", "bucket_ritardo_arrivo"],
    }


def save_gold_tables(tables: Dict[str, pd.DataFrame], out_dir: str) -> None:
    ensure_dir(out_dir)
    keys = gold_keys()

    for name, df in tables.items():
        out_path = os.path.join(out_dir, f"{name}.csv")

        if os.path.exists(out_path) and name in keys:
            old = pd.read_csv(out_path, dtype=str)
            new = df.copy()

            merged = pd.concat([old, new], ignore_index=True)

            for c in merged.columns:
                if c not in keys[name]:
                    merged[c] = pd.to_numeric(merged[c], errors="ignore")

            merged = merged.drop_duplicates(subset=keys[name], keep="last")

            sort_cols = keys[name]
            merged = merged.sort_values(sort_cols).reset_index(drop=True)
            merged.to_csv(out_path, index=False)
        else:
            df.to_csv(out_path, index=False)


def main() -> None:
    cfg = load_yaml("config/pipeline.yml")
    gold_cfg = cfg.get("gold", {})

    start = gold_cfg.get("start")
    end = gold_cfg.get("end")

    # fallback: usa il file silver più recente disponibile in data/silver
    if not start or not end:
        silver_dir = os.path.join("data", "silver")
        if not os.path.exists(silver_dir):
            raise ValueError("gold.start and gold.end missing and data/silver not found")
        cands = [p for p in os.listdir(silver_dir) if p.endswith(".parquet") and p.startswith("treni_")]
        if not cands:
            raise ValueError("no silver parquet found")
        cands.sort()
        last = cands[-1].replace(".parquet", "")
        _, start, end = last.split("_", 2)

    df = _read_silver(start, end)

    tables = build_gold_tables(df, cfg)
    out_dir = os.path.join("data", "gold")
    save_gold_tables(tables, out_dir)

    print({k: int(len(v)) for k, v in tables.items()})


if __name__ == "__main__":
    main()
