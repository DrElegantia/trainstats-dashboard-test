from __future__ import annotations

import io
import os
from typing import Dict, Any, Optional

import pandas as pd

from .utils import ensure_dir, http_get_with_retry, load_yaml


ISTAT_COMUNI_URL = "https://www.istat.it/storage/codici-unita-amministrative/Elenco-comuni-italiani.csv"

CAPOLUOGHI_DEFAULT = [
    "Agrigento","Alessandria","Ancona","Aosta","Arezzo","Ascoli Piceno","Asti","Avellino",
    "Bari","Barletta","Belluno","Benevento","Bergamo","Biella","Bologna","Bolzano","Brescia","Brindisi",
    "Cagliari","Caltanissetta","Campobasso","Carbonia","Caserta","Catania","Catanzaro","Chieti","Como","Cosenza",
    "Cremona","Crotone","Cuneo",
    "Enna",
    "Fermo","Ferrara","Firenze","Foggia","Forlì","Frosinone",
    "Genova","Gorizia","Grosseto",
    "Imperia","Isernia",
    "L'Aquila","La Spezia","Latina","Lecce","Lecco","Livorno","Lodi","Lucca",
    "Macerata","Mantova","Massa","Matera","Messina","Milano","Modena","Monza",
    "Napoli","Novara","Nuoro",
    "Oristano",
    "Padova","Palermo","Parma","Pavia","Perugia","Pesaro","Pescara","Piacenza","Pisa","Pistoia","Pordenone","Potenza","Prato",
    "Ragusa","Ravenna","Reggio Calabria","Reggio Emilia","Rieti","Rimini","Roma","Rovigo",
    "Salerno","Sassari","Savona","Siena","Siracusa","Sondrio",
    "Taranto","Teramo","Terni","Torino","Trapani","Trento","Treviso","Trieste",
    "Udine",
    "Varese","Venezia","Verbania","Vercelli","Verona","Vibo Valentia","Vicenza","Viterbo"
]


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


def _pick_col(cols: list[str], must_contain: str) -> Optional[str]:
    needle = must_contain.lower()
    for c in cols:
        if needle in c.lower():
            return c
    return None


def _find_city_col(cols: list[str]) -> Optional[str]:
    for c in cols:
        cl = c.lower()
        if "denominazione" in cl and "ital" in cl:
            return c
    return _pick_col(cols, "denominazione")


def _find_cap_flag_col(cols: list[str]) -> Optional[str]:
    for c in cols:
        cl = c.lower()
        if "capoluogo" in cl and "prov" in cl:
            return c
    return _pick_col(cols, "capoluogo")


def _parse_istat_csv(raw_text: str) -> pd.DataFrame:
    df = pd.read_csv(io.StringIO(raw_text), sep=None, engine="python", dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _build_default_capoluoghi() -> pd.DataFrame:
    out = pd.DataFrame({"citta": sorted(set([c.strip() for c in CAPOLUOGHI_DEFAULT if str(c).strip()]))})
    out = out.reset_index(drop=True)
    return out


def _try_refresh_from_istat(cfg: Dict[str, Any]) -> Optional[pd.DataFrame]:
    net = cfg.get("network", {})
    timeout = int(net.get("timeout_seconds", 20))
    max_retries = int(net.get("max_retries", 3))
    backoff = int(net.get("backoff_factor", 2))

    r = http_get_with_retry(
        ISTAT_COMUNI_URL,
        timeout=timeout,
        max_retries=max_retries,
        backoff_factor=backoff,
    )
    raw = r.content.decode("utf8", errors="replace")
    if "<html" in raw[:500].lower():
        raise RuntimeError("ISTAT download returned HTML, not CSV")

    df = _parse_istat_csv(raw)
    cols = list(df.columns)

    col_city = _find_city_col(cols)
    col_flag = _find_cap_flag_col(cols)
    if not col_city or not col_flag:
        raise RuntimeError(f"ISTAT columns not found. columns={cols}")

    flag = df[col_flag].astype(str).str.strip().str.upper()
    is_cap = flag.isin(["1", "SI", "SÌ", "TRUE", "Y", "YES"])

    out = df.loc[is_cap, [col_city]].rename(columns={col_city: "citta"})
    out["citta"] = out["citta"].astype(str).str.strip()
    out = out[out["citta"] != ""].drop_duplicates(subset=["citta"]).sort_values("citta").reset_index(drop=True)

    if len(out) < 50:
        raise RuntimeError(f"Too few capoluoghi parsed from ISTAT: {len(out)}")

    return out


def build_capoluoghi_provincia_csv(cfg: Dict[str, Any]) -> pd.DataFrame:
    local_path = os.path.join("data", "stations", "capoluoghi_provincia.csv")

    refresh = os.environ.get("REFRESH_CAPOLUOGHI_ISTAT", "").strip() == "1"

    if os.path.exists(local_path) and not refresh:
        df = pd.read_csv(local_path, dtype=str)
        if "citta" not in df.columns:
            for alt in ("capoluogo", "nome", "comune", "city"):
                if alt in df.columns:
                    df = df.rename(columns={alt: "citta"})
                    break
        if "citta" not in df.columns:
            raise ValueError(f"{local_path} must contain a 'citta' column")
        df["citta"] = df["citta"].astype(str).str.strip()
        df = df[df["citta"] != ""].drop_duplicates(subset=["citta"]).sort_values("citta").reset_index(drop=True)
        return df[["citta"]]

    out = None

    if refresh:
        try:
            out = _try_refresh_from_istat(cfg)
            print({"capoluoghi_source": "istat_refresh", "capoluoghi_rows": int(len(out))})
        except Exception as e:
            print({"capoluoghi_source": "istat_refresh_failed_using_default", "warning": repr(e)})
            out = None

    if out is None:
        out = _build_default_capoluoghi()
        print({"capoluoghi_source": "embedded_default", "capoluoghi_rows": int(len(out))})

    ensure_dir(os.path.dirname(local_path))
    out.to_csv(local_path, index=False)

    return out


def main() -> None:
    cfg: Dict[str, Any] = load_yaml("config/pipeline.yml")

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
    if {"lat", "lon"}.issubset(joined.columns):
        missing_mask = joined["lat"].isna() | joined["lon"].isna()
    else:
        missing_mask = pd.Series(True, index=joined.index)
    missing = joined.loc[missing_mask, missing_cols].drop_duplicates()

    missing_path = os.path.join("data", "stations", "stations_unknown.csv")
    ensure_dir(os.path.dirname(missing_path))
    missing.to_csv(missing_path, index=False)

    cap = build_capoluoghi_provincia_csv(cfg)
    cap.to_csv(os.path.join(out_dir, "capoluoghi_provincia.csv"), index=False)

    print(
        {
            "stations_dim_rows": int(len(joined)),
            "stations_missing_coords": int(len(missing)),
            "capoluoghi_rows": int(len(cap)),
        }
    )


if __name__ == "__main__":
    main()
