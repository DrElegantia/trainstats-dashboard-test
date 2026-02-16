# scripts/build_station_dim.py
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Set, Optional, Tuple

import pandas as pd

try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def collect_station_codes() -> Set[str]:
    """Raccoglie tutti i codici stazione unici dai file silver."""
    codes: Set[str] = set()
    silver_root = Path("data") / "silver"
    if not silver_root.exists():
        return codes
    for parquet_file in silver_root.rglob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            if "cod_partenza" in df.columns:
                codes.update(df["cod_partenza"].dropna().astype(str).unique())
            if "cod_arrivo" in df.columns:
                codes.update(df["cod_arrivo"].dropna().astype(str).unique())
        except Exception as e:
            print(f"Warning: Could not read {parquet_file}: {e}")
    return codes


def collect_station_names() -> Dict[str, str]:
    """Raccoglie i nomi delle stazioni dai file silver."""
    names: Dict[str, str] = {}
    silver_root = Path("data") / "silver"
    if not silver_root.exists():
        return names
    for parquet_file in silver_root.rglob("*.parquet"):
        try:
            df = pd.read_parquet(parquet_file)
            if "cod_partenza" in df.columns and "nome_partenza" in df.columns:
                part_df = df[["cod_partenza", "nome_partenza"]].dropna()
                for _, row in part_df.iterrows():
                    code = str(row["cod_partenza"])
                    name = str(row["nome_partenza"])
                    if code and name and code not in names:
                        names[code] = name
            if "cod_arrivo" in df.columns and "nome_arrivo" in df.columns:
                arr_df = df[["cod_arrivo", "nome_arrivo"]].dropna()
                for _, row in arr_df.iterrows():
                    code = str(row["cod_arrivo"])
                    name = str(row["nome_arrivo"])
                    if code and name and code not in names:
                        names[code] = name
        except Exception as e:
            print(f"Warning: Could not read {parquet_file}: {e}")
    return names


def load_cached_coords() -> Dict[str, Tuple[float, float]]:
    """Carica coordinate già geocodificate da stations/stations.csv."""
    cache: Dict[str, Tuple[float, float]] = {}
    cache_path = Path("stations") / "stations.csv"
    if not cache_path.exists():
        return cache
    try:
        df = pd.read_csv(cache_path)
        for _, row in df.iterrows():
            code = str(row.get("cod_stazione", "")).strip()
            lat = row.get("lat")
            lon = row.get("lon")
            if code and pd.notna(lat) and pd.notna(lon):
                cache[code] = (float(lat), float(lon))
    except Exception as e:
        print(f"Warning: Could not read cache {cache_path}: {e}")
    return cache


def save_coords_cache(records: list) -> None:
    """Salva la cache delle coordinate in stations/stations.csv."""
    cache_dir = Path("stations")
    ensure_dir(str(cache_dir))
    cache_path = cache_dir / "stations.csv"
    df = pd.DataFrame(records)
    df.to_csv(cache_path, index=False, encoding="utf-8")
    print(f"Coords cache saved: {cache_path} ({len(df)} stations)")


def clean_station_name(name: str) -> str:
    """Pulisce il nome stazione per migliorare il geocoding."""
    # Rimuovi suffissi tipici di RFI
    import re
    name = name.strip()
    # Rimuovi cose come "(S05)" o codici tra parentesi
    name = re.sub(r'\(S\d+\)', '', name)
    # Sostituisci abbreviazioni comuni
    name = name.replace("S.", "San ").replace("S ", "San ")
    name = name.replace("P.", "Porta ").replace("P ", "Porta ")
    return name.strip()


def geocode_station(geolocator, name: str) -> Optional[Tuple[float, float]]:
    """Geocodifica un nome stazione aggiungendo ', Italy' per contesto."""
    if not name:
        return None

    cleaned = clean_station_name(name)
    # Prova prima con "stazione di <nome>, Italy"
    queries = [
        f"stazione {cleaned}, Italia",
        f"{cleaned}, Italia",
        f"{cleaned}, Italy",
    ]

    for query in queries:
        try:
            location = geolocator.geocode(query, timeout=10)
            if location:
                # Verifica che sia in Italia (lat 35-47, lon 6-19)
                if 35 <= location.latitude <= 48 and 6 <= location.longitude <= 19:
                    return (round(location.latitude, 6), round(location.longitude, 6))
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  Geocoding timeout/error for '{query}': {e}")
            time.sleep(2)
        except Exception as e:
            print(f"  Geocoding error for '{query}': {e}")
        time.sleep(1.1)  # Nominatim rate limit: max 1 req/sec

    return None


def build_station_dim() -> pd.DataFrame:
    """Costruisce la tabella dimensionale delle stazioni con coordinate."""
    codes = collect_station_codes()
    names = collect_station_names()

    if not codes:
        return pd.DataFrame(columns=["cod_stazione", "nome_stazione", "lat", "lon"])

    # Carica coordinate già note dalla cache
    cached_coords = load_cached_coords()
    print(f"Loaded {len(cached_coords)} cached coordinates")

    # Prepara geocoder se serve
    codes_needing_geocoding = [c for c in codes if c not in cached_coords]
    geolocator = None
    if codes_needing_geocoding and HAS_GEOPY:
        print(f"Need to geocode {len(codes_needing_geocoding)} stations...")
        geolocator = Nominatim(user_agent="trainstats-dashboard/1.0")
    elif codes_needing_geocoding and not HAS_GEOPY:
        print("WARNING: geopy not installed, cannot geocode new stations")
        print("  Install with: pip install geopy")

    # Costruisci records con coordinate
    records = []
    geocoded_count = 0
    failed_count = 0

    for code in sorted(codes):
        name = names.get(code, "")
        lat = None
        lon = None

        # Prima: prova dalla cache
        if code in cached_coords:
            lat, lon = cached_coords[code]
        # Seconda: geocodifica
        elif geolocator and name:
            print(f"  Geocoding: {code} -> {name}...", end=" ")
            coords = geocode_station(geolocator, name)
            if coords:
                lat, lon = coords
                geocoded_count += 1
                print(f"OK ({lat}, {lon})")
            else:
                failed_count += 1
                print("FAILED")

        records.append({
            "cod_stazione": code,
            "nome_stazione": name,
            "lat": lat,
            "lon": lon,
        })

    print(f"\nGeocode summary: {geocoded_count} new, {failed_count} failed, "
          f"{len(cached_coords)} from cache")

    # Aggiorna la cache con tutti i risultati (inclusi i nuovi)
    cache_records = [r for r in records if r["lat"] is not None]
    if cache_records:
        save_coords_cache(cache_records)

    return pd.DataFrame(records)


def save_station_dim(df: pd.DataFrame) -> None:
    """Salva la dimensione delle stazioni in data/gold/."""
    gold_dir = Path("data") / "gold"
    ensure_dir(str(gold_dir))
    output_path = gold_dir / "stations_dim.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")

    coords_count = df["lat"].notna().sum()
    print(f"Station dimension saved: {output_path}")
    print(f"Total stations: {len(df)}, with coordinates: {coords_count}")


def main() -> None:
    df = build_station_dim()
    if df.empty:
        print("Warning: No station data found in silver files")
        return
    save_station_dim(df)


if __name__ == "__main__":
    main()
