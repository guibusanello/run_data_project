"""
Camada Silver — Limpeza e normalização
Lê o Parquet do Bronze, achata o array 'features' e normaliza tipos.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import timezone

# ─── Configuração de caminhos ─────────────────────────────────────────────────

BRONZE_PATH = Path("data/bronze/runs.parquet")
SILVER_DIR  = Path("data/silver")
OUTPUT      = SILVER_DIR / "runs.parquet"

# ─── Funções auxiliares ───────────────────────────────────────────────────────

def parse_features(features_str: str) -> dict:
    """
    Recebe a coluna 'features' como JSON string e retorna um dict
    com os atributos de cada feature_type como chave.
    Ex: { "weather": {...}, "track_metrics": {...}, ... }
    """
    try:
        features = json.loads(features_str)
        return {f["type"]: f["attributes"] for f in features}
    except Exception:
        return {}

def ms_to_datetime(ms):
    """Converte timestamp em milissegundos para datetime UTC."""
    try:
        return pd.to_datetime(int(ms), unit="ms", utc=True)
    except Exception:
        return None

def ms_to_minutes(ms):
    """Converte duração em milissegundos para minutos."""
    try:
        return round(int(ms) / 60000, 2)
    except Exception:
        return None

# ─── Transformação Silver ─────────────────────────────────────────────────────

def transform_silver():
    df_bronze = pd.read_parquet(BRONZE_PATH)
    print(f"📥 Bronze carregado: {len(df_bronze)} linhas\n")

    records = []

    for _, row in df_bronze.iterrows():
        features = parse_features(row["features"])

        weather        = features.get("weather", {})
        track          = features.get("track_metrics", {})
        fastest        = features.get("fastest_segments", {})
        map_data       = features.get("map", {})

        # ── Segmentos mais rápidos ────────────────────────────────────────────
        segments = {
            s["distance"]: round(s["duration"] / 60000, 2)
            for s in fastest.get("segments", [])
        }

        record = {
            # Identificação
            "id":                   row["id"],
            "user_id":              row["user_id"],

            # Tempo
            "start_time":           ms_to_datetime(row["start_time"]),
            "end_time":             ms_to_datetime(row["end_time"]),
            "duration_min":         ms_to_minutes(row["duration"]),
            "pause_min":            ms_to_minutes(row["pause"]),

            # Desempenho
            "distance_m":           track.get("distance"),
            "distance_km":          round(int(track["distance"]) / 1000, 2) if track.get("distance") else None,
            "avg_speed_ms":         track.get("average_speed"),
            "avg_pace_min_km": round(float(track["average_pace"]) * 1000 / 60, 2) if track.get("average_pace") else None,
            "max_speed_ms":         track.get("max_speed"),
            "elevation_gain_m":     track.get("elevation_gain"),
            "elevation_loss_m":     track.get("elevation_loss"),
            "surface":              track.get("surface"),

            # Segmentos mais rápidos (em minutos)
            "fastest_1km_min":      segments.get("1km"),
            "fastest_1mi_min":      segments.get("1mi"),
            "fastest_5km_min":      segments.get("5km"),

            # Saúde e bem-estar
            "calories":             row.get("calories"),
            "dehydration_ml":       row.get("dehydration_volume"),
            "subjective_feeling":   row.get("subjective_feeling"),

            # Clima
            "weather_condition":    weather.get("conditions"),
            "temperature_c":        weather.get("temperature"),
            "humidity_pct":         weather.get("humidity"),
            "wind_speed_ms":        weather.get("wind_speed"),

            # Localização inicial
            "start_latitude":       map_data.get("start_latitude"),
            "start_longitude":      map_data.get("start_longitude"),

            # Metadados
            "plausible":            row.get("plausible"),
            "tracking_method":      row.get("tracking_method"),
            "_source_file":         row.get("_source_file"),
            "_ingested_at":         row.get("_ingested_at"),
        }

        records.append(record)

    df_silver = pd.DataFrame(records)

    # ── Corrigir tipos ────────────────────────────────────────────────────────
    float_cols = [
        "avg_speed_ms", "avg_pace_min_km", "max_speed_ms",
        "wind_speed_ms", "temperature_c", "humidity_pct",
        "start_latitude", "start_longitude"
    ]
    for col in float_cols:
        df_silver[col] = pd.to_numeric(df_silver[col], errors="coerce")

    int_cols = ["distance_m", "calories", "dehydration_ml",
                "elevation_gain_m", "elevation_loss_m"]
    for col in int_cols:
        df_silver[col] = pd.to_numeric(df_silver[col], errors="coerce").astype("Int64")

    df_silver.to_parquet(OUTPUT, index=False)

    print(f"🥈 Silver salvo em: {OUTPUT}")
    print(f"   Linhas:  {len(df_silver)}")
    print(f"   Colunas: {len(df_silver.columns)}")
    print(f"\nColunas disponíveis:")
    for col in df_silver.columns:
        print(f"  - {col} ({df_silver[col].dtype})")

if __name__ == "__main__":
    transform_silver()