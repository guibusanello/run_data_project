"""
Camada Gold — Agregações e métricas analíticas
Lê o Parquet do Silver e gera tabelas prontas para análise.
"""

import pandas as pd
from pathlib import Path

# ─── Configuração de caminhos ─────────────────────────────────────────────────

SILVER_PATH = Path("data/silver/runs.parquet")
GOLD_DIR    = Path("data/gold")

# ─── Carregamento ─────────────────────────────────────────────────────────────

def load_silver() -> pd.DataFrame:
    df = pd.read_parquet(SILVER_PATH)

    # Garante que start_time é datetime e extrai campos de período
    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
    df["year"]       = df["start_time"].dt.year
    df["month"]      = df["start_time"].dt.month
    df["week"]       = df["start_time"].dt.isocalendar().week.astype(int)
    df["year_month"] = df["start_time"].dt.to_period("M").astype(str)
    df["year_week"]  = df["start_time"].dt.to_period("W").astype(str)

    return df

# ─── Tabelas Gold ─────────────────────────────────────────────────────────────

def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Volume mensal de corridas."""
    return (
        df.groupby("year_month")
        .agg(
            total_runs       = ("id", "count"),
            total_km         = ("distance_km", "sum"),
            avg_pace_min_km  = ("avg_pace_min_km", "mean"),
            total_calories   = ("calories", "sum"),
            total_duration_h = ("duration_min", lambda x: round(x.sum() / 60, 2)),
            avg_distance_km  = ("distance_km", "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("year_month")
    )

def weekly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Evolução semanal de pace e distância."""
    return (
        df.groupby("year_week")
        .agg(
            total_runs      = ("id", "count"),
            total_km        = ("distance_km", "sum"),
            avg_pace_min_km = ("avg_pace_min_km", "mean"),
            avg_distance_km = ("distance_km", "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("year_week")
    )

def yearly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Visão anual consolidada."""
    return (
        df.groupby("year")
        .agg(
            total_runs       = ("id", "count"),
            total_km         = ("distance_km", "sum"),
            avg_pace_min_km  = ("avg_pace_min_km", "mean"),
            total_calories   = ("calories", "sum"),
            total_duration_h = ("duration_min", lambda x: round(x.sum() / 60, 2)),
            avg_distance_km  = ("distance_km", "mean"),
            longest_run_km   = ("distance_km", "max"),
        )
        .round(2)
        .reset_index()
        .sort_values("year")
    )

def personal_bests(df: pd.DataFrame) -> pd.DataFrame:
    """Melhores tempos nos segmentos 1km, 1mi e 5km."""
    segments = {
        "1km":  "fastest_1km_min",
        "1mi":  "fastest_1mi_min",
        "5km":  "fastest_5km_min",
    }

    records = []
    for distance, col in segments.items():
        valid = df[df[col].notna()].copy()
        if valid.empty:
            continue
        best = valid.loc[valid[col].idxmin()]
        records.append({
            "distance":     distance,
            "best_time_min": round(best[col], 2),
            "date":         best["start_time"].date(),
            "pace_min_km":  round(best["avg_pace_min_km"], 2) if pd.notna(best["avg_pace_min_km"]) else None,
            "distance_km":  best["distance_km"],
        })

    return pd.DataFrame(records)

# ─── Execução ─────────────────────────────────────────────────────────────────

def build_gold():
    df = load_silver()
    print(f"📥 Silver carregado: {len(df)} linhas\n")

    tables = {
        "monthly_summary": monthly_summary(df),
        "weekly_summary":  weekly_summary(df),
        "yearly_summary":  yearly_summary(df),
        "personal_bests":  personal_bests(df),
    }

    for name, table in tables.items():
        path = GOLD_DIR / f"{name}.parquet"
        table.to_parquet(path, index=False)
        print(f"🥇 {name}.parquet salvo → {len(table)} linhas")
        print(table.to_string(index=False))
        print()

    print("✅ Camada Gold concluída!")

if __name__ == "__main__":
    build_gold()