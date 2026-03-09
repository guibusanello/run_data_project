"""
Camada Bronze — Ingestão dos JSONs brutos
Lê todos os arquivos JSON de data/raw/ e consolida em um único Parquet.
Nenhum campo é transformado ou removido.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ─── Configuração de caminhos ─────────────────────────────────────────────────

RAW_DIR    = Path("data/raw")
BRONZE_DIR = Path("data/bronze")
OUTPUT     = BRONZE_DIR / "runs.parquet"

# ─── Ingestão ─────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    """Lê um arquivo JSON e retorna como dicionário."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ingest_bronze():
    json_files = sorted(RAW_DIR.glob("*.json"))

    if not json_files:
        print("⚠️  Nenhum arquivo JSON encontrado em data/raw/")
        return

    print(f"📂 {len(json_files)} arquivos encontrados em data/raw/\n")

    records = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for path in json_files:
        data = load_json(path)

        # Adiciona metadados de controle — únicos campos que inserimos no Bronze
        data["_source_file"] = path.name
        data["_ingested_at"] = ingested_at

        records.append(data)
        print(f"  ✅ {path.name}")

    # Converte para DataFrame e salva como Parquet
    df = pd.DataFrame(records)
    df.to_parquet(OUTPUT, index=False)

    print(f"\n🥉 Bronze salvo em: {OUTPUT}")
    print(f"   Linhas:  {len(df)}")
    print(f"   Colunas: {len(df.columns)}")
    print(f"\nColunas disponíveis:")
    for col in df.columns:
        print(f"  - {col}")

if __name__ == "__main__":
    ingest_bronze()