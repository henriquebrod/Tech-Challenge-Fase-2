import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")

DATASET_ID = os.getenv(
    "BIGQUERY_DATASET_ID",
    "alfabetizacao_streaming"
)

TABLE_VALIDOS_ID = os.getenv(
    "BIGQUERY_TABLE_VALIDOS_ID",
    "eventos_validos_particionado"
)

MAX_BYTES_BILLED = int(
    os.getenv("BIGQUERY_MAX_BYTES_BILLED", "1073741824")
)

if not PROJECT_ID:
    raise ValueError(
        "Variável de ambiente GCP_PROJECT_ID não configurada."
    )

bigquery_client = bigquery.Client(
    project=PROJECT_ID
)

tabela_validos = (
    f"{PROJECT_ID}.{DATASET_ID}.{TABLE_VALIDOS_ID}"
)

query = f"""
    SELECT
        ano,
        CAST(id_municipio AS STRING) AS id_municipio,
        '2' AS serie,
        '3' AS rede,
        indicador_alfabetizacao AS taxa_alfabetizacao,
        event_id,
        produced_at AS event_ts,
        ingested_at AS ingest_ts
    FROM `{tabela_validos}`
"""

job_config = bigquery.QueryJobConfig(
    maximum_bytes_billed=MAX_BYTES_BILLED
)

df = bigquery_client.query(
    query,
    job_config=job_config
).to_dataframe()

df["id_municipio"] = (
    df["id_municipio"]
    .astype(str)
    .str.zfill(7)
)

df = df.drop_duplicates(
    subset=["event_id"],
    keep="last"
)

df = df[
    (df["taxa_alfabetizacao"] >= 0) &
    (df["taxa_alfabetizacao"] <= 100)
]

# Mesmo data_lake do pipeline batch, independente da pasta de onde o script é chamado
output_path = (
    Path(__file__).resolve().parent.parent
    / "data_lake" / "silver" / "streaming_eventos_validos.parquet"
)

output_path.parent.mkdir(
    parents=True,
    exist_ok=True
)

df.to_parquet(
    output_path,
    index=False
)

print(
    f"✅ Silver gerada com sucesso: {output_path}"
)

# Mesmo arquivo na Silver do BigQuery (alfabetizacao_silver.streaming_eventos_validos)
sys.path.append(str(Path(__file__).resolve().parent.parent))
from nuvem.lake import enviar_para_lake  # noqa: E402

enviar_para_lake([output_path])

print(
    f"📦 Total de eventos exportados: {len(df)}"
)
