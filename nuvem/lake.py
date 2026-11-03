"""
Data lake no BigQuery.

O pipeline grava cada camada em data_lake/ e este módulo carrega os arquivos no
BigQuery com load jobs (sem custo, funcionam no BigQuery Sandbox):

    data_lake/bronze/<entidade>_raw_<ts>.parquet -> <prefixo>_bronze.<entidade>_raw   (APPEND, partição por ingestão)
    data_lake/silver/<entidade>_silver.parquet   -> <prefixo>_silver.<entidade>       (substitui)
    data_lake/gold/<tabela>.parquet|.csv         -> <prefixo>_gold.<tabela>           (substitui)
    data_lake/quality_reports/<relatorio>.csv    -> <prefixo>_monitoramento.<relatorio> (substitui pelo CSV acumulado)

A Bronze acumula cargas (WRITE_APPEND): uma execução nunca apaga a anterior.
Sem LAKE_BIGQUERY=true no .env, nada é enviado (execução só local, usada nos testes).
"""
import csv
import os
import re
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_LAKE = BASE_DIR / "data_lake"
load_dotenv(BASE_DIR / ".env")

DATASET_POR_CAMADA = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "quality_reports": "monitoramento",
}

_cliente = None


def lake_configurado():
    return os.getenv("LAKE_BIGQUERY", "false").lower() == "true"


def _client():
    global _cliente
    if _cliente is None:
        from google.cloud import bigquery
        _cliente = bigquery.Client(project=os.getenv("GCP_PROJECT_ID") or None)
    return _cliente


def destino(caminho):
    """(dataset, tabela, append) de um arquivo do data_lake."""
    relativo = Path(caminho).resolve().relative_to(DATA_LAKE)
    camada = relativo.parts[0]
    nome = Path(relativo.name).stem
    if camada == "bronze":
        nome = re.sub(r"_\d{8}_\d{6}$", "", nome)   # municipio_raw_20261101_120000 -> municipio_raw
    elif camada == "silver":
        nome = re.sub(r"_silver$", "", nome)
    prefixo = os.getenv("BQ_DATASET_PREFIXO", "alfabetizacao")
    return f"{prefixo}_{DATASET_POR_CAMADA[camada]}", nome, camada == "bronze"


def _config_carga(caminho, append):
    from google.cloud import bigquery
    modo = bigquery.WriteDisposition.WRITE_APPEND if append else bigquery.WriteDisposition.WRITE_TRUNCATE
    if caminho.suffix == ".parquet":
        config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, write_disposition=modo)
        if append:
            # --- Bronze: partição por data de ingestão (consulta por carga lê só a partição)
            config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
        return config
    # --- CSV de logs e checks: colunas com tipos misturados (valor, detalhe), tudo como texto
    with open(caminho, encoding="utf-8", newline="") as arquivo:
        colunas = next(csv.reader(arquivo))
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=modo,
        schema=[bigquery.SchemaField(c, "STRING") for c in colunas],
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )


def enviar_para_lake(caminhos):
    """Carrega arquivos do data_lake no BigQuery. Retorna o total de linhas carregadas."""
    if not lake_configurado():
        return 0
    from google.cloud import bigquery
    client = _client()
    local = os.getenv("BQ_LOCATION", "US")
    total = 0
    for caminho in caminhos:
        caminho = Path(caminho)
        if not caminho.is_file():
            continue
        dataset, tabela, append = destino(caminho)
        novo = bigquery.Dataset(f"{client.project}.{dataset}")
        novo.location = local
        client.create_dataset(novo, exists_ok=True)

        tabela_id = f"{client.project}.{dataset}.{tabela}"
        with open(caminho, "rb") as arquivo:
            job = client.load_table_from_file(arquivo, tabela_id, job_config=_config_carga(caminho, append),
                                              location=local)
        job.result()
        total += job.output_rows or 0
        print(f"   [BigQuery] {tabela_id}: {'+' if append else ''}{job.output_rows} linhas "
              f"({'APPEND' if append else 'substitui'}, total {client.get_table(tabela_id).num_rows}, job {job.job_id})")
    return total


def publicar_relatorios():
    """Log de execução, checks e anomalias (histórico acumulado nos CSVs) no BigQuery."""
    pasta = DATA_LAKE / "quality_reports"
    return enviar_para_lake([pasta / "execution_log.csv", pasta / "gold_checks.csv", pasta / "gold_anomalias.csv"])
