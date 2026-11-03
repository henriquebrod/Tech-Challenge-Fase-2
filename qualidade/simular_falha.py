"""
Falha controlada de uma regra bloqueante de qualidade.

Lê a Silver de município, monta em memória uma cópia com uma linha repetida e roda a
mesma regra de chave única usada pelo pipeline, dentro de monitorar_etapa. Esperado:

- a regra fica REPROVADO em data_lake/quality_reports/gold_checks.csv;
- a etapa fica FALHA em data_lake/quality_reports/execution_log.csv;
- o alerta por e-mail é enviado (GMAIL_* no .env);
- o processo termina com código de saída 1.

O arquivo da Silver não é alterado.

Uso, na raiz do projeto:
    python -m qualidade.simular_falha
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from monitoramento.registrador import monitorar_etapa
from qualidade.verificador import checar_chave_unica, configurar_verificador

BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_SILVER = BASE_DIR / "data_lake" / "silver" / "municipio_silver.parquet"
CHAVES = ['ano', 'id_municipio', 'serie', 'rede']


def main():
    # --- ponteiro do Git LFS tem ~130 bytes: sem o dado real não há o que testar
    if not ARQUIVO_SILVER.exists() or ARQUIVO_SILVER.stat().st_size < 1024:
        sys.exit(f"Silver de município indisponível em {ARQUIVO_SILVER}. "
                 "Rode o notebook (ou `git lfs pull`) antes da falha controlada.")

    run_id = "FALHA_CONTROLADA_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    configurar_verificador(run_id=run_id)

    df = pd.read_parquet(ARQUIVO_SILVER, columns=CHAVES)
    corrompido = pd.concat([df, df.head(1)], ignore_index=True)
    print(f"Run {run_id}: {len(df)} linhas lidas, 1 linha repetida inserida em memória.")

    with monitorar_etapa("falha_controlada_silver_municipio", run_id=run_id):
        checar_chave_unica(corrompido, CHAVES, 'municipio_silver (cópia com linha repetida)')
        print("Esta mensagem NAO deve aparecer: a regra bloqueante deixou o dado passar.")


if __name__ == "__main__":
    main()
