
import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

# Caminho relativo à pasta principal do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_LOG = BASE_DIR / "data_lake" / "quality_reports" / "execution_log.csv"

CAMPOS = [
    "run_id",
    "etapa",
    "inicio",
    "fim",
    "duracao_segundos",
    "linhas_entrada",
    "linhas_saida",
    "bytes_lidos",
    "status",
    "erro",
]


def agora():
    return datetime.now(timezone.utc)


def registrar_execucao(
    etapa,
    inicio,
    linhas_entrada=None,
    linhas_saida=None,
    bytes_lidos=None,
    status="SUCESSO",
    erro="",
    run_id=None,
):
    fim = agora()
    duracao = (fim - inicio).total_seconds()

    registro = {
        "run_id": run_id or uuid4().hex,
        "etapa": etapa,
        "inicio": inicio.isoformat(),
        "fim": fim.isoformat(),
        "duracao_segundos": round(duracao, 3),
        "linhas_entrada": linhas_entrada,
        "linhas_saida": linhas_saida,
        "bytes_lidos": bytes_lidos,
        "status": status,
        "erro": str(erro),
    }

    ARQUIVO_LOG.parent.mkdir(parents=True, exist_ok=True)

    with open(ARQUIVO_LOG, "a", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=CAMPOS)

        if arquivo.tell() == 0:
            escritor.writeheader()

        escritor.writerow(registro)
        arquivo.flush()
        os.fsync(arquivo.fileno())

    print(f"[{status}] {etapa} | {duracao:.3f}s")
    return registro


from contextlib import contextmanager

ARQUIVO_CHECKS = ARQUIVO_LOG.parent / "gold_checks.csv"


def sincronizar_relatorios():
    # Log e checks vão para o BigQuery a cada etapa, inclusive na falha,
    # para o histórico ficar na nuvem mesmo se a execução parar
    try:
        from nuvem.lake import enviar_para_lake
        enviar_para_lake([ARQUIVO_LOG, ARQUIVO_CHECKS])
    except Exception as erro_envio:
        print(
            f"[AVISO] Não foi possível enviar os relatórios ao BigQuery: "
            f"{type(erro_envio).__name__}: {erro_envio}"
        )


@contextmanager
def monitorar_etapa(etapa, run_id=None, metricas=None):
    inicio = agora()

    try:
        yield
    except Exception as erro:
        descricao_erro = f"{type(erro).__name__}: {erro}"

        # Registra a falha no CSV
        registrar_execucao(
            etapa=etapa,
            inicio=inicio,
            status="FALHA",
            erro=descricao_erro,
            run_id=run_id
        )
        sincronizar_relatorios()

        # Envia alerta por e-mail
        try:
            from monitoramento.alertas import enviar_alerta

            enviar_alerta(
                etapa=etapa,
                erro=descricao_erro,
                run_id=run_id
            )
        except Exception as erro_alerta:
            print(
                f"[AVISO] Não foi possível enviar o alerta: "
                f"{type(erro_alerta).__name__}: {erro_alerta}"
            )

        # Mantém a interrupção original do pipeline
        raise
    else:
        registrar_execucao(
            etapa=etapa,
            inicio=inicio,
            linhas_entrada=metricas.get("linhas_entrada") if metricas else None,
            linhas_saida=metricas.get("linhas_saida") if metricas else None,
            bytes_lidos=metricas.get("bytes_lidos") if metricas else None,
            status="SUCESSO",
            run_id=run_id
        )
        sincronizar_relatorios()
