import json
import uuid
import os
import sys
import threading
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

from google.cloud import pubsub_v1
from google.cloud import bigquery
load_dotenv()

# Raiz do projeto no path para reaproveitar o alerta do monitoramento
sys.path.append(str(Path(__file__).resolve().parent.parent))


PROJECT_ID = os.getenv("GCP_PROJECT_ID")
SUBSCRIPTION_ID = os.getenv(
    "PUBSUB_SUBSCRIPTION_ID",
    "alfabetizacao-eventos-sub"
)

DATASET_ID = os.getenv(
    "BIGQUERY_DATASET_ID",
    "alfabetizacao_streaming"
)

TABLE_VALIDOS_ID = os.getenv(
    "BIGQUERY_TABLE_VALIDOS_ID",
    "eventos_validos_particionado"
)

TABLE_INVALIDOS_ID = os.getenv(
    "BIGQUERY_TABLE_INVALIDOS_ID",
    "eventos_invalidos"
)

TABLE_METRICAS_ID = os.getenv(
    "BIGQUERY_TABLE_METRICAS_ID",
    "metricas_streaming"
)

if not PROJECT_ID:
    raise ValueError(
        "Variável de ambiente GCP_PROJECT_ID não configurada."
    )

bigquery_client = bigquery.Client(project=PROJECT_ID)

tabela_validos = (
    f"{PROJECT_ID}.{DATASET_ID}.{TABLE_VALIDOS_ID}"
)

tabela_invalidos = (
    f"{PROJECT_ID}.{DATASET_ID}.{TABLE_INVALIDOS_ID}"
)

tabela_metricas = (
    f"{PROJECT_ID}.{DATASET_ID}.{TABLE_METRICAS_ID}"
)

lote_eventos = []
contador_invalidos = 0
TAMANHO_LOTE = int(
    os.getenv("STREAMING_BATCH_SIZE", "10")
)
BATCH_MAX_WAIT_SECONDS = float(
    os.getenv("STREAMING_BATCH_MAX_WAIT_SECONDS", "10")
)
DEDUP_MAX_BYTES_BILLED = int(
    os.getenv("BIGQUERY_DEDUP_MAX_BYTES_BILLED", "1073741824")
)
if TAMANHO_LOTE < 1:
    raise ValueError("STREAMING_BATCH_SIZE deve ser maior que zero.")
if BATCH_MAX_WAIT_SECONDS <= 0:
    raise ValueError("STREAMING_BATCH_MAX_WAIT_SECONDS deve ser maior que zero.")
if DEDUP_MAX_BYTES_BILLED < 1:
    raise ValueError("BIGQUERY_DEDUP_MAX_BYTES_BILLED deve ser maior que zero.")

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_ID
)

def alertar_falha(etapa, erro, execution_id=None):
    # O alerta não pode derrubar o consumidor: a falha já ficou em metricas_streaming
    try:
        from monitoramento.alertas import enviar_alerta

        enviar_alerta(etapa=etapa, erro=str(erro), run_id=execution_id)
    except Exception as erro_alerta:
        print(f"[AVISO] Não foi possível enviar o alerta: {type(erro_alerta).__name__}: {erro_alerta}")


eventos_processados = set()
lote_lock = threading.RLock()
flush_lock = threading.Lock()
lote_timer = None
mensagens_pendentes = {}


def buscar_eventos_ja_persistidos(event_ids):
    if not event_ids:
        return set()

    query = f"""
        SELECT DISTINCT event_id
        FROM `{tabela_validos}`
        WHERE event_id IN UNNEST(@event_ids)
          AND ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
    """

    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=DEDUP_MAX_BYTES_BILLED,
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "event_ids",
                "STRING",
                event_ids
            )
        ]
    )

    resultado = bigquery_client.query(
        query,
        job_config=job_config
    ).result()

    return {linha.event_id for linha in resultado}

def evento_invalido_ja_registrado(event_id):
    if not event_id:
        return False

    query = f"""
        SELECT 1
        FROM `{tabela_invalidos}`
        WHERE event_id = @event_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=DEDUP_MAX_BYTES_BILLED,
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "event_id",
                "STRING",
                event_id
            )
        ]
    )

    resultado = bigquery_client.query(
        query,
        job_config=job_config
    ).result()

    return next(iter(resultado), None) is not None


def validar_evento(evento):
    campos_obrigatorios = [
        "event_id",
        "produced_at",
        "ano",
        "sigla_uf",
        "id_municipio",
        "indicador_alfabetizacao",
        "origem"
    ]

    for campo in campos_obrigatorios:
        if campo not in evento:
            return False, f"Campo obrigatório ausente: {campo}"

    indicador = evento["indicador_alfabetizacao"]

    if not isinstance(indicador, (int, float)):
        return False, "Indicador de alfabetização deve ser numérico"

    if indicador < 0 or indicador > 100:
        return False, "Indicador de alfabetização deve estar entre 0 e 100"

    if evento["origem"] != "simulado":
        return False, "Origem do evento deve ser 'simulado'"

    return True, None

def registrar_evento_invalido(evento, ingested_at, motivo, message):
    global contador_invalidos

    event_id = evento.get("event_id")

    registro_invalido = {
        "event_id": evento.get("event_id"),
        "produced_at": evento.get("produced_at"),
        "ingested_at": ingested_at.isoformat(),
        "ano": evento.get("ano"),
        "sigla_uf": evento.get("sigla_uf"),
        "id_municipio": evento.get("id_municipio"),
        "indicador_alfabetizacao": evento.get("indicador_alfabetizacao"),
        "origem": evento.get("origem"),
        "motivo_erro": motivo
    }

    try:
        if evento_invalido_ja_registrado(event_id):
            print("⚠️ Evento inválido duplicado ignorado:")
            print(f"event_id: {event_id}")
            print("-" * 50)
            message.ack()
            return

        df_invalido = pd.DataFrame([registro_invalido])

        job = bigquery_client.load_table_from_dataframe(
            df_invalido,
            tabela_invalidos
        )

        job.result()

        contador_invalidos += 1

        print("❌ Evento inválido:")
        print(evento)
        print(f"Motivo: {motivo}")
        print("☁️ Evento inválido gravado no BigQuery.")
        print("-" * 50)

        message.ack()

    except Exception as erro:
        print("❌ Erro ao gravar evento inválido no BigQuery:")
        print(erro)
        alertar_falha("streaming_gravar_invalido", erro, event_id)
        message.nack()

def registrar_metrica(
    execution_id,
    started_at,
    finished_at,
    input_count,
    output_count,
    invalid_count,
    status,
    error_message=None
):
    duration_seconds = (
        finished_at - started_at
    ).total_seconds()

    registro_metrica = {
        "execution_id": execution_id,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": duration_seconds,
        "input_count": input_count,
        "output_count": output_count,
        "invalid_count": invalid_count,
        "status": status,
        "error_message": error_message
    }

    try:
        df_metrica = pd.DataFrame([registro_metrica])

        job = bigquery_client.load_table_from_dataframe(
            df_metrica,
            tabela_metricas
        )

        job.result()

        print("📊 Métrica registrada no BigQuery.")

    except Exception as erro:
        print("❌ Erro ao registrar métrica:")
        print(erro)
        alertar_falha("streaming_registrar_metrica", erro, execution_id)


def enviar_lote_bigquery():
    global lote_timer, contador_invalidos

    if not flush_lock.acquire(blocking=False):
        return

    try:
        with lote_lock:
            if not lote_eventos:
                return

            if lote_timer is not None:
                lote_timer.cancel()
                lote_timer = None

            eventos_do_lote = list(lote_eventos)
            lote_eventos.clear()
            ids_do_lote = [evento["event_id"] for evento in eventos_do_lote]
            invalidos_do_lote = contador_invalidos
            contador_invalidos = 0

        execution_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)

        ids_existentes = set()
        consulta_concluida = False
        ids_novos = set(ids_do_lote)

        try:
            ids_existentes = buscar_eventos_ja_persistidos(ids_do_lote)
            consulta_concluida = True
            ids_novos = set(ids_do_lote) - ids_existentes
            eventos_novos = [
                evento for evento in eventos_do_lote
                if evento["event_id"] in ids_novos
            ]

            if eventos_novos:
                df = pd.DataFrame(eventos_novos)
                job = bigquery_client.load_table_from_dataframe(df, tabela_validos)
                job.result()

            finished_at = datetime.now(timezone.utc)
            registrar_metrica(
                execution_id=execution_id,
                started_at=started_at,
                finished_at=finished_at,
                input_count=len(eventos_do_lote),
                output_count=len(eventos_novos),
                invalid_count=invalidos_do_lote,
                status="SUCESSO"
            )

            with lote_lock:
                mensagens_do_lote = []
                for event_id in ids_do_lote:
                    eventos_processados.add(event_id)
                    mensagens_do_lote.extend(mensagens_pendentes.pop(event_id, []))

            if eventos_novos:
                print(f"☁️ Lote de {len(eventos_novos)} eventos novos enviado ao BigQuery.")
            if ids_existentes:
                print(f"⚠️ {len(ids_existentes)} eventos já gravados foram ignorados.")
            for mensagem in mensagens_do_lote:
                mensagem.ack()
            print("✅ Mensagens confirmadas no Pub/Sub.")

        except Exception as erro:
            finished_at = datetime.now(timezone.utc)
            registrar_metrica(
                execution_id=execution_id,
                started_at=started_at,
                finished_at=finished_at,
                input_count=len(eventos_do_lote),
                output_count=0,
                invalid_count=invalidos_do_lote,
                status="ERRO",
                error_message=str(erro)
            )

            with lote_lock:
                mensagens_confirmaveis = []
                mensagens_para_reentrega = []
                ids_confirmaveis = ids_existentes if consulta_concluida else set()
                for event_id in ids_do_lote:
                    destino = (
                        mensagens_confirmaveis
                        if event_id in ids_confirmaveis
                        else mensagens_para_reentrega
                    )
                    destino.extend(mensagens_pendentes.pop(event_id, []))
                    if event_id in ids_confirmaveis:
                        eventos_processados.add(event_id)

            print("❌ Erro ao enviar lote para o BigQuery:")
            print(erro)
            alertar_falha("streaming_gravar_lote", erro, execution_id)
            for mensagem in mensagens_confirmaveis:
                mensagem.ack()
            for mensagem in mensagens_para_reentrega:
                mensagem.nack()

        finally:
            with lote_lock:
                if lote_eventos:
                    _agendar_flush_lote()
    finally:
        flush_lock.release()


def _agendar_flush_lote():
    global lote_timer
    if lote_timer is None and lote_eventos:
        lote_timer = threading.Timer(
            BATCH_MAX_WAIT_SECONDS,
            enviar_lote_bigquery
        )
        lote_timer.daemon = True
        lote_timer.start()

def receber_mensagem(message):
    ingested_at = datetime.now(timezone.utc)

    try:
        evento = json.loads(
            message.data.decode("utf-8")
        )

    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        motivo = f"JSON inválido: {erro}"

        registrar_evento_invalido(
            {},
            ingested_at,
            motivo,
            message
        )
        return

    if not isinstance(evento, dict):
        motivo = "Formato inválido: o evento deve ser um objeto JSON"

        registrar_evento_invalido(
            {},
            ingested_at,
            motivo,
            message
        )
        return

    valido, motivo = validar_evento(evento)

    if not valido:
        registrar_evento_invalido(
            evento,
            ingested_at,
            motivo,
            message
        )
        return

    event_id = evento.get("event_id")
    deve_enviar_lote = False

    with lote_lock:
        if event_id in eventos_processados:
            print("⚠️ Evento duplicado ignorado:")
            print(f"event_id: {event_id}")
            print("-" * 50)
            message.ack()
            return

        if event_id in mensagens_pendentes:
            # O evento original ainda não foi persistido. Confirme esta entrega
            # somente junto com o lote que contém o evento original.
            mensagens_pendentes[event_id].append(message)
            print("⚠️ Duplicata recebida antes da gravação; confirmação aguardará o lote.")
            return

        produced_at = pd.to_datetime(
            evento["produced_at"],
            utc=True
        ).to_pydatetime()
        latencia = (ingested_at - produced_at).total_seconds()

        registro = {
            "event_id": event_id,
            "produced_at": evento["produced_at"],
            "ingested_at": ingested_at.isoformat(),
            "ano": evento["ano"],
            "sigla_uf": evento["sigla_uf"],
            "id_municipio": evento["id_municipio"],
            "indicador_alfabetizacao": evento["indicador_alfabetizacao"],
            "origem": evento["origem"],
            "latencia_segundos": latencia
        }

        lote_eventos.append(registro)
        mensagens_pendentes[event_id] = [message]
        _agendar_flush_lote()
        tamanho_lote_atual = len(lote_eventos)
        deve_enviar_lote = tamanho_lote_atual >= TAMANHO_LOTE

    print(f"📦 Evento adicionado ao lote: {tamanho_lote_atual}/{TAMANHO_LOTE}")

    print("✅ Evento válido:")
    print(evento)
    print(f"Horário de produção: {produced_at.isoformat()}")
    print(f"Horário de chegada: {ingested_at.isoformat()}")
    print(f"Latência: {latencia:.3f} segundos")
    print("-" * 50)

    if deve_enviar_lote:
        enviar_lote_bigquery()


print("Aguardando eventos...")

streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=receber_mensagem
)

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    if lote_timer is not None:
        lote_timer.cancel()
    enviar_lote_bigquery()
    print("\nConsumidor encerrado.")
