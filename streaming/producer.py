import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv

from google.cloud import pubsub_v1

load_dotenv()


PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "alfabetizacao-eventos")
INTERVALO_SEGUNDOS = float(
    os.getenv("STREAMING_INTERVAL_SECONDS", "5")
)

MAX_EVENTOS_TESTE = int(
    os.getenv("STREAMING_MAX_TEST_EVENTS", "20")
)

MODO_TESTE = (
    os.getenv("MODO_TESTE", "false").lower() == "true"
)
TESTE_DETERMINISTICO = (
    os.getenv("STREAMING_TEST_DETERMINISTIC", "false").lower() == "true"
)

# Ano com meta municipal publicada (2024), para o evento ser comparável na Gold
ANO_EVENTO = int(os.getenv("STREAMING_ANO", "2024"))

# Municípios sorteados pelo simulador, no formato "codigo_ibge:UF,..."
MUNICIPIOS = [
    tuple(item.strip().split(":"))
    for item in os.getenv(
        "STREAMING_MUNICIPIOS",
        "4118501:PR,3550308:SP,3304557:RJ,2304400:CE,2927408:BA,"
        "3106200:MG,4106902:PR,4314902:RS,1302603:AM,2611606:PE"
    ).split(",")
]

if not PROJECT_ID:
    raise ValueError(
        "Variável de ambiente GCP_PROJECT_ID não configurada."
    )

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

ultimo_event_id = None
indice_evento_teste = 0


def gerar_evento():
    global ultimo_event_id, indice_evento_teste
    id_municipio, sigla_uf = random.choice(MUNICIPIOS)
    evento = {
        "event_id": str(uuid.uuid4()),
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "ano": ANO_EVENTO,
        "sigla_uf": sigla_uf,
        "id_municipio": id_municipio,
        "indicador_alfabetizacao": round(random.uniform(60, 90), 2),
        "origem": "simulado"
    }

    id_atual = evento["event_id"]

    if not MODO_TESTE:
        return evento

    tipos_evento = [
        "valido",
        "duplicado",
        "indicador_invalido",
        "campo_ausente"
    ]

    if TESTE_DETERMINISTICO:
        tipo_evento = tipos_evento[indice_evento_teste % len(tipos_evento)]
        indice_evento_teste += 1
    else:
        tipo_evento = random.choices(
            [
                "valido",
                "indicador_invalido",
                "campo_ausente",
                "duplicado"
            ],
            weights=[75, 10, 10, 5],
            k=1
        )[0]

    if tipo_evento == "indicador_invalido":
        evento["indicador_alfabetizacao"] = 150

    elif tipo_evento == "campo_ausente":
        evento.pop("sigla_uf")

    elif tipo_evento == "duplicado":
        if ultimo_event_id is not None:
            evento["event_id"] = ultimo_event_id

    ultimo_event_id = evento["event_id"]

    return evento


contador_eventos = 0

while contador_eventos < MAX_EVENTOS_TESTE:
    evento = gerar_evento()

    mensagem = json.dumps(evento).encode("utf-8")

    future = publisher.publish(
        topic_path,
        data=mensagem
    )

    print(f"Evento publicado: {future.result()}")
    print(evento)
    print("-" * 50)

    contador_eventos += 1
    time.sleep(INTERVALO_SEGUNDOS)
print(
    f"✅ Limite de {MAX_EVENTOS_TESTE} eventos atingido. "
    "Produtor encerrado."
)
