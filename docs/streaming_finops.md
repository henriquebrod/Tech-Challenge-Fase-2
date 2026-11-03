# Streaming e FinOps — Indicadores de Alfabetização

Este módulo complementa a ingestão em lote com uma simulação de atualizações de indicadores de alfabetização. Os eventos são enviados pelo Pub/Sub, validados pelo consumidor e gravados no BigQuery. Depois, um script exporta os eventos válidos para Parquet na Silver do mesmo `data_lake`, e o notebook os incorpora à Gold de município com `origem='simulado'`.

Os dados do streaming são simulados. O campo `origem` recebe o valor `simulado`, para que eles possam ser separados dos dados oficiais em todas as camadas.

Todos os comandos abaixo são executados na raiz do projeto.

## Como o fluxo funciona

```text
streaming/producer.py → Pub/Sub → streaming/consumer.py → BigQuery → streaming/exportar_silver.py → data_lake/silver → Gold (notebook, 4.2)
                                          ├── eventos inválidos
                                          ├── métricas de execução
                                          └── alerta por e-mail em falha de gravação
```

O produtor publica eventos com um `event_id` e `produced_at`. O ano vem de `STREAMING_ANO` (padrão 2024, ano com meta municipal publicada) e o município é sorteado da lista `STREAMING_MUNICIPIOS`. O consumidor registra `ingested_at`, calcula a latência e verifica os campos e valores recebidos. Eventos inválidos vão para `eventos_invalidos`; os válidos são gravados em `eventos_validos_particionado`. O script `streaming/exportar_silver.py` gera `data_lake/silver/streaming_eventos_validos.parquet`.

O consumidor grava o lote quando chega a dez eventos ou quando o primeiro evento espera dez segundos. Antes da gravação, consulta o BigQuery uma vez com os IDs do lote. Os valores podem ser alterados por `STREAMING_BATCH_SIZE` e `STREAMING_BATCH_MAX_WAIT_SECONDS`. Se a gravação falhar, as mensagens recebem `nack` para nova entrega, a métrica do lote fica com status `ERRO` e o alerta por e-mail de `monitoramento/alertas.py` é enviado. O ID só é considerado processado depois que o lote é gravado.

O Pub/Sub pode entregar a mesma mensagem mais de uma vez. O consumidor mantém em memória os `event_id` já tratados durante a execução, evitando reprocessá-los enquanto o processo está ativo. Essa lista é temporária: some quando o consumidor reinicia e cresce conforme novos eventos são processados. Por isso, a consulta por lote ao BigQuery complementa a verificação e identifica IDs gravados nos últimos 60 dias. Duplicatas recebidas enquanto o evento original ainda está pendente só são confirmadas depois que o lote for gravado.

O arquivo Silver gerado contém somente eventos válidos e sem repetição de `event_id`, com as colunas `ano`, `id_municipio` (texto com sete dígitos), `serie` (`'2'`), `rede` (`'3'`, Municipal no dicionário), `taxa_alfabetizacao` (de 0 a 100), `event_id`, `event_ts` e `ingest_ts`. Na Gold (`gold_municipio_resultado_vs_meta`), esses registros entram com `origem='simulado'` e `fonte_taxa='streaming_simulado'`, comparados à meta municipal do mesmo ano, rede e série; o join com a meta é medido como regra informativa. Para cada município e ano fica o evento ingerido por último.

## Batch e streaming

O batch carrega conjuntos históricos em execuções planejadas; o streaming recebe atualizações individualmente pelo Pub/Sub e as grava em micro-lotes. No simulador, a publicação ocorre a cada cinco segundos; o consumidor fecha o lote ao chegar a dez eventos ou após dez segundos de espera. O streaming reduz o tempo até a atualização ficar disponível, enquanto o batch é mais adequado para cargas maiores e periódicas. O Pub/Sub oferece entrega pelo menos uma vez, então o consumidor usa `event_id` para evitar persistir novamente eventos válidos já processados.

## Rodar o fluxo

Requisitos: Python 3.12, dependências de `requirements.txt` e acesso ao projeto Google Cloud por Application Default Credentials.

```bash
pip install -r requirements.txt
gcloud auth login
gcloud auth application-default login
cp .env.example .env
```

Preencha `GCP_PROJECT_ID` (e, para os alertas, `GMAIL_*`) no `.env`. Para reproduzir em um projeto próprio, use uma conta com permissão para habilitar APIs e criar recursos no Google Cloud. Os comandos abaixo são executados uma vez e podem ser ignorados para recursos que já existam.

No terminal Bash, carregue as variáveis do `.env` e habilite as APIs:

```bash
set -a
source .env
set +a

gcloud services enable pubsub.googleapis.com bigquery.googleapis.com \
  --project="$GCP_PROJECT_ID"
```

Crie o tópico e a assinatura do Pub/Sub:

```bash
gcloud pubsub topics create "$PUBSUB_TOPIC_ID" \
  --project="$GCP_PROJECT_ID"

gcloud pubsub subscriptions create "$PUBSUB_SUBSCRIPTION_ID" \
  --topic="$PUBSUB_TOPIC_ID" \
  --project="$GCP_PROJECT_ID"
```

Crie o dataset e as tabelas do BigQuery na localização `US` (a mesma da Base dos Dados e dos datasets Gold). O dataset usado nas execuções de outubro foi criado em `southamerica-east1`, como mostra o print `15_bigquery_bytes_faturados.png`; os dois funcionam, porque o exportador grava a Silver a partir de um arquivo local:

```bash
bq --project_id="$GCP_PROJECT_ID" --location=US \
  mk --dataset "$GCP_PROJECT_ID:$BIGQUERY_DATASET_ID"

bq --project_id="$GCP_PROJECT_ID" --location=US \
  mk --table \
  --time_partitioning_type=DAY \
  --time_partitioning_field=ingested_at \
  --time_partitioning_expiration=5184000 \
  "$GCP_PROJECT_ID:$BIGQUERY_DATASET_ID.$BIGQUERY_TABLE_VALIDOS_ID" \
  'event_id:STRING,produced_at:TIMESTAMP,ingested_at:TIMESTAMP,ano:INTEGER,sigla_uf:STRING,id_municipio:STRING,indicador_alfabetizacao:FLOAT,origem:STRING,latencia_segundos:FLOAT'

bq --project_id="$GCP_PROJECT_ID" --location=US \
  mk --table \
  "$GCP_PROJECT_ID:$BIGQUERY_DATASET_ID.$BIGQUERY_TABLE_INVALIDOS_ID" \
  'event_id:STRING,produced_at:TIMESTAMP,ingested_at:TIMESTAMP,ano:INTEGER,sigla_uf:STRING,id_municipio:STRING,indicador_alfabetizacao:FLOAT,origem:STRING,motivo_erro:STRING'

bq --project_id="$GCP_PROJECT_ID" --location=US \
  mk --table \
  "$GCP_PROJECT_ID:$BIGQUERY_DATASET_ID.$BIGQUERY_TABLE_METRICAS_ID" \
  'execution_id:STRING,started_at:TIMESTAMP,finished_at:TIMESTAMP,duration_seconds:FLOAT,input_count:INTEGER,output_count:INTEGER,invalid_count:INTEGER,status:STRING,error_message:STRING'
```

A coluna `latencia_segundos` da tabela de válidos é gravada pelo consumidor; ela não estava no comando original de criação da tabela.

Depois de criar os recursos, inicie o consumidor antes do produtor, cada um em seu terminal:

```bash
python streaming/consumer.py
python streaming/producer.py
```

Depois de receber eventos, gere o arquivo Parquet e rode o notebook a partir da etapa 4 (ou inteiro) para levar os eventos à Gold:

```bash
python streaming/exportar_silver.py
```

Para publicar somente eventos normais, use `MODO_TESTE=false`. O produtor encerra ao atingir o limite definido em `STREAMING_MAX_TEST_EVENTS` (4 no `.env.example`). Para um teste curto e repetível dos casos de qualidade, configure `MODO_TESTE=true`, `STREAMING_TEST_DETERMINISTIC=true` e `STREAMING_MAX_TEST_EVENTS=4`. O produtor envia, nesta ordem, um evento válido, um evento com o mesmo `event_id`, um evento com indicador inválido e um evento sem `sigla_uf`. Para um teste aleatório, use `STREAMING_TEST_DETERMINISTIC=false`; nesse modo, os pesos são 75%, 10%, 10% e 5%, respectivamente. Depois do teste, volte `MODO_TESTE=false`.

Credenciais não devem ser colocadas no repositório.

## Validações e métricas

O consumidor verifica os campos obrigatórios e exige indicador entre 0 e 100. Também calcula a latência usando `produced_at` e o horário de chegada registrado em `ingested_at`, gravados em momentos diferentes (produtor e consumidor).

As métricas são gravadas em `metricas_streaming`: início, fim, duração, quantidade de eventos, inválidos e status. Para consultar as execuções recentes:

```sql
SELECT *
FROM `SEU_PROJECT_ID.alfabetizacao_streaming.metricas_streaming`
ORDER BY started_at DESC
LIMIT 20;
```

A latência é a diferença entre `produced_at` e `ingested_at`. A consulta registrada em `evidencias/streaming_pubsub_2026-10/13_latencia_streaming_resultados.png` mostra média de **0,878 s** (mínimo 0,209 s, máximo 1,624 s).

**Atenção — latência com fila acumulada.** O arquivo `data_lake/silver/streaming_eventos_validos.parquet` exportado em 01/11 (62 eventos únicos, de 17/10 a 29/10/2026) tem latência mínima de 0,209 s e máxima de 432.868 s (cerca de 5 dias); o print `18_eventos_validos_particionado.png` também mostra eventos com cerca de 1.200 s. Os 21 eventos acima de 60 s são mensagens publicadas enquanto o consumidor estava desligado e consumidas depois: a latência medida está correta, mas mede espera na fila, não processamento. Nos 41 eventos consumidos em tempo real, a mediana é **0,89 s** e o p95 é **2,5 s**.

## Medições e estimativa de custo

Uma consulta representativa no BigQuery processou 496 B, faturou 10 MB e durou 347 ms; o uso de slot foi de 49 ms. A consulta real de deduplicação processou 3.213 B e faturou o mínimo de 10 MiB. Nas medições das tabelas, `eventos_validos_particionado` tinha 84 linhas, cinco partições, 8,27 KB lógicos e 22,47 KB físicos. `eventos_invalidos` tinha 17 linhas, 2,29 KB lógicos e 14,98 KB físicos.

Em 01/11, a tabela tinha 91 linhas e 62 `event_id` únicos: 29 duplicatas gravadas em 19, 20 e 23/10, e nenhuma em 29/10, quando o consumidor já consultava o BigQuery por lote antes de gravar (prints `evidencias/execucao_final/04_` e `05_anomalia_duplicatas_*`). O exportador remove os `event_id` repetidos, então a Silver e a Gold recebem só os 62 eventos únicos.

A tabela `eventos_validos_particionado` é particionada diariamente por `ingested_at`, com expiração das partições após 60 dias.

As consultas ao BigQuery foram executadas no modo Sandbox. A estimativa abaixo usa 100 mil eventos por dia, durante 30 dias, e os preços de referência consultados em 28/10/2026. A projeção considera as franquias gratuitas indicadas quando disponíveis e mostra o custo após as franquias para estimar o mesmo desenho em uso real. O piso de faturamento foi calculado a partir da medição da consulta de deduplicação.

A projeção de armazenamento usa a medição inicial de 1.212 bytes para 12 eventos válidos, cerca de 101 bytes por evento. Aplicada a 3 milhões de eventos por mês, essa média resulta em aproximadamente 0,282 GiB de novos dados mensais.

| Serviço | Unidade | Quantidade medida | Quantidade projetada | Preço unitário de referência | Custo mensal projetado |
|---|---|---:|---:|---:|---:|
| BigQuery — deduplicação (piso de faturamento) | Bytes por consulta / TiB por mês | 3.213 B processados e 10 MiB faturados por consulta | ~2,86 TiB (300 mil consultas × 10 MiB) | US$ 6,25/TiB após 1 TiB gratuito | Piso de ~US$ 11,63, se a franquia estiver disponível |
| BigQuery — armazenamento | GiB-mês | 8,27 KB lógicos na medição da tabela | ~0,282 GiB novos por mês | ~US$ 0,023/GiB-mês após 10 GiB gratuitos | US$ 0,00 se permanecer dentro da franquia |
| Pub/Sub — publicação e entrega | GB transferidos por mês | Não medido diretamente; usada a premissa de 1 KB por evento | ~6 GB (publicação e entrega) | US$ 40/TiB após 10 GiB gratuitos | US$ 0,00 se permanecer dentro da franquia |

Na projeção de 3 milhões de eventos por mês, considera-se uma consulta para cada lote de dez eventos: cerca de 300 mil consultas. O mínimo faturável de 10 MiB por consulta representa um piso de aproximadamente 2,86 TiB/mês, ou US$ 11,63 após a franquia compartilhada de 1 TiB. Esse piso não é uma previsão completa: com o aumento da tabela, a consulta de deduplicação pode processar mais bytes. O cálculo também não inclui consultas de monitoramento agendadas, pois não há agendador configurado neste módulo. A referência de preço é a [página oficial do BigQuery](https://cloud.google.com/bigquery/pricing).

No consumidor, `BIGQUERY_DEDUP_MAX_BYTES_BILLED=1073741824` define um teto de 1 GiB por consulta de deduplicação. A busca considera apenas os 60 dias retidos na tabela particionada. Se uma consulta ultrapassar o teto, o BigQuery a interromperá e as mensagens serão reentregues até o limite ser revisto. A exportação Silver também tem teto de 1 GiB por execução (`BIGQUERY_MAX_BYTES_BILLED=1073741824`); como ela lê a tabela toda e é manual, seu custo depende da frequência e do tamanho da tabela.

Em uma projeção separada de crescimento para 10 milhões de eventos por mês, com lotes médios de dez eventos, o piso de faturamento das consultas de deduplicação é de cerca de 9,54 TiB. Após 1 TiB gratuito, isso representa aproximadamente US$ 53,35. Esse cálculo não é uma previsão completa: o volume processado pela consulta pode crescer junto com a tabela, e a estimativa precisa ser revista com dry run ou medição em escala representativa.

Nos cenários projetados, a deduplicação é o principal ponto de atenção de custo. A estimativa é um piso, pois os bytes processados podem aumentar com o crescimento da tabela. Em volumes maiores, vale avaliar formas de reduzir as consultas ao BigQuery sem perder a proteção contra duplicados após a reinicialização do consumidor.

Esta tabela cobre só o streaming. O custo do batch (consultas da ingestão, armazenamento do `data_lake`) ainda precisa entrar na estimativa: os bytes de cada consulta passam a ser medidos por dry run e gravados em `bytes_lidos` nas etapas `bronze_*` do `execution_log.csv`.

## Orçamento e alertas

Foi configurado no Google Cloud um orçamento mensal de **R$ 30,00**, cobrindo BigQuery, Cloud Pub/Sub e Cloud Storage (`evidencias/execucao_final/01_orcamento_bigquery_pubsub_storage.png`). A conta de faturamento do projeto foi encerrada depois das execuções do streaming; desde então o projeto está no BigQuery Sandbox, onde não há cobrança possível. Os alertas por e-mail estão definidos para **50% (R$ 15,00), 90% (R$ 27,00) e 100% (R$ 30,00)** do orçamento e são enviados a administradores e usuários de faturamento. O orçamento alerta sobre o gasto, mas não interrompe automaticamente os serviços.

Para reproduzir essa configuração no Console do Google Cloud:

1. Abra **Faturamento (Billing)** e selecione a conta de faturamento usada pelo projeto.
2. Acesse **Orçamentos e alertas (Budgets & alerts)** e escolha **Criar orçamento (Create budget)**.
3. Informe um nome para o orçamento e selecione o projeto que contém os recursos deste fluxo. Configure o período como mensal e o valor como **R$ 30,00**.
4. Nos filtros de serviços, selecione **BigQuery** e **Cloud Pub/Sub**.
5. Configure alertas de gasto nos limites de **50%, 90% e 100%** do orçamento. Ative o envio de notificações por e-mail para administradores e usuários da conta de faturamento.
6. Revise o escopo e salve o orçamento.

Os nomes dos menus podem aparecer em português ou inglês, conforme o idioma do Console. O orçamento monitora os custos dos serviços selecionados, mas não limita nem interrompe o uso. A configuração é vinculada à conta de faturamento e ao projeto selecionado; não é criada pelos scripts deste repositório.

## Evidências e limites

As evidências das execuções estão em `evidencias/streaming_pubsub_2026-10/`. Elas mostram o tópico e a assinatura, publicação e consumo de mensagens, inválidos e duplicatas, métricas, latência, tabelas e particionamento no BigQuery e o custo das consultas. O orçamento está em `evidencias/execucao_final/01_orcamento_bigquery_pubsub_storage.png`.

O produtor gera dados sintéticos e em volume de teste. Os eventos do arquivo exportado atual são todos de `ano=2026` e do município 4118501, gerados antes da parametrização de ano e municípios; na Gold eles viram uma única linha, comparada com a meta de 2026 da trajetória pactuada em 2024 (status "não atingiu"). Uma nova rodada do produtor e do consumidor (que exige Pub/Sub e, portanto, faturamento ativo) é necessária para ter eventos de 2024 em vários municípios. A exportação Silver é executada manualmente; não há agendador ou orquestrador neste módulo.
