# Documentação Técnica de Dados e Regras de Negócio

## 1. Objetivo

Este documento integra a documentação técnica do projeto **Pipeline Híbrido para Análise da Alfabetização no Brasil** e complementa o `README.md`.

Seu objetivo é registrar os **dados, tabelas, campos, tipos, relacionamentos, transformações, regras de negócio e regras de qualidade** utilizados pelo pipeline, servindo como referência técnica para manutenção, validação e evolução da solução.

## 2. Convenções de tipos

O projeto utiliza pandas/Parquet localmente e BigQuery na nuvem. Para facilitar a leitura, este documento usa o **tipo lógico** do dado e, quando aplicável, o tipo utilizado no processamento.

| Tipo lógico | pandas                  | BigQuery                             | Uso                                          |
| ----------- | ----------------------- | ------------------------------------ | -------------------------------------------- |
| Inteiro     | `Int64`                 | `INTEGER`                            | ano, contagens e classificações inteiras     |
| Texto       | `object` / `string`     | `STRING`                             | identificadores, códigos e descrições        |
| Decimal     | `float64`               | `FLOAT`                              | taxas, médias, pesos e proporções            |
| Data/hora   | string ISO / `datetime` | `TIMESTAMP`                          | produção, ingestão, início e fim de execução |
| Booleano    | `bool`                  | `BOOLEAN` ou texto em relatórios CSV | flags internas e resultados de validação     |

### 2.1 Metadados comuns da Bronze/Silver

As entidades ingeridas recebem quatro campos técnicos de linhagem:

| Campo                  | Tipo   | Regra / significado                                          |
| ---------------------- | ------ | ------------------------------------------------------------ |
| `_ingestion_timestamp` | STRING | Timestamp da carga no formato `YYYYMMDD_HHMMSS`              |
| `_source_table`        | STRING | Tabela de origem na Base dos Dados                           |
| `_source_system`       | STRING | Valor utilizado: `basedosdados_bigquery`                     |
| `_record_hash`         | STRING | Hash MD5 do conteúdo do registro, usado para rastreabilidade |

## 3. Dicionário de Dados — Fonte e Bronze

A Bronze preserva os campos da fonte e adiciona os metadados de linhagem descritos na seção 2.1. A política de escrita no BigQuery é `WRITE_APPEND`, preservando cargas anteriores.

Os tipos abaixo correspondem aos tipos registrados pelo pandas durante a execução documentada do pipeline. A coluna **Nulos registrados** informa a quantidade observada nessa execução.

### 3.1 `municipio`

| Campo                     | Tipo pandas | Nulos registrados | Descrição / regra                                                                             |
| ------------------------- | ----------- | ----------------: | --------------------------------------------------------------------------------------------- |
| `ano`                     | `Int64`     |                 0 | Ano de referência do indicador.                                                               |
| `id_municipio`            | `object`    |                 0 | Código do município utilizado nas chaves de integração.                                       |
| `serie`                   | `object`    |                 0 | Código da série escolar.                                                                      |
| `rede`                    | `object`    |                 0 | Código da rede de ensino; a descrição é obtida pelo dicionário oficial.                       |
| `taxa_alfabetizacao`      | `float64`   |                 0 | Percentual oficial de alfabetização. Valores não nulos são validados no intervalo de 0 a 100. |
| `media_portugues`         | `float64`   |                 0 | Média de proficiência em Língua Portuguesa.                                                   |
| `proporcao_aluno_nivel_0` | `float64`   |            11.547 | Proporção de alunos no nível 0.                                                               |
| `proporcao_aluno_nivel_1` | `float64`   |            11.547 | Proporção de alunos no nível 1.                                                               |
| `proporcao_aluno_nivel_2` | `float64`   |            11.547 | Proporção de alunos no nível 2.                                                               |
| `proporcao_aluno_nivel_3` | `float64`   |            11.547 | Proporção de alunos no nível 3.                                                               |
| `proporcao_aluno_nivel_4` | `float64`   |            11.547 | Proporção de alunos no nível 4.                                                               |
| `proporcao_aluno_nivel_5` | `float64`   |            11.547 | Proporção de alunos no nível 5.                                                               |
| `proporcao_aluno_nivel_6` | `float64`   |            11.547 | Proporção de alunos no nível 6.                                                               |
| `proporcao_aluno_nivel_7` | `float64`   |            11.547 | Proporção de alunos no nível 7.                                                               |
| `proporcao_aluno_nivel_8` | `float64`   |            11.547 | Proporção de alunos no nível 8.                                                               |

### 3.2 `uf`

| Campo                     | Tipo pandas | Nulos registrados | Descrição / regra                                                                             |
| ------------------------- | ----------- | ----------------: | --------------------------------------------------------------------------------------------- |
| `ano`                     | `Int64`     |                 0 | Ano de referência.                                                                            |
| `sigla_uf`                | `object`    |                 0 | Sigla da Unidade Federativa.                                                                  |
| `serie`                   | `object`    |                 0 | Código da série escolar.                                                                      |
| `rede`                    | `object`    |                 0 | Código da rede de ensino.                                                                     |
| `taxa_alfabetizacao`      | `float64`   |                 0 | Percentual oficial de alfabetização. Valores não nulos são validados no intervalo de 0 a 100. |
| `media_portugues`         | `float64`   |                 0 | Média de proficiência em Língua Portuguesa.                                                   |
| `proporcao_aluno_nivel_0` | `float64`   |                70 | Proporção de alunos no nível 0.                                                               |
| `proporcao_aluno_nivel_1` | `float64`   |                70 | Proporção de alunos no nível 1.                                                               |
| `proporcao_aluno_nivel_2` | `float64`   |                70 | Proporção de alunos no nível 2.                                                               |
| `proporcao_aluno_nivel_3` | `float64`   |                70 | Proporção de alunos no nível 3.                                                               |
| `proporcao_aluno_nivel_4` | `float64`   |                70 | Proporção de alunos no nível 4.                                                               |
| `proporcao_aluno_nivel_5` | `float64`   |                70 | Proporção de alunos no nível 5.                                                               |
| `proporcao_aluno_nivel_6` | `float64`   |                70 | Proporção de alunos no nível 6.                                                               |
| `proporcao_aluno_nivel_7` | `float64`   |                70 | Proporção de alunos no nível 7.                                                               |
| `proporcao_aluno_nivel_8` | `float64`   |                70 | Proporção de alunos no nível 8.                                                               |

### 3.3 `meta_alfabetizacao_brasil`

| Campo                     | Tipo pandas | Nulos registrados | Descrição / regra                                      |
| ------------------------- | ----------- | ----------------: | ------------------------------------------------------ |
| `ano`                     | `Int64`     |                 0 | Ano da linha de resultado/pactuação nacional.          |
| `rede`                    | `object`    |                 0 | Rede à qual a taxa e a trajetória de metas se referem. |
| `taxa_alfabetizacao`      | `float64`   |                 0 | Taxa oficial publicada para o ano.                     |
| `meta_alfabetizacao_2024` | `float64`   |                 0 | Meta pactuada para 2024.                               |
| `meta_alfabetizacao_2025` | `float64`   |                 0 | Meta pactuada para 2025.                               |
| `meta_alfabetizacao_2026` | `float64`   |                 0 | Meta pactuada para 2026.                               |
| `meta_alfabetizacao_2027` | `float64`   |                 0 | Meta pactuada para 2027.                               |
| `meta_alfabetizacao_2028` | `float64`   |                 0 | Meta pactuada para 2028.                               |
| `meta_alfabetizacao_2029` | `float64`   |                 0 | Meta pactuada para 2029.                               |
| `meta_alfabetizacao_2030` | `float64`   |                 0 | Meta pactuada para 2030.                               |
| `percentual_participacao` | `float64`   |                 0 | Percentual de participação da avaliação.               |

### 3.4 `meta_alfabetizacao_uf`

| Campo                     | Tipo pandas | Nulos registrados | Descrição / regra                             |
| ------------------------- | ----------- | ----------------: | --------------------------------------------- |
| `ano`                     | `Int64`     |                 0 | Ano da linha de resultado/pactuação estadual. |
| `sigla_uf`                | `object`    |                 0 | Sigla da Unidade Federativa.                  |
| `rede`                    | `object`    |                 0 | Rede à qual a taxa e as metas se referem.     |
| `taxa_alfabetizacao`      | `float64`   |                 4 | Taxa publicada para a UF.                     |
| `meta_alfabetizacao_2024` | `float64`   |                 9 | Meta pactuada para 2024.                      |
| `meta_alfabetizacao_2025` | `float64`   |                 3 | Meta pactuada para 2025.                      |
| `meta_alfabetizacao_2026` | `float64`   |                 2 | Meta pactuada para 2026.                      |
| `meta_alfabetizacao_2027` | `float64`   |                 2 | Meta pactuada para 2027.                      |
| `meta_alfabetizacao_2028` | `float64`   |                 2 | Meta pactuada para 2028.                      |
| `meta_alfabetizacao_2029` | `float64`   |                 2 | Meta pactuada para 2029.                      |
| `meta_alfabetizacao_2030` | `float64`   |                 2 | Meta pactuada para 2030.                      |
| `percentual_participacao` | `float64`   |                 4 | Percentual de participação.                   |

### 3.5 `meta_alfabetizacao_municipio`

| Campo                     | Tipo pandas | Nulos registrados | Descrição / regra                                       |
| ------------------------- | ----------- | ----------------: | ------------------------------------------------------- |
| `ano`                     | `Int64`     |                 0 | Ano da linha de resultado/pactuação municipal.          |
| `id_municipio`            | `object`    |                 0 | Código do município utilizado nas chaves de integração. |
| `rede`                    | `object`    |                 0 | Rede à qual a taxa e as metas se referem.               |
| `taxa_alfabetizacao`      | `float64`   |               120 | Taxa municipal publicada.                               |
| `meta_alfabetizacao_2024` | `float64`   |               240 | Meta pactuada para 2024.                                |
| `meta_alfabetizacao_2025` | `float64`   |                 0 | Meta pactuada para 2025.                                |
| `meta_alfabetizacao_2026` | `float64`   |                 0 | Meta pactuada para 2026.                                |
| `meta_alfabetizacao_2027` | `float64`   |                 0 | Meta pactuada para 2027.                                |
| `meta_alfabetizacao_2028` | `float64`   |                 0 | Meta pactuada para 2028.                                |
| `meta_alfabetizacao_2029` | `float64`   |                 0 | Meta pactuada para 2029.                                |
| `meta_alfabetizacao_2030` | `float64`   |                 0 | Meta pactuada para 2030.                                |
| `nivel_alfabetizacao`     | `Int64`     |               120 | Nível de alfabetização informado pela fonte.            |
| `percentual_participacao` | `float64`   |               120 | Percentual de participação.                             |

### 3.6 `dicionario`

| Campo                | Tipo pandas | Nulos registrados | Descrição / regra                                 |
| -------------------- | ----------- | ----------------: | ------------------------------------------------- |
| `id_tabela`          | `object`    |                 0 | Identifica a tabela à qual o mapeamento pertence. |
| `nome_coluna`        | `object`    |                 0 | Identifica a coluna categórica traduzida.         |
| `chave`              | `object`    |                 0 | Código original da categoria.                     |
| `cobertura_temporal` | `object`    |                 0 | Período de cobertura informado pela fonte.        |
| `valor`              | `object`    |                 0 | Descrição oficial associada ao código.            |

### 3.7 `alunos`

| Campo                   | Tipo pandas | Nulos registrados | Descrição / regra                                                            |
| ----------------------- | ----------- | ----------------: | ---------------------------------------------------------------------------- |
| `ano`                   | `Int64`     |                 0 | Ano da avaliação.                                                            |
| `id_municipio`          | `object`    |                 0 | Código do município.                                                         |
| `id_escola`             | `object`    |                 0 | Identificador da escola.                                                     |
| `id_aluno`              | `object`    |                 0 | Identificador do aluno.                                                      |
| `caderno`               | `object`    |                 0 | Código do caderno/prova.                                                     |
| `serie`                 | `object`    |                 0 | Código da série escolar.                                                     |
| `rede`                  | `object`    |                 0 | Código da rede de ensino.                                                    |
| `presenca`              | `object`    |                 0 | Código de presença; a descrição é obtida pelo dicionário.                    |
| `preenchimento_caderno` | `object`    |                 0 | Código de preenchimento do caderno; a descrição é obtida pelo dicionário.    |
| `alfabetizado`          | `object`    |                 0 | Indicador categórico de alfabetização; a descrição é obtida pelo dicionário. |
| `proficiencia`          | `float64`   |           513.338 | Pontuação de proficiência.                                                   |
| `peso_aluno`            | `float64`   |           513.338 | Peso amostral utilizado no cálculo ponderado.                                |

## 4. Camada Silver

A Silver mantém os campos das entidades, padroniza tipos, remove duplicatas exatas e preserva valores nulos da fonte.

### 4.1 Regras de transformação

- `id_municipio`, `sigla_uf`, `serie`, `rede`, `id_escola`, `id_aluno`, `caderno`, `presenca`, `preenchimento_caderno` e `alfabetizado` são tratados como texto nas respectivas entidades em que existem;
- `taxa_alfabetizacao`, `media_portugues`, `proporcao_aluno_nivel_0`, `proporcao_aluno_nivel_1`, `proporcao_aluno_nivel_2`, `proporcao_aluno_nivel_3`, `proporcao_aluno_nivel_4`, `proporcao_aluno_nivel_5`, `proporcao_aluno_nivel_6`, `proporcao_aluno_nivel_7`, `proporcao_aluno_nivel_8`, `proficiencia`, `peso_aluno`, `percentual_participacao` e as metas anuais são convertidos para valores numéricos quando presentes;
- valores que não podem ser convertidos numericamente passam a nulos por meio de `pd.to_numeric` com `errors='coerce'`;
- valores nulos não são preenchidos com zero;
- duplicatas exatas são removidas;
- o campo `_record_hash` é recalculado após o tratamento;
- os arquivos Silver são gravados em Parquet;
- no BigQuery, a Silver é publicada com `WRITE_TRUNCATE`.

### 4.2 Chaves e validações por entidade

| Entidade                       | Regra de identificação / unicidade utilizada                                                                    |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `municipio`                    | unicidade em `ano + id_municipio + serie + rede`                                                                |
| `uf`                           | unicidade em `ano + sigla_uf + serie + rede`                                                                    |
| `meta_alfabetizacao_brasil`    | unicidade em `ano + rede`                                                                                       |
| `meta_alfabetizacao_uf`        | unicidade em `ano + sigla_uf + rede`                                                                            |
| `meta_alfabetizacao_municipio` | unicidade em `ano + id_municipio + rede`                                                                        |
| `dicionario`                   | ausência de nulos em `id_tabela + nome_coluna + chave + valor` e unicidade em `id_tabela + nome_coluna + chave` |
| `alunos`                       | ausência de nulos em `ano + id_municipio + id_escola + id_aluno` e unicidade do aluno em `ano + id_aluno`       |

### 4.3 Regras de domínio e consistência

- `taxa_alfabetizacao`: valores não nulos devem permanecer entre 0 e 100;
- metas de alfabetização: valores não nulos devem permanecer entre 0 e 100;
- `peso_aluno`: não pode ser negativo;
- chaves obrigatórias não podem ser nulas;
- o módulo `qualidade/verificador.py` também disponibiliza verificações de chave nula ou vazia, chave única, formato, domínio, intervalo e percentual de nulos.

## 5. Dicionário de Dados — Streaming

### 5.1 Evento publicado pelo Producer

Arquivo: `streaming/producer.py`.

| Campo                     | Tipo               | Obrigatório | Regra                                              |
| ------------------------- | ------------------ | ----------- | -------------------------------------------------- |
| `event_id`                | STRING             | Sim         | UUID do evento; chave de deduplicação              |
| `produced_at`             | TIMESTAMP/ISO-8601 | Sim         | Gerado pelo producer em UTC                        |
| `ano`                     | INTEGER            | Sim         | Configurado por `STREAMING_ANO`; padrão atual 2024 |
| `sigla_uf`                | STRING             | Sim         | UF associada ao município simulado                 |
| `id_municipio`            | STRING             | Sim         | Código IBGE do município simulado                  |
| `indicador_alfabetizacao` | FLOAT              | Sim         | Valor simulado; deve permanecer entre 0 e 100      |
| `origem`                  | STRING             | Sim         | Deve ser exatamente `simulado`                     |

O modo de teste pode gerar intencionalmente evento válido, duplicado, indicador fora do domínio e evento com campo ausente.

### 5.2 `eventos_validos_particionado`

Tabela BigQuery utilizada pelo consumer.

| Campo                     | Tipo BigQuery | Regra                                   |
| ------------------------- | ------------- | --------------------------------------- |
| `event_id`                | STRING        | Identificador único lógico              |
| `produced_at`             | TIMESTAMP     | Horário do producer                     |
| `ingested_at`             | TIMESTAMP     | Horário de chegada no consumer          |
| `ano`                     | INTEGER       | Ano do evento                           |
| `sigla_uf`                | STRING        | UF                                      |
| `id_municipio`            | STRING        | Município                               |
| `indicador_alfabetizacao` | FLOAT         | Domínio 0–100                           |
| `origem`                  | STRING        | `simulado`                              |
| `latencia_segundos`       | FLOAT         | `ingested_at - produced_at` em segundos |

A tabela é particionada por `ingested_at` e, na configuração documentada, possui retenção de 60 dias.

### 5.3 `eventos_invalidos`

| Campo                     | Tipo BigQuery | Regra                                                |
| ------------------------- | ------------- | ---------------------------------------------------- |
| `event_id`                | STRING        | Pode estar ausente quando o JSON não é interpretável |
| `produced_at`             | TIMESTAMP     | Valor recebido, se disponível                        |
| `ingested_at`             | TIMESTAMP     | Momento em que o consumer tratou o evento            |
| `ano`                     | INTEGER       | Valor recebido, se disponível                        |
| `sigla_uf`                | STRING        | Valor recebido, se disponível                        |
| `id_municipio`            | STRING        | Valor recebido, se disponível                        |
| `indicador_alfabetizacao` | FLOAT         | Valor recebido, mesmo quando inválido                |
| `origem`                  | STRING        | Valor recebido                                       |
| `motivo_erro`             | STRING        | Motivo da rejeição                                   |

### 5.4 `metricas_streaming`

| Campo              | Tipo BigQuery | Descrição                              |
| ------------------ | ------------- | -------------------------------------- |
| `execution_id`     | STRING        | Identificador do processamento do lote |
| `started_at`       | TIMESTAMP     | Início do envio do lote                |
| `finished_at`      | TIMESTAMP     | Fim do processamento                   |
| `duration_seconds` | FLOAT         | Duração do lote                        |
| `input_count`      | INTEGER       | Eventos recebidos no lote              |
| `output_count`     | INTEGER       | Eventos novos efetivamente persistidos |
| `invalid_count`    | INTEGER       | Inválidos contabilizados               |
| `status`           | STRING        | `SUCESSO` ou `ERRO`                    |
| `error_message`    | STRING        | Mensagem de erro quando aplicável      |

### 5.5 Silver do Streaming — `streaming_eventos_validos`

Gerada por `streaming/exportar_silver.py`.

| Campo                | Tipo lógico | Transformação                                                   |
| -------------------- | ----------- | --------------------------------------------------------------- |
| `ano`                | INTEGER     | Mantido do evento                                               |
| `id_municipio`       | STRING      | Convertido para texto e completado para 7 dígitos               |
| `serie`              | STRING      | Valor fixo `'2'`                                                |
| `rede`               | STRING      | Valor fixo `'3'`, correspondente à rede Municipal no dicionário |
| `taxa_alfabetizacao` | FLOAT       | Renomeado de `indicador_alfabetizacao`                          |
| `event_id`           | STRING      | Mantido para deduplicação                                       |
| `event_ts`           | TIMESTAMP   | Renomeado de `produced_at`                                      |
| `ingest_ts`          | TIMESTAMP   | Renomeado de `ingested_at`                                      |

Antes da gravação são removidas duplicidades por `event_id`, mantendo a última ocorrência, e taxas fora de 0–100 são descartadas.

## 6. Monitoramento e Relatórios Técnicos

### 6.1 `execution_log.csv`

| Campo              | Descrição                          |
| ------------------ | ---------------------------------- |
| `run_id`           | Identificador da execução          |
| `etapa`            | Nome técnico da etapa              |
| `inicio`           | Início da etapa                    |
| `fim`              | Fim da etapa                       |
| `duracao_segundos` | Duração                            |
| `linhas_entrada`   | Quantidade de registros de entrada |
| `linhas_saida`     | Quantidade de registros de saída   |
| `bytes_lidos`      | Volume lido/estimado               |
| `status`           | `SUCESSO` ou `FALHA`               |
| `erro`             | Mensagem da exceção, quando houver |

### 6.2 `gold_checks.csv`

| Campo           | Descrição                                      |
| --------------- | ---------------------------------------------- |
| `gold_run_ts`   | Identificador da execução que produziu o check |
| `regra`         | Nome da regra aplicada                         |
| `tabela`        | Entidade/tabela validada                       |
| `tipo`          | `bloqueante` ou `informativa`                  |
| `status`        | `APROVADO` ou `REPROVADO`                      |
| `valor`         | Valor medido                                   |
| `limiar`        | Limite utilizado                               |
| `detalhe`       | Evidência textual da regra                     |
| `registrado_em` | Timestamp do registro                          |

### 6.3 `gold_anomalias.csv`

| Campo         | Descrição                         |
| ------------- | --------------------------------- |
| `gold_run_ts` | Identificador da execução         |
| `codigo`      | Código da anomalia                |
| `descricao`   | Descrição do problema investigado |
| `evidencia`   | Dados que sustentam a análise     |

### 6.4 Tipagem no BigQuery dos relatórios CSV

O módulo `nuvem/lake.py` cria o schema dos arquivos CSV de monitoramento com **todas as colunas como `STRING`**. Isso é intencional para suportar valores heterogêneos em campos como `valor`, `limiar`, `detalhe` e `erro`.

## 7. Camada Gold

A camada Gold concentra as estruturas analíticas derivadas das camadas Silver e das regras de integração do projeto.

### 7.1 `municipio_metas_gold`

A tabela é formada por `inner join` entre `municipio_silver` e `meta_alfabetizacao_municipio_silver`, utilizando:

```text
ano + id_municipio + rede_descricao
```

Após o join, `rede_descricao` é removida. Os campos resultantes registrados pela execução são:

- `ano`;
- `id_municipio`;
- `serie`;
- `rede_oficial`;
- `taxa_alfabetizacao_oficial`;
- `media_portugues`;
- `proporcao_aluno_nivel_0`;
- `proporcao_aluno_nivel_1`;
- `proporcao_aluno_nivel_2`;
- `proporcao_aluno_nivel_3`;
- `proporcao_aluno_nivel_4`;
- `proporcao_aluno_nivel_5`;
- `proporcao_aluno_nivel_6`;
- `proporcao_aluno_nivel_7`;
- `proporcao_aluno_nivel_8`;
- `_ingestion_timestamp_oficial`;
- `_source_table_oficial`;
- `_source_system_oficial`;
- `_record_hash_oficial`;
- `rede_meta`;
- `taxa_alfabetizacao_meta`;
- `meta_alfabetizacao_2024`;
- `meta_alfabetizacao_2025`;
- `meta_alfabetizacao_2026`;
- `meta_alfabetizacao_2027`;
- `meta_alfabetizacao_2028`;
- `meta_alfabetizacao_2029`;
- `meta_alfabetizacao_2030`;
- `nivel_alfabetizacao`;
- `percentual_participacao`;
- `_ingestion_timestamp_meta`;
- `_source_table_meta`;
- `_source_system_meta`;
- `_record_hash_meta`.

### 7.2 `uf_metas_gold`

A tabela é formada por `inner join` entre `uf_silver` e `meta_alfabetizacao_uf_silver`, utilizando:

```text
ano + sigla_uf + rede_descricao
```

Após o join, `rede_descricao` é removida. Os campos resultantes registrados pela execução são:

- `ano`;
- `sigla_uf`;
- `serie`;
- `rede_oficial`;
- `taxa_alfabetizacao_oficial`;
- `media_portugues`;
- `proporcao_aluno_nivel_0`;
- `proporcao_aluno_nivel_1`;
- `proporcao_aluno_nivel_2`;
- `proporcao_aluno_nivel_3`;
- `proporcao_aluno_nivel_4`;
- `proporcao_aluno_nivel_5`;
- `proporcao_aluno_nivel_6`;
- `proporcao_aluno_nivel_7`;
- `proporcao_aluno_nivel_8`;
- `_ingestion_timestamp_oficial`;
- `_source_table_oficial`;
- `_source_system_oficial`;
- `_record_hash_oficial`;
- `rede_meta`;
- `taxa_alfabetizacao_meta`;
- `meta_alfabetizacao_2024`;
- `meta_alfabetizacao_2025`;
- `meta_alfabetizacao_2026`;
- `meta_alfabetizacao_2027`;
- `meta_alfabetizacao_2028`;
- `meta_alfabetizacao_2029`;
- `meta_alfabetizacao_2030`;
- `percentual_participacao`;
- `_ingestion_timestamp_meta`;
- `_source_table_meta`;
- `_source_system_meta`;
- `_record_hash_meta`.

### 7.3 `alunos_gold`

A tabela mantém os campos de `alunos_silver` e adiciona descrições do dicionário. Os campos resultantes registrados pela execução são:

- `ano`;
- `id_municipio`;
- `id_escola`;
- `id_aluno`;
- `caderno`;
- `serie`;
- `rede`;
- `presenca`;
- `preenchimento_caderno`;
- `alfabetizado`;
- `proficiencia`;
- `peso_aluno`;
- `_ingestion_timestamp`;
- `_source_table`;
- `_source_system`;
- `_record_hash`;
- `alfabetizado_descricao`;
- `preenchimento_caderno_descricao`;
- `presenca_descricao`;
- `rede_descricao`;
- `serie_descricao`.

### 7.4 Campos analíticos registrados nos testes e relatórios

Os artefatos técnicos do projeto também registram os seguintes campos utilizados nas análises e validações:

- `rede_chave`;
- `meta_do_ano`;
- `ano_pactuacao_meta`;
- `status_meta`;
- `motivo_sem_meta`;
- `gap_meta_do_ano_pp`;
- `distancia_meta_pactuada_2030_pp`;
- `distancia_cenario_aspiracional_2030_pp`;
- `n_alunos_avaliados`;
- `soma_peso_avaliados`;
- `soma_peso_alfabetizados`;
- `taxa_microdado`;
- `alvo_taxa_alfabetizacao`;
- `media_portugues_ano_anterior`.

## 8. Regras de Negócio

### RN01 — Critério de alfabetização

O projeto adota o ponto de corte de **743 pontos** de proficiência como referência conceitual para considerar o estudante alfabetizado. A base também possui o campo `alfabetizado`; os relatórios técnicos registram a validação cruzada entre esse campo e `proficiencia >= 743`.

### RN02 — Cálculo ponderado do indicador

A taxa recalculada a partir do microdado utiliza peso amostral:

```text
taxa = soma(peso dos alfabetizados) / soma(peso dos avaliados) * 100
```

Os campos intermediários utilizados no cálculo são:

- `n_alunos_avaliados`;
- `soma_peso_avaliados`;
- `soma_peso_alfabetizados`;
- `taxa_microdado`.

### RN03 — Universo de alunos avaliados

O denominador considera alunos avaliados, isto é, registros compatíveis com presença, caderno preenchido e proficiência/peso válidos. Ausentes não devem ser incluídos no denominador do indicador.

### RN04 — Nulo não é zero

Valores nulos de proficiência, taxa ou meta são preservados. Não se utiliza `fillna(0)` para transformar ausência de informação em valor numérico zero.

### RN05 — Tradução de códigos

Códigos categóricos são traduzidos usando a tabela oficial `dicionario`. Valores não reconhecidos devem ser identificados como desconhecidos e submetidos a check de qualidade; não devem ser silenciosamente convertidos em uma categoria válida.

### RN06 — Normalização da rede

O pipeline cria uma chave semântica `rede_chave` para alinhar os códigos das tabelas de resultado com as descrições das tabelas de meta. A rede `Pública` das metas é tratada como o agrupamento estadual + municipal conforme a validação registrada no projeto.

### RN07 — Comparação com meta

Resultado e meta só podem ser comparados no mesmo recorte de localidade, rede e ano aplicável.

Status previstos:

- `atingiu`;
- `não atingiu`;
- `sem resultado`;
- `sem meta cadastrada`.

A ausência de meta não pode ser classificada como `não atingiu`.

### RN08 — Trajetória de meta

Quando existe linha de pactuação para o próprio ano, utiliza-se a meta dessa linha. Para um ano posterior à última pactuação disponível, o pipeline utiliza a trajetória da pactuação mais recente e registra o ano em `ano_pactuacao_meta`. Anos anteriores ao início das pactuações permanecem sem meta.

### RN09 — Meta 2030 x cenário aspiracional

São conceitos diferentes:

- meta pactuada de 2030: valor oficial da fonte (`80%` para a rede pública);
- cenário aspiracional: `100%`.

As distâncias são mantidas em campos diferentes para evitar mistura semântica.

### RN10 — Streaming simulado

Todo registro sintético deve permanecer identificável com `origem = 'simulado'`. O simulador não pode se misturar aos indicadores oficiais sem possibilidade de separação.

### RN11 — Deduplicação do streaming

`event_id` é a chave lógica de deduplicação. O consumer combina:

1. controle em memória;
2. controle de mensagens pendentes;
3. consulta ao BigQuery por lote, considerando eventos persistidos nos últimos 60 dias.

O lote só é confirmado (`ack`) após persistência bem-sucedida ou confirmação de que o ID já existe.

### RN12 — Micro-lotes

O consumer envia o lote quando ocorre primeiro:

- `STREAMING_BATCH_SIZE` eventos — padrão 10;
- `STREAMING_BATCH_MAX_WAIT_SECONDS` — padrão 10 segundos.

### RN13 — Latência do streaming

```text
latencia_segundos = ingested_at - produced_at
```

Os timestamps são criados em componentes diferentes: producer e consumer.

### RN14 — Features da Fase 3

A base de features deve usar apenas informações conhecidas antes do alvo. A regra `sem_vazamento_de_alvo` é bloqueante. Os testes e relatórios registram `alvo_taxa_alfabetizacao` e features do ano anterior, como `media_portugues_ano_anterior`.

## 9. Regras de Qualidade

O módulo `qualidade/verificador.py` implementa dois tipos de regra:

- **bloqueante:** falha levanta `FalhaQualidadeBloqueante` e interrompe a execução;
- **informativa:** registra a ocorrência, mas não impede a continuidade.

### 9.1 Regras bloqueantes

- chave nula ou vazia;
- chave de negócio duplicada;
- `id_municipio` fora do padrão de 7 dígitos;
- UF fora do domínio esperado;
- taxa/meta fora de 0–100;
- códigos categóricos desconhecidos em regras críticas;
- tipos incompatíveis nas chaves de joins;
- taxa de correspondência abaixo do limiar nos joins críticos;
- vazamento de alvo na base de features.

### 9.2 Regras informativas

- percentual de nulos;
- duplicatas exatas removidas;
- cobertura de microdados;
- joins não críticos;
- convergência entre microdado recalculado e indicador oficial;
- correlação máxima entre feature e alvo.

### 9.3 Correspondência de joins

As validações registram a regra `taxa_correspondencia_join:<nome_do_join>` depois das junções. A finalidade é detectar joins que executam sem erro técnico, mas deixam registros sem par.

Entre os relacionamentos monitorados estão:

- município × meta municipal;
- município × pesos dos alunos;
- UF × meta UF;
- UF × pesos dos alunos;
- Brasil × agregado das UFs;
- Brasil × microdado;
- UF × microdado;
- features × ano anterior;
- features × UF do ano anterior.

## 10. Relacionamentos e Linhagem

### 10.1 Fluxo batch

```text
Base dos Dados
    ↓
Bronze (cópia bruta + metadados)
    ↓
Silver (tipagem, limpeza e validação)
    ↓
Gold (regras de negócio, agregações e joins)
```

### 10.2 Relacionamentos principais

| Origem                         | Destino / uso                   | Chaves principais                               |
| ------------------------------ | ------------------------------- | ----------------------------------------------- |
| `municipio`                    | resultado municipal × meta      | `ano`, `id_municipio`, rede normalizada         |
| `meta_alfabetizacao_municipio` | resultado municipal × meta      | `ano`, `id_municipio`, rede normalizada         |
| `uf`                           | resultado estadual × meta       | `ano`, `sigla_uf`, rede normalizada             |
| `meta_alfabetizacao_uf`        | resultado estadual × meta       | `ano`, `sigla_uf`, rede normalizada             |
| `meta_alfabetizacao_brasil`    | comparação nacional             | `ano`, rede normalizada                         |
| `alunos`                       | recálculo ponderado e validação | ano/localidade/rede conforme nível de agregação |
| `dicionario`                   | tradução das categorias         | `id_tabela`, `nome_coluna`, `chave`             |
| `streaming_eventos_validos`    | resultado municipal simulado    | `ano`, `id_municipio`, `serie`, `rede`          |

### 10.3 Fluxo streaming

```text
Producer
  ↓
Pub/Sub
  ↓
Consumer
  ├─ inválido → eventos_invalidos
  └─ válido → deduplicação → eventos_validos_particionado
                              ↓
                    exportar_silver.py
                              ↓
                  streaming_eventos_validos
                              ↓
                   Gold municipal (simulado)
```

## 11. Política de Escrita e Persistência

| Camada            | Política                                                    |
| ----------------- | ----------------------------------------------------------- |
| Bronze local      | um Parquet por carga                                        |
| Bronze BigQuery   | `WRITE_APPEND`                                              |
| Silver local      | arquivo derivado substituível                               |
| Silver BigQuery   | `WRITE_TRUNCATE`                                            |
| Gold BigQuery     | `WRITE_TRUNCATE`, após validação/publicação                 |
| Quality reports   | CSV acumulado local; espelho completo reenviado ao BigQuery |
| Streaming válidos | cargas incrementais por micro-lote                          |

A Bronze é a camada que preserva o histórico de ingestões. Silver e Gold são reconstruíveis a partir das camadas anteriores.

## 12. Variáveis Técnicas Relevantes

### Streaming

- `GCP_PROJECT_ID`;
- `PUBSUB_TOPIC_ID`;
- `PUBSUB_SUBSCRIPTION_ID`;
- `BIGQUERY_DATASET_ID`;
- `BIGQUERY_TABLE_VALIDOS_ID`;
- `BIGQUERY_TABLE_INVALIDOS_ID`;
- `BIGQUERY_TABLE_METRICAS_ID`;
- `STREAMING_INTERVAL_SECONDS`;
- `STREAMING_MAX_TEST_EVENTS`;
- `STREAMING_TEST_DETERMINISTIC`;
- `STREAMING_ANO`;
- `STREAMING_MUNICIPIOS`;
- `STREAMING_BATCH_SIZE`;
- `STREAMING_BATCH_MAX_WAIT_SECONDS`;
- `BIGQUERY_DEDUP_MAX_BYTES_BILLED`;
- `BIGQUERY_MAX_BYTES_BILLED`.

### Data Lake / BigQuery

- `LAKE_BIGQUERY`;
- `BQ_DATASET_PREFIXO`;
- `BQ_LOCATION`;
- `GCP_PROJECT_ID`.

### Alertas

As credenciais e destinatários do Gmail são obtidos por variáveis de ambiente e não devem ser versionados.
