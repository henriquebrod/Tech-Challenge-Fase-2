
# Monitoramento e Alertas — Tech Challenge Fase 2

## 1. Objetivo

Monitorar a execução das etapas do pipeline de dados, registrar informações operacionais e enviar alertas automáticos em caso de falha.

## 2. Registro das execuções

O módulo `registrador.py` registra as execuções no arquivo:

`data_lake/quality_reports/execution_log.csv`

Os campos registrados são:

- `run_id`: identificador da execução.
- `etapa`: nome da etapa executada.
- `inicio` e `fim`: horários da execução.
- `duracao_segundos`: tempo de processamento.
- `linhas_entrada` e `linhas_saida`: quantidade de registros.
- `bytes_lidos`: volume estimado ou medido de dados, conforme a etapa.
- `status`: SUCESSO ou FALHA.
- `erro`: descrição do erro, quando houver.

## 3. Detecção de falhas

As etapas são executadas utilizando `monitorar_etapa`.

Quando ocorre uma exceção:

1. A etapa é identificada como FALHA.
2. O erro é registrado no arquivo CSV.
3. O envio de um alerta por e-mail é solicitado.
4. A exceção é propagada, interrompendo a execução não tratada.

## 4. Alertas automáticos

O módulo `alertas.py` utiliza o servidor SMTP do Gmail.

As credenciais são carregadas do arquivo `.env`, que não deve ser publicado no GitHub.

O alerta contém:

- Nome da etapa.
- Identificador da execução.
- Descrição do erro.

O envio é tentado imediatamente após o registro da falha, mas o tempo de recebimento depende do serviço de e-mail e da conexão. No teste de 26/10/2026, a falha foi gravada às 03:36:12 UTC e o e-mail chegou às 00:36 no horário de Brasília (UTC-3), ou seja, em menos de um minuto (resolução do print).

## 5. Evidências

As capturas do teste estão disponíveis na pasta `evidencias/monitoramento_2026-10`:

- `01_falha_pipeline.png`: falha e envio do alerta no terminal.
- `02_registro_falha_csv.png`: registro da falha no CSV.
- `03_email_alerta_recebido.png`: recebimento do alerta no Gmail.

A falha registrada em 26/10/2026 (`TESTE_FALHA_EMAIL`) aparece nos prints, mas não está mais no `execution_log.csv` atual: o arquivo foi recriado depois. O log só deve crescer; não apague nem recrie o arquivo.

## 6. Testes de falha

Falha de uma regra bloqueante de qualidade (é a evidência pedida pelo enunciado):

```powershell
python -m qualidade.simular_falha
```

Roda a regra de chave única sobre uma cópia da Silver de município com uma linha repetida. O check fica `REPROVADO` em `gold_checks.csv`, a etapa fica `FALHA` no `execution_log.csv`, o alerta é enviado e o processo termina com código 1. A Silver não é alterada.

Teste só do canal de alerta:

```powershell
python -m monitoramento.teste_alerta
```

O teste provoca intencionalmente uma exceção `ValueError`. Portanto, a mensagem de erro no terminal é esperada.

## 7. Observação sobre bytes_lidos

Nas etapas da Bronze (`bronze_*_2_2`), a métrica é o total de bytes processados pela consulta no BigQuery (medido pelo próprio job, depois de um dry run).

Nas etapas da Silver (`silver_*_3_x`), é o tamanho do arquivo Bronze da carga.

Nas etapas 4.2 a 4.4, a métrica corresponde ao tamanho dos arquivos Parquet diretamente lidos.

Nas etapas 4.5 a 4.9, a métrica representa uma estimativa do tamanho dos DataFrames em memória, não o volume físico efetivamente lido do armazenamento. Pode haver dupla contagem de dados compartilhados entre operações.

Por isso, esses valores não devem ser comparados diretamente como medidas equivalentes de leitura física.
