# Roteiro do vídeo executivo (até 5 minutos)

Público simulado: diretoria de uma organização pública de análise educacional.
Ordem pedida: problema → solução → valor → uso em IA.
**Todos os números abaixo saem de `data_lake/gold/gold_numeros_video.csv` (execução de 01/11/2026). Não arredonde nem troque na narração.**

---

## 1. Problema (0:00 – 0:50)

**Tela:** título do projeto e a frase "743 pontos no Saeb = criança alfabetizada".

**Narração:**
> O Brasil se comprometeu a alfabetizar as crianças até o fim do 2º ano do ensino fundamental. O INEP definiu que uma criança está alfabetizada quando atinge 743 pontos no Saeb, e cada rede pactuou uma meta anual até 2030. Para a rede pública, a meta pactuada para 2030 é de 80%.
>
> O problema é que esses dados estão espalhados em sete tabelas — metas do Brasil, dos estados e dos municípios, resultados, microdados de quase 3,9 milhões de alunos e um dicionário de códigos — e cruzá-los do jeito errado gera números que parecem certos e não são.

---

## 2. Solução (0:50 – 2:20)

**Tela:** o diagrama de arquitetura do README (seção 2).

**Narração:**
> Construímos um pipeline de dados no Google Cloud, com BigQuery, em três camadas.
> A camada Bronze guarda a cópia bruta da fonte, sem apagar as cargas anteriores. A Silver limpa e padroniza. A Gold entrega os indicadores prontos: resultado contra a meta por município, por estado e do Brasil, e a evolução no tempo.
>
> Além da carga histórica, simulamos atualizações chegando em tempo quase real por uma fila de mensagens, o Pub/Sub. Cada evento é validado, duplicatas são descartadas, e o dado simulado chega até a camada final marcado como "simulado", para nunca se misturar com o oficial.
>
> Dado ruim não passa: o pipeline tem regras de qualidade que interrompem a execução antes de publicar. Quando algo falha, a falha fica registrada e um e-mail de alerta chega em menos de um minuto.

**Tela (10 s):** print `evidencias/execucao_final/06_alerta_email_falha_controlada.png`.

> Este é um teste de falha proposital: inserimos uma linha duplicada, o pipeline parou e o alerta chegou.

---

## 3. Valor (2:20 – 3:50)

**Gráfico 1 — "Taxa de alfabetização, Brasil, rede pública" (barras)**
- Eixo X: ano (2023, 2024, 2025). Eixo Y: % de crianças alfabetizadas (0 a 100).
- Barras: **55,9% · 59,2% · 66,0%**. Linha tracejada da meta pactuada: **59,9% (2024) · 64,0% (2025)**; 2023 sem meta (ano de linha de base).

> Em 2024, o Brasil ficou em 59,2%, abaixo da meta de 59,9%. Em 2025, chegou a 66%, acima da meta de 64%. Ainda faltam 14 pontos percentuais para a meta pactuada de 80% em 2030.

**Prova de confiança (sem gráfico, texto na tela):** "Recalculado a partir dos alunos: 59,2% = publicado: 59,2% (2024)"

> E esses números são confiáveis: recalculamos a taxa a partir dos dados de cada aluno, e o resultado de 2024 bate exatamente com o oficial — 59,2%. Em 141 de 144 recortes por estado, a diferença ficou abaixo de meio ponto.

**Gráfico 2 — "Municípios em relação à meta de 2024" (barras horizontais)**
- Eixo X: número de municípios. Eixo Y: situação.
- **Atingiu: 2.788 · Não atingiu: 2.444 · Sem meta cadastrada: 216**.

> Nos municípios, 2.788 atingiram a meta de 2024 e 2.444 não. Separamos um terceiro grupo: 216 municípios sem meta cadastrada — eles não falharam, eles precisam primeiro ter a meta definida.

> A desigualdade é grande: em 2025, a taxa vai de 48% no Rio Grande do Norte a 84% no Ceará.

---

## 4. Uso em inteligência artificial (3:50 – 4:40)

**Tela:** tabela `gold_features_fase3` (algumas colunas).

**Narração:**
> A camada final já está pronta para um modelo preditivo: para cada município, sabemos o resultado do ano anterior, a proficiência média, a participação e a meta do ano. Com isso, dá para prever quais municípios tendem a não atingir a meta e priorizar o apoio antes da próxima avaliação.
>
> Tomamos um cuidado: nenhuma informação do mesmo ano da prova entra como variável, para o modelo não "colar" a resposta. Uma regra do pipeline bloqueia isso automaticamente.

---

## 5. Fechamento (4:40 – 5:00)

> Hoje o pipeline roda com custo zero, dentro da camada gratuita do BigQuery. Projetamos o mesmo desenho em produção, com carga semanal e cem mil eventos por dia, em cerca de 12 dólares por mês.
> Resumo: dados públicos integrados, números verificados, alerta quando algo quebra e uma base pronta para prever onde agir primeiro.

---

## Cuidados

- **Projeção de 90,7% em 2030:** se mencionar, diga "cenário de tendência com apenas três anos de dados, não é previsão".
- **Brasil 2023 (55,9%):** se citar, diga que esse número não fecha com a soma dos estados (57,46%) e que mantivemos o oficial.
- **"Meta de 80%" é a meta pactuada da fonte.** Os 100% (universalização) são cenário aspiracional; não troque um pelo outro.
- **Gráficos:** eixos rotulados com unidade (% ou número de municípios) e ano.
- Gravar com tela + voz; uma ou duas pessoas apresentando. Depois de publicar, colocar o link no topo do `README.md`.
