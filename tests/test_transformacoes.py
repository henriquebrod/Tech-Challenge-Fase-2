
import ast
import json
import unicodedata
from pathlib import Path
import numpy as np
import pandas as pd
import pytest


# Caminho do notebook original
NOTEBOOK = (
    Path(__file__).resolve().parents[1]
    / "pipeline_alfabetizacao.ipynb"
)


NOMES_PADRAO = {
    "normalizar_texto",
    "chave_rede",
    "extrair_meta_do_ano",
    "classificar_meta",
    "taxa_ponderada_microdado",
}


def carregar_funcoes(nomes=NOMES_PADRAO, extras=None):
    """Carrega apenas as funções necessárias do notebook."""

    with NOTEBOOK.open(encoding="utf-8") as arquivo:
        notebook = json.load(arquivo)

    # Reúne o código das células Python
    codigo = "\n".join(
        "".join(celula.get("source", []))
        for celula in notebook["cells"]
        if celula["cell_type"] == "code"
    )

    # Ignora comandos especiais do Jupyter
    codigo = "\n".join(
        linha
        for linha in codigo.splitlines()
        if not linha.lstrip().startswith(("!", "%"))
    )

    # Localiza as funções desejadas
    arvore = ast.parse(codigo)

    funcoes = [
        no
        for no in arvore.body
        if isinstance(no, ast.FunctionDef)
        and no.name in nomes
    ]

    assert len(funcoes) == len(nomes), (
        "Não foi possível localizar todas as funções "
        "necessárias no notebook."
    )

    # Dependências utilizadas pelas funções
    ambiente = {
        "pd": pd,
        "unicodedata": unicodedata,
        "np": np,
        "CENARIO_ASPIRACIONAL_2030": 100.0,
        "REDE_PUBLICA_DAS_METAS": "publica_estadual_municipal",
        "REDES_AGREGADAS": {
            "publica_estadual_municipal": [
                "estadual",
                "municipal",
            ],
            "publica_federal_estadual_municipal": [
                "federal",
                "estadual",
                "municipal",
            ],
            "total": [
                "federal",
                "estadual",
                "municipal",
                "privada",
            ],
        },
    }
    ambiente.update(extras or {})

    # Executa somente as definições das funções selecionadas
    modulo = ast.Module(
        body=funcoes,
        type_ignores=[],
    )

    exec(
        compile(modulo, str(NOTEBOOK), "exec"),
        ambiente,
    )

    return ambiente


@pytest.mark.parametrize(
    "entrada, esperado",
    [
        ("Municipal", "municipal"),
        ("Estadual", "estadual"),
        ("Federal", "federal"),
        ("Privada", "privada"),
        ("Total", "total"),
        ("Pública", "publica_estadual_municipal"),
        (
            "Pública (Estadual e Municipal)",
            "publica_estadual_municipal",
        ),
        ("Rede inexistente", "desconhecido"),
        (None, "desconhecido"),
    ],
)
def test_chave_rede(entrada, esperado):
    funcoes = carregar_funcoes()
    chave_rede = funcoes["chave_rede"]

    resultado = chave_rede(entrada)

    assert resultado == esperado


def test_extrair_meta_do_ano():
    extrair_meta_do_ano = carregar_funcoes()["extrair_meta_do_ano"]

    # Dados fictícios para validar a transformação
    df = pd.DataFrame({
        "ano": [2023, 2024, 2025, 2024],
        "meta_alfabetizacao_2024": [50.0, 60.0, 70.0, 65.0],
        "meta_alfabetizacao_2025": [55.0, 65.0, 75.0, 80.0],
    })

    resultado = extrair_meta_do_ano(df)

    # 2023 não possui coluna de meta
    assert pd.isna(resultado.iloc[0])

    # 2024 utiliza a meta da coluna de 2024
    assert resultado.iloc[1] == 60.0

    # 2025 utiliza a meta da coluna de 2025
    assert resultado.iloc[2] == 75.0

    # Outra linha de 2024 mantém sua própria meta
    assert resultado.iloc[3] == 65.0


def test_extrair_meta_ano_ausente():
    extrair_meta_do_ano = carregar_funcoes()["extrair_meta_do_ano"]

    df = pd.DataFrame({
        "ano": [2023, 2026],
        "meta_alfabetizacao_2024": [50.0, 60.0],
    })

    resultado = extrair_meta_do_ano(df)

    assert resultado.isna().all()


def test_classificar_meta_status():
    classificar_meta = carregar_funcoes()["classificar_meta"]

    df = pd.DataFrame({
        "ano": [2024, 2024, 2024, 2023],
        "taxa_alfabetizacao": [70.0, 55.0, np.nan, 60.0],
        "meta_do_ano": [65.0, 65.0, 65.0, np.nan],
        "meta_alfabetizacao_2030": [80.0, 80.0, 80.0, 80.0],
    })

    tem_linha_meta = np.array([True, True, True, True])
    ano_coberto = np.array([True, True, True, False])

    resultado = classificar_meta(
        df.copy(), tem_linha_meta, ano_coberto
    )

    assert resultado["status_meta"].tolist() == [
        "atingiu",
        "não atingiu",
        "sem resultado",
        "sem meta cadastrada",
    ]

    assert resultado.loc[3, "motivo_sem_meta"] == (
        "ano sem tabela de meta publicada"
    )


def test_classificar_meta_calculos():
    classificar_meta = carregar_funcoes()["classificar_meta"]

    df = pd.DataFrame({
        "ano": [2024],
        "taxa_alfabetizacao": [70.0],
        "meta_do_ano": [65.0],
        "meta_alfabetizacao_2030": [80.0],
    })

    resultado = classificar_meta(
        df.copy(),
        np.array([True]),
        np.array([True]),
    )

    assert resultado.loc[0, "gap_meta_do_ano_pp"] == 5.0

    assert (
        resultado.loc[0, "distancia_meta_pactuada_2030_pp"]
        == 10.0
    )

    assert (
        resultado.loc[0, "distancia_cenario_aspiracional_2030_pp"]
        == 30.0
    )


def test_resultado_vs_meta_ano_posterior_usa_trajetoria_mais_recente():
    checks = []
    funcoes = carregar_funcoes(
        {"join_medido", "extrair_meta_do_ano", "classificar_meta", "montar_resultado_vs_meta"},
        {
            "LIMIAR_JOIN": {"municipio_x_meta_municipio": 0.90},
            "registrar_check": lambda *args, **kwargs: checks.append(args),
        },
    )

    # Pactuações de 2023 e 2024; a de 2024 é a mais recente
    meta = pd.DataFrame({
        "ano": [2023, 2024],
        "id_municipio": ["A", "A"],
        "rede_chave": ["municipal", "municipal"],
        "meta_alfabetizacao_2024": [50.0, 60.0],
        "meta_alfabetizacao_2026": [70.0, 79.0],
        "meta_alfabetizacao_2030": [80.0, 80.0],
    })
    resultado = pd.DataFrame({
        "ano": [2024, 2026, 2026, 2022],
        "id_municipio": ["A", "A", "B", "A"],
        "rede_chave": ["municipal"] * 4,
        "taxa_alfabetizacao": [65.0, 61.72, 90.0, 40.0],
    })

    df = funcoes["montar_resultado_vs_meta"](
        resultado, meta, ["id_municipio"], "municipio_x_meta_municipio", "teste", bloqueante=False
    ).set_index(["ano", "id_municipio"])

    # Ano com linha de pactuação: meta daquela linha
    assert df.loc[(2024, "A"), "meta_do_ano"] == 60.0
    assert df.loc[(2024, "A"), "status_meta"] == "atingiu"

    # Ano posterior à última pactuação: trajetória de 2024
    assert df.loc[(2026, "A"), "meta_do_ano"] == 79.0
    assert df.loc[(2026, "A"), "ano_pactuacao_meta"] == 2024
    assert df.loc[(2026, "A"), "status_meta"] == "não atingiu"

    # Localidade sem meta continua sem meta (não vira "não atingiu")
    assert df.loc[(2026, "B"), "status_meta"] == "sem meta cadastrada"
    assert df.loc[(2026, "B"), "motivo_sem_meta"] == "localidade sem linha na tabela de meta"

    # Ano anterior às pactuações continua sem meta
    assert df.loc[(2022, "A"), "status_meta"] == "sem meta cadastrada"
    assert df.loc[(2022, "A"), "motivo_sem_meta"] == "ano sem tabela de meta publicada"

    # O check informativo aponta o ano sem meta e registra a trajetória usada
    check_anos = next(c for c in checks if c[0].startswith("anos_resultado_sem_tabela_meta"))
    assert check_anos[3] is False
    assert "2026" in check_anos[6] and "2024" in check_anos[6]


def test_taxa_ponderada_microdado():
    taxa_ponderada = carregar_funcoes()["taxa_ponderada_microdado"]

    # Dois alunos com pesos diferentes:
    # alfabetizado = peso 3; não alfabetizado = peso 1
    df = pd.DataFrame({
        "municipio": ["A", "A"],
        "peso_aluno": [3.0, 1.0],
        "_alfabetizado": [1, 0],
    })

    resultado = taxa_ponderada(df, ["municipio"])

    assert resultado.loc[0, "n_alunos_avaliados"] == 2
    assert resultado.loc[0, "soma_peso_avaliados"] == 4.0
    assert resultado.loc[0, "soma_peso_alfabetizados"] == 3.0
    assert resultado.loc[0, "taxa_microdado"] == 75.0


def test_taxa_ponderada_por_municipio():
    taxa_ponderada = carregar_funcoes()["taxa_ponderada_microdado"]

    df = pd.DataFrame({
        "municipio": ["A", "A", "B", "B"],
        "peso_aluno": [1.0, 1.0, 2.0, 2.0],
        "_alfabetizado": [1, 0, 1, 1],
    })

    resultado = taxa_ponderada(df, ["municipio"])
    resultado = resultado.set_index("municipio")

    assert resultado.loc["A", "taxa_microdado"] == 50.0
    assert resultado.loc["B", "taxa_microdado"] == 100.0

    assert resultado.loc["A", "n_alunos_avaliados"] == 2
    assert resultado.loc["B", "n_alunos_avaliados"] == 2


def test_taxa_ponderada_nenhum_alfabetizado():
    taxa_ponderada = carregar_funcoes()["taxa_ponderada_microdado"]

    df = pd.DataFrame({
        "municipio": ["A", "A"],
        "peso_aluno": [2.0, 3.0],
        "_alfabetizado": [0, 0],
    })

    resultado = taxa_ponderada(df, ["municipio"])

    assert resultado.loc[0, "soma_peso_alfabetizados"] == 0.0
    assert resultado.loc[0, "taxa_microdado"] == 0.0
