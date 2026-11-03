
import ast
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from qualidade import verificador
from qualidade.verificador import (
    FalhaQualidadeBloqueante,
    checar_chave_nao_nula,
    checar_chave_unica,
    checar_dominio,
    checar_formato,
    checar_intervalo,
    configurar_verificador,
    registrar_check,
)

NOTEBOOK = (
    Path(__file__).resolve().parents[1]
    / "pipeline_alfabetizacao.ipynb"
)


@pytest.fixture
def arquivo_checks(tmp_path):
    """Cada teste grava os checks num CSV próprio."""
    arquivo = tmp_path / "checks.csv"
    configurar_verificador(run_id="TESTE", arquivo=arquivo)
    yield arquivo
    configurar_verificador(run_id=None, arquivo=verificador.ARQUIVO_CHECKS)


def test_bloqueante_reprovada_interrompe_e_fica_registrada(arquivo_checks):
    with pytest.raises(FalhaQualidadeBloqueante):
        registrar_check("regra", "tabela", "bloqueante", False, 1, 0)

    registros = pd.read_csv(arquivo_checks)
    assert registros["status"].tolist() == ["REPROVADO"]
    assert registros["gold_run_ts"].tolist() == ["TESTE"]


def test_informativa_reprovada_nao_interrompe(arquivo_checks):
    registrar_check("regra_a", "tabela", "informativa", False, 1, 0)
    registrar_check("regra_b", "tabela", "informativa", True, 0, 0)

    # --- acumula linhas com um único cabeçalho
    registros = pd.read_csv(arquivo_checks)
    assert registros["regra"].tolist() == ["regra_a", "regra_b"]


def test_tipo_de_regra_invalido(arquivo_checks):
    with pytest.raises(ValueError):
        registrar_check("regra", "tabela", "informativo", True)


def test_chave_unica_reprova_linha_repetida(arquivo_checks):
    df = pd.DataFrame({"ano": [2024, 2024], "id_municipio": ["1100015", "1100015"]})

    with pytest.raises(FalhaQualidadeBloqueante):
        checar_chave_unica(df, ["ano", "id_municipio"], "municipio")


def test_chave_nula_convertida_em_texto(arquivo_checks):
    # --- astype(str) transforma nulo em 'nan'; continua sendo chave inválida
    df = pd.DataFrame({"id_municipio": ["1100015", "nan"]})

    with pytest.raises(FalhaQualidadeBloqueante):
        checar_chave_nao_nula(df, ["id_municipio"], "alunos")


def test_formato_id_municipio(arquivo_checks):
    checar_formato(pd.DataFrame({"id": ["1100015", "5300108"]}), "id", r"\d{7}", "ok")

    with pytest.raises(FalhaQualidadeBloqueante):
        checar_formato(pd.DataFrame({"id": ["1100015", "110001"]}), "id", r"\d{7}", "curto")


def test_dominio_uf(arquivo_checks):
    with pytest.raises(FalhaQualidadeBloqueante):
        checar_dominio(pd.DataFrame({"uf": ["SP", "XX"]}), "uf", {"SP", "RJ"}, "uf")


def test_intervalo_ignora_nulo(arquivo_checks):
    checar_intervalo(pd.DataFrame({"taxa": [0.0, 55.5, np.nan, 100.0]}), "taxa", 0, 100, "ok")

    with pytest.raises(FalhaQualidadeBloqueante):
        checar_intervalo(pd.DataFrame({"taxa": [50.0, 150.0]}), "taxa", 0, 100, "fora")


def carregar_join_medido(limiares):
    """Carrega join_medido do notebook com o verificador real."""
    with NOTEBOOK.open(encoding="utf-8") as arquivo:
        notebook = json.load(arquivo)

    codigo = "\n".join(
        "".join(celula.get("source", []))
        for celula in notebook["cells"]
        if celula["cell_type"] == "code"
    )
    codigo = "\n".join(
        linha for linha in codigo.splitlines()
        if not linha.lstrip().startswith(("!", "%"))
    )
    funcao = [
        no for no in ast.parse(codigo).body
        if isinstance(no, ast.FunctionDef) and no.name == "join_medido"
    ]
    assert len(funcao) == 1

    ambiente = {
        "pd": pd,
        "np": np,
        "registrar_check": registrar_check,
        "LIMIAR_JOIN": limiares,
    }
    exec(compile(ast.Module(body=funcao, type_ignores=[]), str(NOTEBOOK), "exec"), ambiente)
    return ambiente["join_medido"]


def test_join_medido_reprova_abaixo_do_limiar(arquivo_checks):
    join_medido = carregar_join_medido({"teste": 1.0})
    esquerda = pd.DataFrame({"ano": [2024, 2025]})
    direita = pd.DataFrame({"ano": [2024], "peso": [10.0]})

    with pytest.raises(FalhaQualidadeBloqueante):
        join_medido(esquerda, direita, ["ano"], "teste", "tabela", bloqueante=True)


def test_join_medido_mede_so_as_linhas_da_mascara(arquivo_checks):
    join_medido = carregar_join_medido({"teste": 1.0})
    esquerda = pd.DataFrame({"ano": [2024, 2025]})
    direita = pd.DataFrame({"ano": [2024], "peso": [10.0]})

    # --- 2025 não tem par por desenho (sem microdado) e fica fora da taxa
    resultado = join_medido(esquerda, direita, ["ano"], "teste", "tabela",
                            bloqueante=True, medir=esquerda["ano"] == 2024)

    assert len(resultado) == 2
    registros = pd.read_csv(arquivo_checks)
    taxa = registros[registros["regra"] == "taxa_correspondencia_join:teste"]
    assert taxa["valor"].tolist() == [1.0]
    assert taxa["detalhe"].tolist() == ["1/1 linhas com par"]
