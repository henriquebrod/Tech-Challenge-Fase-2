import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from colorama import Fore

# Caminho relativo à pasta principal do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_CHECKS = BASE_DIR / "data_lake" / "quality_reports" / "gold_checks.csv"

TIPOS = ("bloqueante", "informativa")

_config = {"run_id": None, "arquivo": ARQUIVO_CHECKS}


class FalhaQualidadeBloqueante(Exception):
    pass


def configurar_verificador(run_id, arquivo=None):
    # --- id da execução gravado em cada check e arquivo onde os checks acumulam
    _config["run_id"] = run_id
    if arquivo is not None:
        _config["arquivo"] = Path(arquivo)


def registrar_check(regra, tabela, tipo, aprovado, valor=None, limiar=None, detalhe=''):
    # --- grava em csv (append) e para tudo se a bloqueante falhar
    if tipo not in TIPOS:
        raise ValueError(f"tipo de regra inválido: {tipo!r} (use {TIPOS})")

    aprovado = bool(aprovado)
    arquivo = Path(_config["arquivo"])
    registro = pd.DataFrame([{
        'gold_run_ts':   _config["run_id"],
        'regra':         regra,
        'tabela':        tabela,
        'tipo':          tipo,
        'status':        'APROVADO' if aprovado else 'REPROVADO',
        'valor':         valor,
        'limiar':        limiar,
        'detalhe':       detalhe,
        'registrado_em': datetime.now(timezone.utc).isoformat(),
    }])
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    registro.to_csv(arquivo, mode='a', index=False, header=not os.path.exists(arquivo))

    cor = Fore.GREEN if aprovado else (Fore.RED if tipo == 'bloqueante' else Fore.YELLOW)
    print(f"{cor}   [{tipo.upper()}] {regra} ({tabela}): {'OK' if aprovado else 'FALHOU'}"
          f" | valor={valor} limiar={limiar} {detalhe}")
    if not aprovado and tipo == 'bloqueante':
        raise FalhaQualidadeBloqueante(f"{regra} em {tabela}: valor={valor}, limiar={limiar}. {detalhe}")


def checar_chave_nao_nula(df, chaves, tabela):
    # --- nulo, vazio ou nulo convertido em texto ('nan', 'None') não é chave
    n_invalidos = int(df[chaves].isna().any(axis=1).sum())
    for chave in chaves:
        if pd.api.types.is_string_dtype(df[chave]) or df[chave].dtype == object:
            texto = df[chave].dropna().astype(str).str.strip()
            n_invalidos += int(texto.eq('').sum() + texto.str.lower().isin(['nan', 'none', '<na>']).sum())

    registrar_check(f"chave_nula_ou_vazia:{'+'.join(chaves)}", tabela, 'bloqueante',
                    n_invalidos == 0, n_invalidos, 0)


def checar_chave_unica(df, chaves, tabela):
    checar_chave_nao_nula(df, chaves, tabela)

    n_dup = int(df.duplicated(subset=chaves).sum())
    registrar_check(f"chave_unica:{'+'.join(chaves)}", tabela, 'bloqueante', n_dup == 0, n_dup, 0)


def checar_formato(df, coluna, padrao, tabela, tipo='bloqueante'):
    # --- ex.: id_municipio com 7 dígitos em todas as tabelas, senão o join não casa
    texto = df[coluna].astype('string')
    invalidos = texto.isna() | ~texto.str.fullmatch(padrao).fillna(False)
    n_invalidos = int(invalidos.sum())
    exemplos = df.loc[invalidos, coluna].astype(str).unique()[:5].tolist()
    registrar_check(f"formato:{coluna}", tabela, tipo, n_invalidos == 0, n_invalidos, 0,
                    f"padrão {padrao}; exemplos: {exemplos}" if n_invalidos else f"padrão {padrao}")


def checar_dominio(df, coluna, valores_validos, tabela, tipo='bloqueante'):
    invalidos = ~df[coluna].isin(valores_validos)
    n_invalidos = int(invalidos.sum())
    exemplos = df.loc[invalidos, coluna].astype(str).unique()[:10].tolist()
    registrar_check(f"dominio:{coluna}", tabela, tipo, n_invalidos == 0, n_invalidos, 0,
                    f"valores fora do domínio: {exemplos}" if n_invalidos else '')


def checar_intervalo(df, coluna, minimo, maximo, tabela, tipo='bloqueante'):
    # --- nulo não é erro aqui (continua nulo); só valor fora da escala
    valores = pd.to_numeric(df[coluna], errors='coerce')
    fora = valores.notna() & ((valores < minimo) | (valores > maximo))
    n_fora = int(fora.sum())
    registrar_check(f"intervalo:{coluna}", tabela, tipo, n_fora == 0, n_fora, f"[{minimo}, {maximo}]",
                    f"min={valores.min()} max={valores.max()}" if n_fora else '')


def checar_percentual_nulos(df, colunas, tabela):
    # --- só registra: nulo em indicador é informação da fonte, não erro
    for coluna in colunas:
        percentual = round(100 * float(df[coluna].isna().mean()), 2) if len(df) else 0.0
        registrar_check(f"percentual_nulos:{coluna}", tabela, 'informativa', True, percentual, None,
                        f"{int(df[coluna].isna().sum())} nulos em {len(df)} linhas")
