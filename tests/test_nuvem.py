from nuvem import lake


def test_sem_lake_configurado_nao_envia(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKE_BIGQUERY", "false")
    arquivo = tmp_path / "x.csv"
    arquivo.write_text("a\n1\n", encoding="utf-8")

    assert lake.lake_configurado() is False
    assert lake.enviar_para_lake([arquivo]) == 0


def test_destino_de_cada_camada(monkeypatch):
    monkeypatch.setenv("BQ_DATASET_PREFIXO", "alfabetizacao")
    dl = lake.DATA_LAKE

    assert lake.destino(dl / "bronze" / "municipio_raw_20261101_120000.parquet") == \
        ("alfabetizacao_bronze", "municipio_raw", True)
    assert lake.destino(dl / "silver" / "meta_alfabetizacao_uf_silver.parquet") == \
        ("alfabetizacao_silver", "meta_alfabetizacao_uf", False)
    assert lake.destino(dl / "silver" / "streaming_eventos_validos.parquet") == \
        ("alfabetizacao_silver", "streaming_eventos_validos", False)
    assert lake.destino(dl / "gold" / "gold_features_fase3.parquet") == \
        ("alfabetizacao_gold", "gold_features_fase3", False)
    assert lake.destino(dl / "quality_reports" / "execution_log.csv") == \
        ("alfabetizacao_monitoramento", "execution_log", False)
