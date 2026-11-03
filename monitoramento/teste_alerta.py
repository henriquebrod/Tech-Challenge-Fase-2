from monitoramento.registrador import monitorar_etapa

with monitorar_etapa(
    "TESTE_FALHA_EMAIL",
    run_id="TESTE-ALERTA-001"
):
    raise ValueError("Falha simulada para testar o alerta automatico")