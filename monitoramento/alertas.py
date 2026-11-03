import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from dotenv import load_dotenv

# Localiza o arquivo .env na raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
ARQUIVO_ENV = BASE_DIR / ".env"


def _configuracao_gmail():
    # Lê o .env a cada alerta: um processo filho (ex.: Apêndice A do notebook) herda as variáveis
    # carregadas no início da execução e não veria uma senha corrigida depois
    load_dotenv(ARQUIVO_ENV, override=True)
    return os.getenv("GMAIL_REMETENTE"), os.getenv("GMAIL_DESTINATARIO"), os.getenv("GMAIL_SENHA_APP")


def _login(servidor, remetente, senha_app):
    # Só AUTH PLAIN: se o Gmail recusar a senha (535), o erro real aparece. Com o padrão do smtplib,
    # a tentativa seguinte (AUTH LOGIN) faz o Gmail fechar a conexão e esconde o 535.
    servidor.ehlo()
    servidor.esmtp_features["auth"] = "PLAIN"
    try:
        servidor.login(remetente, senha_app)
    except smtplib.SMTPAuthenticationError as erro:
        raise smtplib.SMTPAuthenticationError(
            erro.smtp_code,
            "Gmail recusou GMAIL_REMETENTE/GMAIL_SENHA_APP: use uma senha de app (16 letras) "
            "gerada na mesma conta do remetente.",
        ) from erro


def enviar_alerta(etapa, erro, run_id=None):
    remetente, destinatario, senha_app = _configuracao_gmail()

    if not all([remetente, destinatario, senha_app]):
        raise ValueError("Configurações do Gmail incompletas no arquivo .env")

    mensagem = EmailMessage()
    mensagem["From"] = remetente
    mensagem["To"] = destinatario
    mensagem["Subject"] = f"[ALERTA TECH CHALLENGE] Falha em {etapa}"

    mensagem.set_content(
        f"Foi identificada uma falha no pipeline.\n\n"
        f"Etapa: {etapa}\n"
        f"Run ID: {run_id}\n"
        f"Erro: {erro}\n\n"
        f"Verifique o arquivo execution_log.csv."
    )

    senha_app = senha_app.replace(" ", "")
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20) as servidor:
            _login(servidor, remetente, senha_app)
            servidor.send_message(mensagem)
    except smtplib.SMTPAuthenticationError:
        raise
    except (smtplib.SMTPServerDisconnected, ConnectionError, TimeoutError, OSError) as erro_465:
        # Porta 465 bloqueada por rede ou antivírus: segunda tentativa pela 587 (STARTTLS)
        print(f"[AVISO] Envio pela porta 465 falhou ({type(erro_465).__name__}); tentando a 587.")
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as servidor:
            servidor.starttls()
            _login(servidor, remetente, senha_app)
            servidor.send_message(mensagem)

    print(f"[ALERTA ENVIADO] Falha na etapa {etapa}")