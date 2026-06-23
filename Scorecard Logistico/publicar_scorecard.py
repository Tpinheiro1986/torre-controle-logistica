#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
publicar_scorecard.py
=====================

Publica o painel **Scorecard Logístico** na Torre de Controle, seguindo o
mesmo padrão dos demais painéis (OTD, Custo de Servir, etc.):

  • Os dados ficam no Supabase Storage, bucket "dashboards", no caminho
    "scorecard/dados.json"  (espelhando "otd/dados.json").
  • A autenticação usa o usuário administrador (mesmo modelo do app web,
    que faz o upload autenticado como admin) — nada de chave secreta no código.

O que o script faz:
  1. Extrai a constante `const DATA = {...}` de `scorecard/index.html`
     (fonte única de verdade — o mesmo dado que o painel embarca offline).
  2. Grava `scorecard/dados.json` ao lado do HTML.
  3. Autentica no Supabase (e-mail + senha do admin) e faz UPSERT do JSON
     em dashboards/scorecard/dados.json (cache desativado, como nos outros).
  4. (Opcional, com --git) versiona os arquivos estáticos no GitHub
     — index.html (capa) e scorecard/index.html + dados.json — e faz push,
     que é o que publica o site no GitHub Pages.

------------------------------------------------------------------------
Pré-requisitos
------------------------------------------------------------------------
  pip install requests python-dotenv

Configuração (crie um arquivo .env nesta pasta — veja .env.example):
  SUPABASE_URL=https://ennsbpibfnuwlvtodukg.supabase.co
  SUPABASE_ANON_KEY=sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONyI5
  SCORECARD_ADMIN_EMAIL=thiago_balao@yahoo.com.br
  SCORECARD_ADMIN_PASSWORD=********           # senha do admin (NUNCA versione)

------------------------------------------------------------------------
Uso
------------------------------------------------------------------------
  # 1) só publicar os dados no Supabase:
  python publicar_scorecard.py

  # 2) publicar os dados E versionar a capa + scorecard no GitHub:
  python publicar_scorecard.py --git -m "feat: publica Scorecard Logistico"

  # 3) apenas gerar o dados.json local, sem subir nada:
  python publicar_scorecard.py --apenas-json
"""

import os
import re
import sys
import json
import getpass
import argparse
import subprocess
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Falta a dependência 'requests'. Rode:  pip install requests python-dotenv")

# ---------------------------------------------------------------------------
# Caminhos (definidos antes de tudo, pois o .env é lido a partir desta pasta)
# ---------------------------------------------------------------------------
HERE      = Path(__file__).resolve().parent             # .../scorecard
HTML_PATH = HERE / "index.html"                         # scorecard/index.html
JSON_PATH = HERE / "dados.json"                          # scorecard/dados.json
CAPA_PATH = HERE.parent / "index.html"                  # capa (raiz do repo)
ENV_PATH  = HERE / ".env"


def carregar_env():
    """Carrega o .env desta pasta para os.environ, SEM nunca derrubar o script.
    Funciona mesmo sem o pacote python-dotenv (parser próprio embutido)."""
    # 1) usa python-dotenv se estiver instalado (best-effort)
    try:
        from dotenv import load_dotenv
        load_dotenv(ENV_PATH)
    except Exception:
        pass
    # 2) parser próprio — robusto a .env ausente, pasta ou bloqueado
    try:
        if not ENV_PATH.exists():
            return
        if not ENV_PATH.is_file():
            print("  …  Aviso: '.env' existe mas não é um arquivo (parece uma pasta).")
            print("  …  Remova esse item e crie um arquivo de texto chamado .env")
            return
        texto = ENV_PATH.read_text(encoding="utf-8-sig")
    except PermissionError:
        print("  …  Aviso: sem permissão para ler o .env (pode estar bloqueado pelo")
        print("  …  OneDrive/antivírus). Vou pedir a senha na hora, ou use:")
        print("  …      set SCORECARD_ADMIN_PASSWORD=suasenha   (antes de rodar)")
        return
    except Exception as e:
        print(f"  …  Aviso: não consegui ler o .env ({e.__class__.__name__}). Seguindo sem ele.")
        return
    for raw in texto.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        chave, valor = line.split("=", 1)
        chave = chave.strip()
        valor = valor.strip().strip('"').strip("'")
        if chave and not os.environ.get(chave):
            os.environ[chave] = valor


carregar_env()

# ---------------------------------------------------------------------------
# Configuração (com defaults iguais aos do app; senha sempre vem do ambiente)
# ---------------------------------------------------------------------------
SUPABASE_URL   = os.environ.get("SUPABASE_URL",  "https://ennsbpibfnuwlvtodukg.supabase.co").rstrip("/")
SUPABASE_ANON  = os.environ.get("SUPABASE_ANON_KEY", "sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONyI5")
ADMIN_EMAIL    = os.environ.get("SCORECARD_ADMIN_EMAIL", "thiago_balao@yahoo.com.br")
ADMIN_PASSWORD = os.environ.get("SCORECARD_ADMIN_PASSWORD", "")

BUCKET = "dashboards"
DEST   = "scorecard/dados.json"          # mesmo padrão de "otd/dados.json"


# ---------------------------------------------------------------------------
# Utilidades de log
# ---------------------------------------------------------------------------
def info(msg): print(f"  …  {msg}")
def ok(msg):   print(f"  ✓  {msg}")
def erro(msg): print(f"  ✗  {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# 1) Extrair DATA do HTML (balanceamento de chaves — robusto a JSON aninhado)
# ---------------------------------------------------------------------------
class DadosNaoEncontrados(Exception):
    pass


def extrair_data(html_path: Path) -> dict:
    if not html_path.exists():
        raise DadosNaoEncontrados(f"HTML não encontrado: {html_path}")
    # utf-8-sig remove BOM se houver
    html = html_path.read_text(encoding="utf-8-sig")

    # Aceita: const/let/var/window.  +  DATA/DADOS  +  espaços livres  +  {
    m = re.search(r'(?:const|let|var|window\.)\s*(?:DATA|DADOS)\s*=\s*\{', html)
    if not m:
        m = re.search(r'\b(?:DATA|DADOS)\s*=\s*\{', html)  # último recurso
    if not m:
        raise DadosNaoEncontrados("marcador de dados não localizado no HTML")

    inicio = m.end() - 1  # posição do '{'
    profundidade = 0
    fim = -1
    for pos in range(inicio, len(html)):
        c = html[pos]
        if c == "{":
            profundidade += 1
        elif c == "}":
            profundidade -= 1
            if profundidade == 0:
                fim = pos
                break
    if fim < 0:
        raise DadosNaoEncontrados("objeto de dados com chaves desbalanceadas")

    try:
        data = json.loads(html[inicio:fim + 1])
    except json.JSONDecodeError as e:
        raise DadosNaoEncontrados(f"objeto de dados não é JSON válido: {e}")

    if not isinstance(data, dict) or not data:
        raise DadosNaoEncontrados("objeto de dados vazio/inesperado")
    return data


def obter_dados():
    """Tenta o HTML (fonte única); se falhar, usa o dados.json existente.
    Retorna (data: dict, fonte: str)."""
    try:
        data = extrair_data(HTML_PATH)
        ok(f"{len(data)} indicadores extraídos de {HTML_PATH.name}.")
        return data, "html"
    except DadosNaoEncontrados as e:
        info(f"Não usei o HTML ({e}).")
        if JSON_PATH.exists():
            try:
                data = json.loads(JSON_PATH.read_text(encoding="utf-8-sig"))
                if isinstance(data, dict) and data:
                    ok(f"Usando {JSON_PATH.name} existente ({len(data)} indicadores).")
                    return data, "json"
            except json.JSONDecodeError as je:
                erro(f"{JSON_PATH.name} existe mas é inválido: {je}")
        # diagnóstico final
        tam = HTML_PATH.stat().st_size if HTML_PATH.exists() else 0
        tem = ("DATA" in HTML_PATH.read_text(encoding="utf-8-sig", errors="ignore")) if HTML_PATH.exists() else False
        erro("Não foi possível obter os dados para publicar.")
        info(f"HTML lido: {HTML_PATH} ({tam} bytes) · contém 'DATA'? {tem}")
        info(f"E não há um {JSON_PATH.name} válido nesta pasta.")
        info("Garanta que scorecard/index.html é o painel gerado (com 'const DATA = {…}')")
        info("ou coloque um dados.json válido ao lado do script.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# 2) Autenticação no Supabase (usuário admin) -> access_token
# ---------------------------------------------------------------------------
def obter_senha() -> str:
    """Resolve a senha do admin: .env/ambiente e, se faltar, pergunta na hora."""
    pwd = ADMIN_PASSWORD or os.environ.get("SCORECARD_ADMIN_PASSWORD", "")
    if pwd:
        return pwd
    try:
        if sys.stdin and sys.stdin.isatty():
            info("Senha não encontrada no .env/ambiente — informe abaixo (não aparece na tela):")
            try:
                pwd = getpass.getpass(f"  Senha do Supabase de {ADMIN_EMAIL}: ")
            except Exception:
                pwd = input(f"  Senha do Supabase de {ADMIN_EMAIL}: ")
            return (pwd or "").strip()
    except Exception:
        pass
    return ""


def autenticar() -> str:
    pwd = obter_senha()
    if not pwd:
        erro("Sem senha do admin — não dá para autenticar no Supabase.")
        if ENV_PATH.is_file():
            info(f".env encontrado em: {ENV_PATH}")
            info("Verifique a linha (sem aspas, sem espaços, não em branco):")
            info("    SCORECARD_ADMIN_PASSWORD=suasenha")
            info("Dica Windows: confirme que o arquivo é '.env' e não '.env.txt'.")
        else:
            info("Alternativa rápida (CMD, antes de rodar o .bat, na mesma janela):")
            info("    set SCORECARD_ADMIN_PASSWORD=suasenha")
        sys.exit(1)
    url = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"
    r = requests.post(
        url,
        headers={"apikey": SUPABASE_ANON, "Content-Type": "application/json"},
        json={"email": ADMIN_EMAIL, "password": pwd},
        timeout=30,
    )
    if r.status_code != 200:
        erro(f"Falha no login ({r.status_code}): {r.text[:200]}")
        sys.exit(1)
    token = r.json().get("access_token")
    if not token:
        sys.exit("Login sem access_token — verifique e-mail/senha do admin.")
    ok(f"Autenticado como {ADMIN_EMAIL}")
    return token


# ---------------------------------------------------------------------------
# 3) Upload (UPSERT) do JSON no Storage
# ---------------------------------------------------------------------------
def publicar_no_supabase(token: str, conteudo: bytes):
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{DEST}"
    headers = {
        "apikey": SUPABASE_ANON,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-upsert": "true",          # cria ou substitui (mesmo comportamento do app)
        "cache-control": "0",
    }
    r = requests.post(url, headers=headers, data=conteudo, timeout=60)
    if r.status_code in (200, 201):
        ok(f"Publicado em {BUCKET}/{DEST} ({len(conteudo)/1024:.1f} KB)")
        return
    # fallback: alguns ambientes exigem PUT para objeto já existente
    r2 = requests.put(url, headers=headers, data=conteudo, timeout=60)
    if r2.status_code in (200, 201):
        ok(f"Publicado (PUT) em {BUCKET}/{DEST} ({len(conteudo)/1024:.1f} KB)")
        return
    erro(f"Upload falhou. POST={r.status_code} {r.text[:150]} | PUT={r2.status_code} {r2.text[:150]}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# 4) (Opcional) versionar no GitHub
# ---------------------------------------------------------------------------
def publicar_no_github(mensagem: str, tudo: bool = False):
    repo = HERE.parent  # raiz do repositório (onde fica a capa index.html)
    try:
        if tudo:
            # publica TODAS as alterações do repositório (capa, scorecard, etc.)
            subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
        else:
            arquivos = [str(p) for p in (CAPA_PATH, HTML_PATH, JSON_PATH) if p.exists()]
            subprocess.run(["git", "-C", str(repo), "add", *arquivos], check=True)
        # se não houver mudanças, o commit falha — tratamos como aviso
        res = subprocess.run(["git", "-C", str(repo), "commit", "-m", mensagem])
        if res.returncode != 0:
            info("Nada novo para commitar (ou commit recusado).")
        subprocess.run(["git", "-C", str(repo), "push"], check=True)
        ok("Alterações enviadas ao GitHub (push concluído).")
        info("O GitHub Pages publica o site automaticamente após o push.")
    except FileNotFoundError:
        erro("Git não encontrado no PATH. Instale o Git ou rode sem --git.")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        erro(f"Comando git falhou: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Publica o Scorecard Logístico (dados no Supabase + opcional GitHub).")
    ap.add_argument("--apenas-json", action="store_true", help="Só gera o dados.json local, sem publicar.")
    ap.add_argument("--git", action="store_true", help="Versiona capa + scorecard no GitHub e faz push.")
    ap.add_argument("--git-all", action="store_true", help="Igual a --git, mas comita TODAS as mudanças do repo (git add -A).")
    ap.add_argument("-m", "--mensagem", default="chore: publica dados do Scorecard Logistico",
                    help="Mensagem de commit (com --git/--git-all).")
    args = ap.parse_args()

    print("\n== Publicação do Scorecard Logístico ==\n")
    info(f"Pasta do script: {HERE}")
    info(f"Lendo dados de: {HTML_PATH.name}")

    data, fonte = obter_dados()

    conteudo = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    JSON_PATH.write_text(conteudo.decode("utf-8"), encoding="utf-8")
    ok(f"Gerado {JSON_PATH.name} ({len(conteudo)/1024:.1f} KB)")

    if args.apenas_json:
        print("\nConcluído (apenas JSON local).\n")
        return

    token = autenticar()
    publicar_no_supabase(token, conteudo)

    if args.git or args.git_all:
        info("Versionando no GitHub…")
        publicar_no_github(args.mensagem, tudo=args.git_all)

    print("\nPublicação concluída.\n")


if __name__ == "__main__":
    main()
