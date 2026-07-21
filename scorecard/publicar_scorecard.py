#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Publicador do Scorecard — Torre de Controle Logística
-----------------------------------------------------
Envia as ATUALIZAÇÕES do scorecard para o GitHub Pages SEM tocar nas
informações passadas:

  • Publica apenas os arquivos do layout (pasta scorecard/).
  • NÃO sobrescreve o dados.json publicado (o histórico fica intacto).
  • Robusto ao OneDrive: limpa locks do .git, recupera rebase interrompido,
    tenta novamente com backoff, e sobe a árvore até achar o .git
    (funciona esteja o script na raiz ou dentro de /scorecard).

Uso:
    Duplo clique em  publicar.bat
    ou:  python publicar_scorecard.py
    Teste sem enviar:  set PUBLICAR_DRYRUN=1  &&  python publicar_scorecard.py
"""
from __future__ import annotations
import os, sys, time, shutil, subprocess, getpass
from pathlib import Path

# ------------------------------------------------------------------ ajustes
BRANCH        = "main"
MENSAGEM      = "Atualiza layout do Scorecard (Torre de Controle)"
PASTA_PUBLICA = "scorecard"                 # o que será versionado/enviado
DADOS_PROTEGIDO = "scorecard/dados.json"    # NUNCA sobrescrito por este script
DRYRUN        = os.environ.get("PUBLICAR_DRYRUN") == "1"

# Upload do dados.json ao Supabase fica DESLIGADO — assim as informações
# passadas não são alteradas. Ligue só se realmente quiser regravar o histórico.
SUBIR_DADOS_SUPABASE = False
SUPABASE_BUCKET  = "dashboards"
SUPABASE_OBJETO  = "scorecard/dados.json"
SUPABASE_URL_PAD = "https://ennsbpibfnuwlvtodukg.supabase.co"

# ------------------------------------------------------------------ util
def achar_repo(inicio: Path) -> Path:
    p = inicio.resolve()
    for cand in [p, *p.parents]:
        if (cand / ".git").exists():
            return cand
    sys.exit("✗ Não encontrei um repositório .git subindo a partir de: " + str(inicio))

def ler_env(repo: Path) -> dict:
    """Parser de .env independente de bibliotecas externas (à prova de falha)."""
    env = {}
    f = repo / ".env"
    if f.is_file():
        for linha in f.read_text(encoding="utf-8", errors="ignore").splitlines():
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            k, v = linha.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env

def git(repo: Path, *args) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=str(repo), text=True, capture_output=True)

def limpar_locks(repo: Path):
    """OneDrive costuma travar o .git no meio da operação."""
    for lock in [repo/".git/index.lock", repo/".git/HEAD.lock", repo/".git/config.lock"]:
        try:
            if lock.exists():
                lock.unlink(); print("· lock removido:", lock.name)
        except Exception as e:
            print("· não consegui remover", lock.name, "-", e)
    for d in [repo/".git/rebase-merge", repo/".git/rebase-apply"]:
        if d.exists():
            shutil.rmtree(d, ignore_errors=True); print("· rebase interrompido limpo:", d.name)

def git_retry(repo: Path, *args, tentativas=4) -> subprocess.CompletedProcess:
    r = None
    for i in range(1, tentativas + 1):
        r = git(repo, *args)
        if r.returncode == 0:
            if r.stdout.strip():
                print(r.stdout.strip())
            return r
        msg = (r.stderr or r.stdout or "").strip()
        ultima = msg.splitlines()[-1] if msg else "(sem detalhe)"
        print(f"· tentativa {i}/{tentativas} de 'git {args[0]}' falhou: {ultima}")
        if any(t in msg for t in ("index.lock", "Unable to create", "another git process", "cannot lock ref")):
            limpar_locks(repo)
        time.sleep(1.2 * i)
    return r

# ------------------------------------------------------------------ fluxo
def main():
    repo = achar_repo(Path(__file__).parent)
    print("Repositório:", repo)
    if DRYRUN:
        print("MODO TESTE (DRYRUN): sem pull/push, sem envio.")
    limpar_locks(repo)

    # 1) versiona só o layout, protegendo o dados.json
    git_retry(repo, "add", PASTA_PUBLICA)
    if not SUBIR_DADOS_SUPABASE:
        git(repo, "reset", "--", DADOS_PROTEGIDO)   # remove do stage p/ não publicar alteração
        git(repo, "checkout", "--", DADOS_PROTEGIDO) # restaura versão publicada, se houver mudança local

    status = git(repo, "status", "--porcelain")
    if not status.stdout.strip():
        print("Nada novo para publicar — árvore limpa.")
    else:
        print("Alterações a publicar:")
        print(status.stdout.strip())
        git_retry(repo, "commit", "-m", MENSAGEM)
        if DRYRUN:
            print("DRYRUN: commit feito localmente; pull/push pulados.")
        else:
            # 2) sincroniza antes de enviar (rebase; se falhar, merge --no-edit)
            pr = git_retry(repo, "pull", "--rebase", "origin", BRANCH)
            if pr.returncode != 0:
                limpar_locks(repo)
                git_retry(repo, "pull", "--no-edit", "origin", BRANCH)
            # 3) envia
            ps = git_retry(repo, "push", "origin", BRANCH)
            if ps.returncode != 0:
                sys.exit("✗ Falha no push. Verifique login do GitHub e rode novamente.")
            print("✓ Publicado no GitHub Pages.")

    # 4) (desligado) upload do histórico ao Supabase
    if SUBIR_DADOS_SUPABASE and not DRYRUN:
        subir_supabase(repo)
    else:
        print("· dados.json NÃO foi enviado ao Supabase — histórico preservado.")

def subir_supabase(repo: Path):
    import urllib.request
    env = ler_env(repo)
    url = env.get("SUPABASE_URL") or SUPABASE_URL_PAD
    key = env.get("SUPABASE_SERVICE_KEY") or env.get("SUPABASE_KEY") or ""
    if not key:
        try:
            key = getpass.getpass("Service key do Supabase (vazio p/ pular): ").strip()
        except Exception:
            key = ""
    if not key:
        print("· sem chave — upload ao Supabase pulado (dados.json intactos).")
        return
    arq = repo / DADOS_PROTEGIDO
    if not arq.is_file():
        print("· dados.json local não encontrado — nada enviado.")
        return
    endpoint = f"{url}/storage/v1/object/{SUPABASE_BUCKET}/{SUPABASE_OBJETO}"
    req = urllib.request.Request(endpoint, data=arq.read_bytes(), method="POST",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json", "x-upsert": "true"})
    try:
        urllib.request.urlopen(req, timeout=30)
        print("✓ dados.json enviado ao Supabase.")
    except Exception as e:
        print("· falha no upload ao Supabase:", e)

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print("ERRO inesperado:", e)
        sys.exit(1)
