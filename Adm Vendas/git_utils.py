# -*- coding: utf-8 -*-
"""
git_utils.py — Publicação Git blindada para repositórios dentro do OneDrive.

Resolve os erros vistos na carga diária:
  - "cannot pull with rebase: You have unstaged changes"  -> pull --rebase --autostash
  - "! [rejected] main -> main (fetch first)"             -> pull antes do push + retry
  - "fatal: Unable to create index.lock"                   -> retry com espera (lock do OneDrive)

Uso como módulo (dentro do script de ingestão, substituindo o bloco Git atual):

    from git_utils import git_publicar
    git_publicar(r"C:\\Users\\thiago.pinheiro\\OneDrive - genommalabinternacional\\Área de Trabalho\\Codigo\\torre-controle-logistica",
                 "carga diaria 04/08/2026")

Uso via linha de comando (chamado pelo .bat, sem precisar editar o script de ingestão):

    python git_utils.py --repo "C:\\...\\torre-controle-logistica" --msg "carga diaria 04/08/2026"

O caminho passado pode ser qualquer subpasta do repositório (ex.: adm-vendas);
a raiz é descoberta automaticamente subindo os diretórios até achar a pasta .git.
"""

import argparse
import os
import subprocess
import sys
import time

BRANCH = "main"
TENTATIVAS_LOCK = 5      # tentativas quando o OneDrive segura index.lock
ESPERA_LOCK = 4          # segundos entre tentativas
TENTATIVAS_PUSH = 3      # tentativas de push (com pull entre elas)


# ---------------------------------------------------------------- utilidades

def achar_raiz_repo(caminho):
    """Sobe os diretórios a partir de `caminho` até encontrar a pasta .git."""
    atual = os.path.abspath(caminho)
    while True:
        if os.path.isdir(os.path.join(atual, ".git")):
            return atual
        pai = os.path.dirname(atual)
        if pai == atual:
            raise RuntimeError(
                f"[ERRO] Nenhum repositório Git encontrado acima de: {caminho}"
            )
        atual = pai


def _run(repo, *args, check=True):
    """Executa um comando git mostrando a saída. Retorna CompletedProcess."""
    cmd = ["git", "-C", repo] + list(args)
    r = subprocess.run(cmd, capture_output=True, text=True,
                       encoding="utf-8", errors="replace")
    saida = (r.stdout or "") + (r.stderr or "")
    if saida.strip():
        print(saida.strip())
    if check and r.returncode != 0:
        raise RuntimeError(f"[ERRO] git {' '.join(args)} falhou (código {r.returncode})")
    return r


def _run_com_retry_lock(repo, *args, check=True):
    """Como _run, mas repete se o OneDrive estiver segurando index.lock."""
    for tentativa in range(1, TENTATIVAS_LOCK + 1):
        cmd = ["git", "-C", repo] + list(args)
        r = subprocess.run(cmd, capture_output=True, text=True,
                           encoding="utf-8", errors="replace")
        saida = (r.stdout or "") + (r.stderr or "")
        if saida.strip():
            print(saida.strip())
        if r.returncode == 0:
            return r
        if "index.lock" in saida or "Unable to create" in saida:
            lock = os.path.join(repo, ".git", "index.lock")
            print(f"[AVISO] Lock do OneDrive detectado "
                  f"(tentativa {tentativa}/{TENTATIVAS_LOCK}). "
                  f"Aguardando {ESPERA_LOCK}s...")
            time.sleep(ESPERA_LOCK)
            # se o lock ficou órfão (nenhum git rodando), remove
            if tentativa >= 3 and os.path.exists(lock):
                try:
                    os.remove(lock)
                    print("[AVISO] index.lock órfão removido.")
                except OSError:
                    pass
            continue
        if check:
            raise RuntimeError(f"[ERRO] git {' '.join(args)} falhou (código {r.returncode})")
        return r
    raise RuntimeError("[ERRO] index.lock persistiu após todas as tentativas. "
                       "Pause a sincronização do OneDrive e rode de novo.")


# ---------------------------------------------------------------- fluxo principal

def git_publicar(caminho, mensagem):
    """
    Fluxo completo e tolerante a falhas:
      1. add -A            (nada fica não-staged — evita o erro de unstaged changes)
      2. commit            (ignora 'nothing to commit')
      3. pull --rebase --autostash   (resolve unstaged + remoto à frente)
         -> fallback: abort do rebase + pull --no-rebase (merge)
      4. push              (com retry: se rejeitado, pull de novo e tenta outra vez)
    """
    repo = achar_raiz_repo(caminho)
    print(f"[GIT] Repositório: {repo}")

    # 1. Stage de tudo
    _run_com_retry_lock(repo, "add", "-A")

    # 2. Commit (não é erro se não houver nada a comitar)
    r = _run_com_retry_lock(repo, "commit", "-m", mensagem, check=False)
    if r.returncode != 0 and "nothing to commit" not in (r.stdout + r.stderr):
        # commit falhou por outro motivo real
        raise RuntimeError("[ERRO] git commit falhou.")

    # 3. Sincronizar com o remoto
    r = _run(repo, "pull", "--rebase", "--autostash", "origin", BRANCH, check=False)
    if r.returncode != 0:
        print("[AVISO] Rebase falhou. Tentando fallback com merge...")
        _run(repo, "rebase", "--abort", check=False)
        _run(repo, "pull", "--no-rebase", "--no-edit", "origin", BRANCH)

    # 4. Push com retry
    for tentativa in range(1, TENTATIVAS_PUSH + 1):
        r = _run(repo, "push", "origin", BRANCH, check=False)
        if r.returncode == 0:
            print("[GIT] Push concluído com sucesso.")
            return
        print(f"[AVISO] Push rejeitado (tentativa {tentativa}/{TENTATIVAS_PUSH}). "
              "Sincronizando de novo...")
        _run(repo, "pull", "--rebase", "--autostash", "origin", BRANCH, check=False)
        time.sleep(2)

    raise RuntimeError("[ERRO] Push falhou após todas as tentativas.")


# ---------------------------------------------------------------- CLI

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Publicação Git blindada (OneDrive-safe)")
    p.add_argument("--repo", required=True,
                   help="Caminho do repositório (ou qualquer subpasta dele)")
    p.add_argument("--msg", required=True, help="Mensagem do commit")
    args = p.parse_args()
    try:
        git_publicar(args.repo, args.msg)
    except RuntimeError as e:
        print(e)
        sys.exit(1)
