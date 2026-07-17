# -*- coding: utf-8 -*-
"""
Aplica o novo scorecard/index.html (indicadores financeiros R$ em Custo
Logístico) no repositório da Torre de Controle e publica no GitHub Pages.

v2 — encontra a raiz do repositório automaticamente: pode ficar na raiz,
dentro de scorecard/ ou em qualquer subpasta do repo.

Uso:
    Deixe este script junto do novo index.html (na mesma pasta) em
    qualquer lugar DENTRO do repositório torre-controle-logistica
    e execute o aplicar.bat (ou: python aplicar_custo_logistico.py).
"""
import os, sys, time, shutil, subprocess
from datetime import datetime

AQUI       = os.path.dirname(os.path.abspath(__file__))
NOVO_IDX   = os.path.join(AQUI, "index.html")
BRANCH     = "main"
COMMIT_MSG = ("Scorecard: indicadores financeiros R$ (Faturamento, Custo "
              "Transporte, T&W, Devolução) + acumulado YoY e CAGR")

def log(msg):  print(f"[aplicar] {msg}")
def erro(msg): print(f"[ERRO] {msg}"); input("\nEnter para sair…"); sys.exit(1)

def achar_repo(inicio):
    """Sobe as pastas a partir de 'inicio' até encontrar um diretório .git."""
    p = os.path.abspath(inicio)
    while True:
        if os.path.isdir(os.path.join(p, ".git")):
            return p
        pai = os.path.dirname(p)
        if pai == p:                       # chegou na raiz do disco
            return None
        p = pai

REPO_DIR = achar_repo(AQUI)

def git(*args, check=True, tentativas=3):
    """Executa git com retentativas (OneDrive costuma travar o .git)."""
    cmd = ["git", "-C", REPO_DIR] + list(args)
    r = None
    for i in range(1, tentativas + 1):
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode == 0:
            return r
        saida = (r.stdout + r.stderr).lower()
        travado = any(t in saida for t in
                      ("unable to create", "index.lock", "permission denied",
                       "unlink", "being used by another process"))
        if travado and i < tentativas:
            log(f"git {' '.join(args)} falhou (provável trava do OneDrive). "
                f"Tentativa {i}/{tentativas}… aguardando 4s")
            lock = os.path.join(REPO_DIR, ".git", "index.lock")
            if os.path.exists(lock):
                try: os.remove(lock); log("index.lock removido.")
                except OSError: pass
            time.sleep(4)
            continue
        if check:
            erro(f"git {' '.join(args)}:\n{r.stdout}\n{r.stderr}")
        return r
    return r

def limpar_rebase_interrompido():
    for pasta in ("rebase-merge", "rebase-apply"):
        p = os.path.join(REPO_DIR, ".git", pasta)
        if os.path.isdir(p):
            log(f"Removendo rebase interrompido: .git/{pasta}")
            shutil.rmtree(p, ignore_errors=True)

def main():
    # --- localizar repo ---
    if not REPO_DIR:
        erro("Não encontrei um repositório git subindo a partir de:\n"
             f"  {AQUI}\n"
             "Coloque este script (e o index.html) em qualquer pasta DENTRO "
             "do repositório torre-controle-logistica.")
    log(f"Repositório encontrado: {REPO_DIR}")
    destino = os.path.join(REPO_DIR, "scorecard", "index.html")
    os.makedirs(os.path.dirname(destino), exist_ok=True)

    # --- validar o novo index ---
    if not os.path.isfile(NOVO_IDX):
        erro(f"Novo index.html não encontrado ao lado do script:\n  {NOVO_IDX}")
    with open(NOVO_IDX, encoding="utf-8") as f:
        conteudo = f.read()
    for chave in ("fat_rs", "ctransp_rs", "ctw_rs", "dev_rs",
                  "fatliq_rs", "devpct", "custo_op_ac", "acumOpt"):
        if chave not in conteudo:
            erro(f"O index.html ao lado do script não é a versão nova "
                 f"(faltou '{chave}'). Confira se copiou o arquivo certo.")
    log("Novo index.html validado (indicadores financeiros presentes).")

    # --- backup + cópia (a menos que o arquivo já esteja no destino) ---
    mesmo_arquivo = os.path.normcase(os.path.abspath(NOVO_IDX)) == \
                    os.path.normcase(os.path.abspath(destino))
    if mesmo_arquivo:
        log("O novo index.html já está em scorecard/ — sem cópia necessária.")
    else:
        if os.path.isfile(destino):
            carimbo = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = os.path.join(os.path.dirname(destino),
                                  f"index_backup_{carimbo}.html")
            shutil.copy2(destino, backup)
            log(f"Backup criado: {os.path.relpath(backup, REPO_DIR)}")
        shutil.copy2(NOVO_IDX, destino)
        log("scorecard/index.html atualizado.")

    # --- git ---
    limpar_rebase_interrompido()
    git("add", "scorecard/index.html")

    r = git("commit", "-m", COMMIT_MSG, check=False)
    if r.returncode != 0:
        if "nothing to commit" in (r.stdout + r.stderr):
            log("Nada novo para commitar (arquivo idêntico ao publicado).")
        else:
            erro(f"commit:\n{r.stdout}\n{r.stderr}")
    else:
        log("Commit criado.")

    r = git("pull", "--rebase", "origin", BRANCH, check=False)
    if r.returncode != 0:
        log("pull --rebase falhou; limpando e tentando pull --no-edit…")
        limpar_rebase_interrompido()
        git("pull", "--no-edit", "origin", BRANCH, check=False)

    git("push", "origin", BRANCH)
    log("Push concluído.")
    log("Publicado! Aguarde ~1 min o GitHub Pages e acesse:")
    log("  https://tpinheiro1986.github.io/torre-controle-logistica/scorecard/")
    log("Depois, entre como admin e use “Lançar / Atualizar” para preencher "
        "Faturamento, Custo de Transporte, Custo Total T&W e Devolução.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado pelo usuário.")
