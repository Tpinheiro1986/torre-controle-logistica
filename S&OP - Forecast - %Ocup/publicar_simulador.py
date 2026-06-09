#!/usr/bin/env python3
"""
================================================================================
  PUBLICADOR DO SIMULADOR S&OP / FORECAST — Torre de Controle
================================================================================

Este script faz TUDO automaticamente:
  1. Clona (ou atualiza) o repositório da Torre
  2. Cria a pasta simulador-sop/ ao lado de simulador-frete/
  3. Coloca o arquivo index.html do simulador
  4. Atualiza o index.html principal da Torre, adicionando o card
     "S&OP - Forecast | % Ocupação"
  5. Faz commit e push para o GitHub

Pré-requisitos (uma vez só):
  - Python 3.8+
  - Git instalado
  - Permissão de push no repositório

Para usar:
  - Coloque ESTE arquivo (publicar_simulador.py) numa pasta junto com:
      • index.html      (página do simulador, gerada pelo Claude)
  - Duplo-clique no arquivo (ou execute "python publicar_simulador.py")
  - Siga as instruções na tela

Versão: 1.0
"""

import os
import sys
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO — ajuste se precisar
# ════════════════════════════════════════════════════════════════════════════

# URL do repositório Git da Torre (será perguntado se não estiver aqui)
REPO_URL_PADRAO = ""   # ex: "https://github.com/usuario/torre-controle.git"

# Nome do branch onde publicar
BRANCH = "main"

# Nome da pasta do simulador no repositório
PASTA_DESTINO = "simulador-sop"

# Configuração do card a inserir no index.html principal
CARD_NOME = "S&OP - Forecast | % Ocupação"
CARD_SUBTITULO = "Ocupação de PP · Genomma × Inovalab · Mensal/Semanal"
CARD_COR = "#7030A0"   # roxo (diferente das cores já usadas)

# Arquivo HTML do simulador (deve estar na mesma pasta deste script)
ARQUIVO_HTML_SIMULADOR = "index.html"

# Marcador único do card (pra evitar duplicação se rodar várias vezes)
CARD_MARKER = '<!-- SIMULADOR-SOP-CARD -->'


# ════════════════════════════════════════════════════════════════════════════
# UTILIDADES VISUAIS
# ════════════════════════════════════════════════════════════════════════════

class C:
    """Cores ANSI para o terminal."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    GRAY = '\033[90m'


def cabecalho(texto):
    largura = 76
    print()
    print(C.BLUE + "═" * largura + C.RESET)
    print(C.BLUE + C.BOLD + f"  {texto}" + C.RESET)
    print(C.BLUE + "═" * largura + C.RESET)


def passo(numero, texto):
    print()
    print(C.CYAN + C.BOLD + f"  [{numero}] {texto}" + C.RESET)


def ok(texto):
    print(C.GREEN + f"      ✓ {texto}" + C.RESET)


def info(texto):
    print(C.GRAY + f"        {texto}" + C.RESET)


def aviso(texto):
    print(C.YELLOW + f"      ⚠ {texto}" + C.RESET)


def erro(texto):
    print(C.RED + f"      ✗ {texto}" + C.RESET)


def perguntar(texto, default=None):
    sufixo = f" [{default}]" if default else ""
    resp = input(C.CYAN + f"      → {texto}{sufixo}: " + C.RESET).strip()
    return resp or (default or "")


def confirmar(texto, default_sim=False):
    sufixo = "[S/n]" if default_sim else "[s/N]"
    resp = input(C.CYAN + f"      → {texto} {sufixo}: " + C.RESET).strip().lower()
    if not resp:
        return default_sim
    return resp in ('s', 'sim', 'y', 'yes')


# ════════════════════════════════════════════════════════════════════════════
# VERIFICAÇÕES DE AMBIENTE
# ════════════════════════════════════════════════════════════════════════════

def verificar_git():
    """Confirma que o Git está instalado."""
    try:
        r = subprocess.run(['git', '--version'], capture_output=True, text=True)
        if r.returncode == 0:
            ok(f"Git instalado: {r.stdout.strip()}")
            return True
    except FileNotFoundError:
        pass
    erro("Git não está instalado.")
    info("Baixe em: https://git-scm.com/downloads")
    info("Depois de instalar, feche e abra esta janela de novo.")
    return False


def verificar_arquivo_html(pasta_script):
    """Confirma que o arquivo index.html do simulador está na pasta."""
    caminho = pasta_script / ARQUIVO_HTML_SIMULADOR
    if not caminho.exists():
        erro(f"Não encontrei o arquivo '{ARQUIVO_HTML_SIMULADOR}' na mesma pasta deste script.")
        info(f"Pasta atual: {pasta_script}")
        info("Coloque o arquivo aí e rode novamente.")
        return None
    tamanho = caminho.stat().st_size / 1024
    ok(f"Encontrei o {ARQUIVO_HTML_SIMULADOR} ({tamanho:.0f} KB)")
    return caminho


# ════════════════════════════════════════════════════════════════════════════
# OPERAÇÕES GIT
# ════════════════════════════════════════════════════════════════════════════

def normalizar_url(url):
    """Aceita várias formas de URL do GitHub e retorna a forma de clone.

    Exemplos aceitos:
      https://github.com/user/repo
      https://github.com/user/repo/
      https://github.com/user/repo.git
      https://github.com/user/repo/tree/main
      https://github.com/user/repo/tree/main/qualquer/pasta
      git@github.com:user/repo.git
    """
    url = url.strip()
    if not url:
        return url

    # SSH não mexer
    if url.startswith('git@'):
        return url

    # Remover /tree/branch/... do final (URL da página web)
    m = re.match(r'(https?://[^/]+/[^/]+/[^/]+)(/tree/[^/]+(/.*)?)?/?$', url)
    if m:
        url = m.group(1)

    # Acrescentar .git se faltar
    if not url.endswith('.git'):
        url = url.rstrip('/') + '.git'

    return url


def rodar(comando, cwd=None, captura=True):
    """Roda um comando shell, retorna (sucesso, saída)."""
    r = subprocess.run(
        comando,
        cwd=cwd,
        capture_output=captura,
        text=True,
        shell=isinstance(comando, str),
        encoding='utf-8',
        errors='replace',
    )
    return r.returncode == 0, (r.stdout or '') + (r.stderr or '')


def clonar_ou_atualizar(repo_url, pasta_destino):
    """Clona o repo (se não existir) ou faz pull (se já existir)."""
    if (pasta_destino / '.git').exists():
        info("Repositório já clonado, atualizando…")
        sucesso, out = rodar(['git', 'fetch', 'origin'], cwd=pasta_destino)
        if not sucesso:
            erro(f"Falha no fetch: {out}")
            return False
        sucesso, out = rodar(['git', 'checkout', BRANCH], cwd=pasta_destino)
        if not sucesso:
            erro(f"Falha no checkout: {out}")
            return False
        sucesso, out = rodar(['git', 'pull', 'origin', BRANCH], cwd=pasta_destino)
        if not sucesso:
            erro(f"Falha no pull: {out}")
            info("Pode ser conflito. Tente apagar a pasta de trabalho e rodar de novo.")
            return False
        ok("Repositório atualizado")
        return True
    else:
        info(f"Clonando do GitHub… (pode demorar 30s)")
        sucesso, out = rodar(['git', 'clone', '-b', BRANCH, repo_url, str(pasta_destino)])
        if not sucesso:
            erro(f"Falha no clone: {out}")
            info("Verifique se a URL está correta e se você tem acesso ao repo.")
            return False
        ok("Repositório clonado")
        return True


def commit_e_push(pasta_repo, mensagem):
    """Faz add, commit e push das mudanças."""
    # Verifica se há algo para commitar
    sucesso, out = rodar(['git', 'status', '--porcelain'], cwd=pasta_repo)
    if not out.strip():
        aviso("Nada mudou — não há o que publicar.")
        return True

    info("Arquivos modificados:")
    for linha in out.strip().split('\n')[:10]:
        info(f"    {linha}")

    # add
    sucesso, out = rodar(['git', 'add', '.'], cwd=pasta_repo)
    if not sucesso:
        erro(f"Falha no add: {out}"); return False

    # commit
    sucesso, out = rodar(['git', 'commit', '-m', mensagem], cwd=pasta_repo)
    if not sucesso:
        erro(f"Falha no commit: {out}"); return False
    ok("Commit feito")

    # push
    info("Enviando para o GitHub… (pode pedir credenciais)")
    sucesso, out = rodar(['git', 'push', 'origin', BRANCH], cwd=pasta_repo, captura=False)
    if not sucesso:
        erro("Falha no push.")
        info("Se aparecer pedindo senha do GitHub, use seu Personal Access Token.")
        info("Gere em: https://github.com/settings/tokens")
        return False
    ok("Push concluído")
    return True


# ════════════════════════════════════════════════════════════════════════════
# MANIPULAÇÃO DO INDEX.HTML PRINCIPAL DA TORRE
# ════════════════════════════════════════════════════════════════════════════

CARD_HTML = f'''      {CARD_MARKER}
      <a class="painel-card" href="{PASTA_DESTINO}/index.html">
        <div class="painel-icon" style="background:{CARD_COR}">
          <svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>
        </div>
        <div class="painel-name">{CARD_NOME}</div>
        <div class="painel-sub">{CARD_SUBTITULO}</div>
        <span class="painel-badge badge-live">● Ao vivo</span>
      </a>
'''


def inserir_card_no_index(pasta_repo):
    """Adiciona o card do simulador no index.html principal, ao lado do Simulador de Frete."""
    index_path = pasta_repo / 'index.html'
    if not index_path.exists():
        erro(f"Não encontrei {index_path}")
        return False

    html = index_path.read_text(encoding='utf-8')

    # Se já tem o marcador, atualiza in-place (idempotente)
    if CARD_MARKER in html:
        info("Card já existe — atualizando…")
        # Remove o card antigo
        pat = re.compile(
            re.escape(CARD_MARKER) + r'.*?</a>\s*',
            re.DOTALL)
        html = pat.sub('', html)
        ok("Card antigo removido")

    # Inseri após o card "Simulador de Frete" (que termina em </a>)
    # Detecta pelo texto "Simulador de Frete"
    # Padrão: o card de Frete começa com <a class="painel-card" href="simulador-frete/...">
    # e termina no próximo </a>
    pat_frete = re.compile(
        r'(<a\s+class="painel-card"\s+href="simulador-frete[^"]*"[^>]*>.*?</a>)',
        re.DOTALL
    )
    m = pat_frete.search(html)
    if m:
        novo_html = html[:m.end()] + '\n\n' + CARD_HTML + html[m.end():]
        info("Card inserido ao lado de 'Simulador de Frete'")
    else:
        # Fallback: inserir no fim do painel-grid
        aviso("Card 'Simulador de Frete' não encontrado, inserindo no fim do grid.")
        pat_grid = re.compile(r'(<div\s+class="painel-grid"[^>]*>)(.*?)(</div>)', re.DOTALL)
        m2 = pat_grid.search(html)
        if not m2:
            erro("Estrutura inesperada no index.html — abortando.")
            return False
        novo_html = html[:m2.start(2)] + m2.group(2).rstrip() + '\n\n' + CARD_HTML + html[m2.start(3):]

    # Salvar
    index_path.write_text(novo_html, encoding='utf-8')
    ok(f"index.html atualizado ({len(novo_html):,} bytes)")
    return True


def copiar_simulador(pasta_repo, html_simulador_origem):
    """Cria a pasta do simulador e copia o index.html dentro."""
    pasta_sim = pasta_repo / PASTA_DESTINO
    pasta_sim.mkdir(exist_ok=True)
    destino = pasta_sim / 'index.html'
    shutil.copy2(html_simulador_origem, destino)
    tamanho = destino.stat().st_size / 1024
    ok(f"index.html do simulador copiado para {PASTA_DESTINO}/ ({tamanho:.0f} KB)")
    return True


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    cabecalho("PUBLICADOR DO SIMULADOR S&OP / FORECAST")
    print()
    print(C.GRAY + "  Este script vai publicar a página do simulador na Torre de Controle." + C.RESET)
    print(C.GRAY + "  Você só precisa responder algumas perguntas." + C.RESET)
    print()

    pasta_script = Path(__file__).parent.resolve()

    # ─── Passo 1: verificações ───
    passo(1, "Verificando ambiente…")
    if not verificar_git():
        input("\n  Pressione Enter para fechar.")
        sys.exit(1)
    html_simulador = verificar_arquivo_html(pasta_script)
    if not html_simulador:
        input("\n  Pressione Enter para fechar.")
        sys.exit(1)

    # ─── Passo 2: configuração do repo ───
    passo(2, "Configuração do repositório")

    # Buscar URL salva
    config_file = pasta_script / '.simulador_config.txt'
    repo_url_salvo = ""
    if config_file.exists():
        repo_url_salvo = config_file.read_text(encoding='utf-8').strip()

    repo_url = REPO_URL_PADRAO or repo_url_salvo
    if repo_url:
        info(f"URL atual: {repo_url}")
        if not confirmar("Usar essa URL?", default_sim=True):
            repo_url = ""

    if not repo_url:
        info("Cole a URL do repositório da Torre (encontra em github.com/.../).")
        info("Pode ser a URL da página normal (ex: github.com/usuario/torre-controle)")
        info("ou a URL .git (ex: github.com/usuario/torre-controle.git).")
        repo_url = perguntar("URL do repositório")
        if not repo_url:
            erro("URL é obrigatória.")
            input("\n  Pressione Enter para fechar.")
            sys.exit(1)

    # Normalizar (remover /tree/, acrescentar .git)
    repo_url_original = repo_url
    repo_url = normalizar_url(repo_url)
    if repo_url != repo_url_original:
        info(f"URL ajustada para: {repo_url}")

    # Salvar URL para próximas execuções
    config_file.write_text(repo_url, encoding='utf-8')

    # ─── Passo 3: pasta de trabalho ───
    passo(3, "Preparando pasta de trabalho")

    # IMPORTANTE: NÃO clonar dentro do path do script se ele tiver caracteres
    # especiais como % ou & — esses bagunçam o CMD do Windows.
    # Usar a pasta home do usuário em vez disso.
    path_seguro = all(c not in str(pasta_script) for c in '%&')

    if path_seguro:
        pasta_trabalho = pasta_script / '_torre_repo'
    else:
        # Path tem caracteres especiais — mover para a home do usuário
        home = Path.home()
        pasta_trabalho = home / '.torre-publicador' / 'repo'
        pasta_trabalho.parent.mkdir(parents=True, exist_ok=True)
        info("Caminho deste script contém caracteres especiais (% ou &).")
        info(f"Vou usar um local seguro: {pasta_trabalho}")

    info(f"Pasta de trabalho: {pasta_trabalho}")

    if not clonar_ou_atualizar(repo_url, pasta_trabalho):
        input("\n  Pressione Enter para fechar.")
        sys.exit(1)

    # ─── Passo 4: copiar HTML do simulador ───
    passo(4, "Copiando página do simulador")
    if not copiar_simulador(pasta_trabalho, html_simulador):
        input("\n  Pressione Enter para fechar.")
        sys.exit(1)

    # ─── Passo 5: atualizar index principal ───
    passo(5, "Atualizando index.html principal da Torre")
    if not inserir_card_no_index(pasta_trabalho):
        input("\n  Pressione Enter para fechar.")
        sys.exit(1)

    # ─── Passo 6: commit + push ───
    passo(6, "Publicando no GitHub")
    timestamp = datetime.now().strftime('%d/%m/%Y %H:%M')
    mensagem_commit = f"Publica simulador S&OP/Forecast ({timestamp})"
    if not commit_e_push(pasta_trabalho, mensagem_commit):
        input("\n  Pressione Enter para fechar.")
        sys.exit(1)

    # ─── Final ───
    print()
    print(C.GREEN + "═" * 76 + C.RESET)
    print(C.GREEN + C.BOLD + "  ✓ TUDO PUBLICADO COM SUCESSO!" + C.RESET)
    print(C.GREEN + "═" * 76 + C.RESET)
    print()
    info("Acesse a Torre normalmente — o card 'S&OP - Forecast' já está disponível.")
    info("Se a Torre estiver no GitHub Pages, pode demorar 1-2 min para aparecer.")
    print()
    input(C.CYAN + "  Pressione Enter para fechar." + C.RESET)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print()
        aviso("Cancelado pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print()
        erro(f"Erro inesperado: {e}")
        import traceback
        print(C.GRAY)
        traceback.print_exc()
        print(C.RESET)
        input("\n  Pressione Enter para fechar.")
        sys.exit(1)
