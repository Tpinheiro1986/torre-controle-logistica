#!/usr/bin/env python3
"""
deploy.py — Deploy automatizado da Torre de Controle

Faz o trabalho que hoje você faz na mão:
  1) Sobe arquivos para o Supabase Storage (bucket "dashboards")
  2) Faz commit + push no GitHub (que publica no GitHub Pages)

Uso:
  python deploy.py                    # deploy completo (Supabase + GitHub)
  python deploy.py --so-supabase      # só Supabase (sem mexer no Git)
  python deploy.py --so-github        # só GitHub (sem mexer no Supabase)
  python deploy.py --dry-run          # mostra o que faria, sem executar
  python deploy.py --msg "minha msg"  # define mensagem do commit

Configuração:
  - Edite deploy.config.json para definir quais arquivos vão pra onde
  - Crie .env com SUPABASE_SERVICE_KEY=... (NUNCA versionar no Git!)
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

# ============================================================
# UTILS DE CORES (funciona em Windows 10+, Mac, Linux)
# ============================================================
def _supports_color():
    if os.name == 'nt':
        try:
            import colorama; colorama.just_fix_windows_console()
            return True
        except ImportError:
            return os.environ.get('TERM') is not None
    return sys.stdout.isatty()

_USE_COLOR = _supports_color()
def _c(text, code):
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text

def info(msg):   print(_c("ℹ ", "36") + msg)
def ok(msg):     print(_c("✓ ", "32") + msg)
def warn(msg):   print(_c("⚠ ", "33") + msg)
def err(msg):    print(_c("✗ ", "31") + msg)
def step(msg):   print("\n" + _c("━" * 60, "90") + "\n" + _c(msg, "1;36") + "\n" + _c("━" * 60, "90"))

# ============================================================
# CARREGAR CONFIG E .env
# ============================================================
ROOT = Path(__file__).parent.resolve()
CONFIG_PATH = ROOT / "deploy.config.json"
ENV_PATH = ROOT / ".env"

def load_env():
    """Carrega .env num dict (sem precisar do pacote python-dotenv)."""
    env = dict(os.environ)
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env

def load_config():
    if not CONFIG_PATH.exists():
        err(f"Arquivo não encontrado: {CONFIG_PATH}")
        err("Rode primeiro: python deploy.py --init")
        sys.exit(1)
    return json.loads(CONFIG_PATH.read_text(encoding='utf-8'))

# ============================================================
# SUPABASE STORAGE — upload via REST API (sem dependências)
# ============================================================
def supabase_upload(supabase_url, service_key, bucket, remote_path, local_path):
    """
    Sobe um arquivo para o bucket. Usa upsert (sobrescreve se existir).
    """
    import urllib.request
    import urllib.error
    import mimetypes

    # Determinar content-type
    ct, _ = mimetypes.guess_type(str(local_path))
    if ct is None:
        ct = 'application/octet-stream'

    # Endpoint: POST /storage/v1/object/{bucket}/{path}
    url = f"{supabase_url}/storage/v1/object/{bucket}/{remote_path}"

    data = Path(local_path).read_bytes()
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header('Authorization', f'Bearer {service_key}')
    req.add_header('Content-Type', ct)
    req.add_header('x-upsert', 'true')           # sobrescreve se já existir
    req.add_header('Cache-Control', 'no-cache')  # navegadores sempre buscam versão nova

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', errors='replace')
    except urllib.error.URLError as e:
        return 0, f"Erro de conexão: {e.reason}"

def supabase_deploy(cfg, env, dry_run=False):
    """Sobe todos os arquivos listados na config para o Supabase."""
    step("[1/2] Deploy no Supabase Storage")

    sb_url = cfg.get('supabase_url')
    bucket = cfg.get('supabase_bucket', 'dashboards')
    service_key = env.get('SUPABASE_SERVICE_KEY')

    if not service_key:
        err("SUPABASE_SERVICE_KEY não encontrada no arquivo .env")
        err("Adicione em .env: SUPABASE_SERVICE_KEY=sua_chave_aqui")
        info("Como pegar a chave: Supabase Studio → Project Settings → API → service_role")
        sys.exit(1)

    files = cfg.get('supabase_files', [])
    if not files:
        warn("Nenhum arquivo configurado em 'supabase_files'. Pulando.")
        return 0

    info(f"Bucket: {bucket}")
    info(f"URL:    {sb_url}")
    info(f"Arquivos a enviar: {len(files)}")

    count_ok, count_err = 0, 0
    for entry in files:
        local = ROOT / entry['local']
        remote = entry['remote']

        if not local.exists():
            err(f"Arquivo local não existe: {local}")
            count_err += 1
            continue

        size_kb = local.stat().st_size / 1024
        size_str = f"{size_kb:,.1f} KB" if size_kb < 1024 else f"{size_kb/1024:,.2f} MB"

        if dry_run:
            info(f"[dry-run] {local.relative_to(ROOT)} → {bucket}/{remote} ({size_str})")
            count_ok += 1
            continue

        print(f"  Enviando {local.relative_to(ROOT)} → {bucket}/{remote} ({size_str})...", end=' ', flush=True)
        status, body = supabase_upload(sb_url, service_key, bucket, remote, local)

        if status == 200:
            print(_c("OK", "32"))
            count_ok += 1
        else:
            print(_c(f"FALHOU (HTTP {status})", "31"))
            err(f"   Resposta: {body[:200]}")
            count_err += 1

    print()
    if count_err == 0:
        ok(f"Supabase: {count_ok} arquivo(s) enviado(s) com sucesso")
    else:
        err(f"Supabase: {count_ok} OK, {count_err} com erro")
    return count_err

# ============================================================
# GIT — commit + push
# ============================================================
def run(cmd, cwd=None, capture=True):
    """Executa comando e retorna (returncode, stdout, stderr)."""
    try:
        p = subprocess.run(cmd, cwd=cwd or ROOT, capture_output=capture, text=True, encoding='utf-8', errors='replace')
        return p.returncode, p.stdout or '', p.stderr or ''
    except FileNotFoundError:
        return 127, '', f'Comando não encontrado: {cmd[0]}'

def git_status_short():
    rc, out, _ = run(['git', 'status', '--porcelain'])
    return out.strip() if rc == 0 else None

def git_deploy(cfg, msg, dry_run=False):
    step("[2/2] Deploy no GitHub (GitHub Pages)")

    # 1) Verificar se é um repo git
    rc, _, _ = run(['git', 'rev-parse', '--is-inside-work-tree'])
    if rc != 0:
        err(f"Pasta não é um repositório Git: {ROOT}")
        err("Clone o repo primeiro ou rode: git init")
        return 1

    # 2) Verificar mudanças
    status = git_status_short()
    if status is None:
        err("Erro ao verificar status do git")
        return 1

    if not status:
        info("Nada para commitar. Repositório já está sincronizado.")
        return 0

    print("Mudanças detectadas:")
    for line in status.splitlines():
        print("  " + line)
    print()

    if dry_run:
        info(f"[dry-run] git add -A && git commit -m \"{msg}\" && git push")
        return 0

    # 3) Add
    info("Adicionando arquivos...")
    rc, out, e = run(['git', 'add', '-A'])
    if rc != 0:
        err(f"git add falhou: {e}")
        return 1

    # 4) Commit
    info(f"Commit: \"{msg}\"")
    rc, out, e = run(['git', 'commit', '-m', msg])
    if rc != 0 and 'nothing to commit' not in (out + e):
        err(f"git commit falhou: {e}")
        return 1

    # 5) Push
    info("Push para o remoto...")
    rc, out, e = run(['git', 'push'])
    if rc != 0:
        err(f"git push falhou: {e}")
        info("Se for primeira vez: git push -u origin main (ou master)")
        return 1

    ok("GitHub: commit + push concluídos. GitHub Pages atualiza em ~30s a 2min.")
    pages_url = cfg.get('github_pages_url')
    if pages_url:
        info(f"Veja em: {pages_url}")
    return 0

# ============================================================
# INIT — criar config + .env de exemplo
# ============================================================
def init_config():
    if CONFIG_PATH.exists():
        warn(f"{CONFIG_PATH.name} já existe. Não sobrescrevendo.")
    else:
        default_cfg = {
            "_comentario": "Configuração do deploy.py — edite conforme necessário",
            "supabase_url": "https://ennsbpibfnuwlvtodukg.supabase.co",
            "supabase_bucket": "dashboards",
            "github_pages_url": "https://tpinheiro1986.github.io/torre-controle-logistica/",
            "default_commit_message": "deploy: atualização painel",
            "supabase_files": [
                {
                    "local": "simulador-frete/tabelas.json",
                    "remote": "simulador-frete/tabelas.json",
                    "_comentario": "Tabelas do simulador de frete"
                }
            ]
        }
        CONFIG_PATH.write_text(json.dumps(default_cfg, ensure_ascii=False, indent=2), encoding='utf-8')
        ok(f"Criado: {CONFIG_PATH.name}")

    if ENV_PATH.exists():
        warn(f"{ENV_PATH.name} já existe. Não sobrescrevendo.")
    else:
        env_template = """# Credenciais — NUNCA COMMITAR NO GIT
# Como pegar SUPABASE_SERVICE_KEY:
#   1. Acesse https://supabase.com/dashboard/project/ennsbpibfnuwlvtodukg/settings/api
#   2. Em "Project API keys", copie a chave service_role (clique no olho para revelar)
#   3. Cole abaixo após o sinal de igual

SUPABASE_SERVICE_KEY=
"""
        ENV_PATH.write_text(env_template, encoding='utf-8')
        ok(f"Criado: {ENV_PATH.name}")
        warn(f"Edite {ENV_PATH.name} e adicione sua service_role key do Supabase")

    # .gitignore — garantir que .env não vai pro Git
    gitignore = ROOT / ".gitignore"
    needed = [".env", "__pycache__/", "*.pyc"]
    existing = gitignore.read_text(encoding='utf-8').splitlines() if gitignore.exists() else []
    to_add = [x for x in needed if x not in existing]
    if to_add:
        with gitignore.open('a', encoding='utf-8') as f:
            if existing and existing[-1].strip():
                f.write('\n')
            f.write('\n# deploy.py\n' + '\n'.join(to_add) + '\n')
        ok(f"Atualizado: .gitignore (adicionado: {', '.join(to_add)})")

    print()
    info("Pronto! Próximos passos:")
    print("  1. Edite .env e adicione sua SUPABASE_SERVICE_KEY")
    print("  2. Confira deploy.config.json e ajuste se quiser")
    print("  3. Rode: python deploy.py")

# ============================================================
# MAIN
# ============================================================
def main():
    p = argparse.ArgumentParser(description="Deploy da Torre de Controle")
    p.add_argument('--so-supabase', action='store_true', help='Deploy apenas no Supabase')
    p.add_argument('--so-github', action='store_true', help='Deploy apenas no GitHub')
    p.add_argument('--dry-run', action='store_true', help='Mostra o que faria sem executar')
    p.add_argument('--msg', help='Mensagem do commit')
    p.add_argument('--init', action='store_true', help='Cria deploy.config.json e .env iniciais')
    args = p.parse_args()

    if args.init:
        init_config()
        return

    cfg = load_config()
    env = load_env()

    msg = args.msg or cfg.get('default_commit_message') or 'deploy: atualização'
    msg = f"{msg} ({dt.datetime.now().strftime('%d/%m/%Y %H:%M')})"

    errors = 0

    if not args.so_github:
        errors += supabase_deploy(cfg, env, dry_run=args.dry_run)

    if not args.so_supabase:
        errors += git_deploy(cfg, msg, dry_run=args.dry_run)

    print()
    if errors == 0:
        ok("Deploy concluído com sucesso!")
    else:
        err(f"Deploy concluído com {errors} erro(s)")
        sys.exit(1)

if __name__ == '__main__':
    main()
