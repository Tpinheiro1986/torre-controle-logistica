# -*- coding: utf-8 -*-
"""
Torre de Controle - Simulador S&OP / S&OE
Atualizador automatico: le os arquivos da pasta _entrada, grava no Supabase
e (se houver index.html novo) publica o painel via git.

Uso:  duplo clique em ATUALIZAR.bat
      ou:  python atualizador.py [--dry-run] [--sim]
"""
import sys, os, io, json, glob, shutil, getpass, subprocess, datetime, traceback

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

AQUI = os.path.dirname(os.path.abspath(__file__))
DRY = '--dry-run' in sys.argv
AUTO_SIM = '--sim' in sys.argv          # responde S automaticamente
OFFLINE = '--offline' in sys.argv       # nao conecta no Supabase (so le os arquivos)

MESES_PT = ['Janeiro', 'Fevereiro', 'Marco', 'Abril', 'Maio', 'Junho',
            'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
MESES_ABR3 = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN',
              'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ']

LOG_LINHAS = []


def log(msg=''):
    print(msg)
    LOG_LINHAS.append(str(msg))


def titulo(t):
    log('')
    log('=' * 66)
    log('  ' + t)
    log('=' * 66)


def norm(t):
    """Normaliza rotulo de coluna: minusculo, sem acento, so letras e numeros."""
    import unicodedata
    s = str('' if t is None else t)
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return ''.join(c for c in s.lower() if c.isalnum())


def parar(msg, codigo=1):
    log('')
    log('ERRO: ' + msg)
    gravar_log()
    if not AUTO_SIM:
        input('\nPressione ENTER para fechar...')
    sys.exit(codigo)


# ============================================================
# CONFIG
# ============================================================
def carregar_config():
    p = os.path.join(AQUI, 'config.json')
    if not os.path.exists(p):
        parar('config.json nao encontrado ao lado do atualizador.')
    with io.open(p, encoding='utf-8') as f:
        return json.load(f)


# ============================================================
# DEPENDENCIAS
# ============================================================
def checar_dependencias():
    faltando = []
    for mod, pkg in [('pandas', 'pandas'), ('openpyxl', 'openpyxl'),
                     ('xlrd', 'xlrd'), ('requests', 'requests')]:
        try:
            __import__(mod)
        except ImportError:
            faltando.append(pkg)
    if faltando:
        log('Instalando bibliotecas faltantes: ' + ', '.join(faltando))
        r = subprocess.run([sys.executable, '-m', 'pip', 'install', '--quiet'] + faltando)
        if r.returncode != 0:
            parar('Falha ao instalar: ' + ', '.join(faltando) +
                  '\nRode manualmente:  pip install ' + ' '.join(faltando))
        log('Bibliotecas instaladas.')


# ============================================================
# CLASSIFICACAO DAS PLANILHAS
# ============================================================
def achar_cabecalho(df_bruto, assinaturas, limite=15):
    """Procura nas primeiras linhas uma que contenha TODAS as colunas de alguma assinatura.
    Retorna (indice_linha, nome_da_assinatura) ou (None, None)."""
    for i in range(min(limite, len(df_bruto))):
        linha = [norm(v) for v in list(df_bruto.iloc[i].values)]
        for nome, obrigatorias in assinaturas:
            if all(any(o == c for c in linha) for o in obrigatorias):
                return i, nome
    return None, None


ASSINATURAS = [
    ('faturamento', ['coditem', 'qtde', 'data']),
    ('faturamento', ['coditem', 'qtde', 'dtemissao']),
    ('cadastro',    ['coditem', 'undppalletcd']),
    ('estoque',     ['item', 'qtdliquida']),
    ('sop',         ['codigo', 'sku', 'abril']),
    ('sop',         ['coditem', 'abril']),
    ('entrada',     ['codigo', 'sku', 'mai26']),
    ('entrada',     ['codigo', 'sku', 'jun26']),
]


def classificar_arquivo(caminho):
    """Devolve lista de (aba, tipo, linha_cabecalho)."""
    import pandas as pd
    achados = []
    try:
        xl = pd.ExcelFile(caminho)
    except Exception as e:
        log('  ! nao consegui abrir: %s' % e)
        return achados
    for aba in xl.sheet_names:
        try:
            bruto = pd.read_excel(xl, aba, header=None, nrows=20)
        except Exception:
            continue
        if bruto.empty:
            continue
        i, tipo = achar_cabecalho(bruto, ASSINATURAS)
        if tipo:
            achados.append((aba, tipo, i))
    return achados


# ============================================================
# LEITORES
# ============================================================
def num(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == '':
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def cod_str(v):
    import pandas as pd
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ''
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    return str(v).strip()


def col(df, *nomes):
    """Acha a coluna do DataFrame por qualquer um dos nomes (normalizado)."""
    alvo = [norm(n) for n in nomes]
    for c in df.columns:
        if norm(c) in alvo:
            return c
    return None


def ler_plano(caminho, aba, header, ano, tipo):
    """S&OP (saida) ou Forecast Pedidos (entrada) -> {(cod, mes): qty}"""
    import pandas as pd
    df = pd.read_excel(caminho, aba, header=header)
    c_cod = col(df, 'Codigo', 'CODIGO', 'Cod Item', 'Cod. Item')
    if c_cod is None:
        return {}, set(), 'coluna de codigo nao encontrada'
    # mapeia colunas de mes
    mapa = {}
    for c in df.columns:
        n = norm(c)
        for i, m in enumerate(MESES_PT):
            if n == norm(m):
                mapa[c] = i + 1
        for i, a in enumerate(MESES_ABR3):
            if n == norm(a + '-' + str(ano)[-2:]) or n == norm(a + str(ano)[-2:]):
                mapa[c] = i + 1
    if not mapa:
        return {}, set(), 'nenhuma coluna de mes reconhecida'
    dados, meses = {}, set()
    for _, r in df.iterrows():
        cod = cod_str(r[c_cod])
        if not cod or cod.upper() == 'TOTAL' or cod == 'nan':
            continue
        for c, m in mapa.items():
            v = num(r[c])
            if v > 0:
                dados[(cod, m)] = dados.get((cod, m), 0.0) + v
            meses.add(m)
    return dados, meses, None


def ler_estoque(caminho, aba, header, campo_qtd):
    """Estoque Atual (uma linha por lote) -> {cod: qty somada}"""
    import pandas as pd
    df = pd.read_excel(caminho, aba, header=header)
    c_cod = col(df, 'Item', 'Cod Item', 'Codigo')
    c_qtd = col(df, campo_qtd, 'Qtd Liquida')
    if c_cod is None or c_qtd is None:
        return {}, 'colunas de item/quantidade nao encontradas'
    out = {}
    for _, r in df.iterrows():
        cod = cod_str(r[c_cod])
        if not cod or cod == 'nan':
            continue
        out[cod] = out.get(cod, 0.0) + num(r[c_qtd])
    return out, None


def ler_cadastro(caminho, aba, header, marcas_inovalab):
    import pandas as pd
    df = pd.read_excel(caminho, aba, header=header)
    c_cod = col(df, 'Cod Item')
    c_desc = col(df, 'Descricao Item', 'Descricao')
    c_marca = col(df, 'Marca')
    c_upp = col(df, 'Und p/ Pallet CD')
    c_upp2 = col(df, 'Maximo Und p/ Pallet')
    c_st = col(df, 'Status Supply')
    c_g14 = col(df, 'GTIN14')
    if c_cod is None or c_upp is None:
        return [], 'colunas Cod Item / Und p/ Pallet CD nao encontradas'
    chaves_ino = [norm(m) for m in marcas_inovalab]
    out, vistos = [], set()
    for _, r in df.iterrows():
        cod = cod_str(r[c_cod])
        if not cod or cod == 'nan' or cod in vistos:
            continue
        vistos.add(cod)
        marca = '' if c_marca is None else str(r[c_marca] or '').strip()
        desc = '' if c_desc is None else str(r[c_desc] or '').strip()
        alvo = norm(marca) + norm(desc)
        emp = 'INOVALAB' if any(k and k in alvo for k in chaves_ino) else 'GENOMMA'
        upp = int(num(r[c_upp])) or (int(num(r[c_upp2])) if c_upp2 is not None else 0)
        reg = {'cod_item': cod, 'descricao': desc, 'marca': marca, 'empresa': emp,
               'upp_cadastro': upp or None, 'upp_efetiva': upp or 2000}
        if c_st is not None:
            v = r[c_st]
            reg['status_supply'] = None if pd.isna(v) else str(v).strip()
        if c_g14 is not None:
            v = r[c_g14]
            reg['gtin14'] = None if pd.isna(v) else cod_str(v)
        out.append(reg)
    return out, None


def ler_faturamento(caminho, aba, header, ano, cfop_venda, cfop_bonif):
    """FT4003 linha a linha -> {(cod, mes): {'venda':x, 'bonif':y}}"""
    import pandas as pd
    df = pd.read_excel(caminho, aba, header=header)
    c_cod = col(df, 'COD ITEM', 'Cod Item')
    c_qtd = col(df, 'QTDE', 'Qtde', 'Quantidade')
    c_data = col(df, 'DATA', 'Dt Emissao', 'Data Emissao')
    c_cfop = col(df, 'CFOP')
    if c_cod is None or c_qtd is None or c_data is None:
        return {}, set(), {}, 'colunas COD ITEM / QTDE / DATA nao encontradas'
    dados, meses = {}, set()
    stats = {'lidas': 0, 'ign_cfop': 0, 'ign_data': 0, 'venda': 0.0, 'bonif': 0.0}
    vend = set(cfop_venda)
    boni = set(cfop_bonif)
    for _, r in df.iterrows():
        cod = cod_str(r[c_cod])
        if not cod or cod == 'nan' or cod.upper().startswith('ICMS'):
            continue
        d = r[c_data]
        if pd.isna(d):
            stats['ign_data'] += 1
            continue
        try:
            d = pd.to_datetime(d)
        except Exception:
            stats['ign_data'] += 1
            continue
        if d.year != ano:
            stats['ign_data'] += 1
            continue
        cf = int(num(r[c_cfop])) if c_cfop is not None else 0
        eh_b = cf in boni
        eh_v = cf in vend
        if c_cfop is not None and not eh_v and not eh_b:
            stats['ign_cfop'] += 1
            continue
        q = num(r[c_qtd])
        if not q:
            continue
        k = (cod, d.month)
        if k not in dados:
            dados[k] = {'venda': 0.0, 'bonif': 0.0}
        if eh_b:
            dados[k]['bonif'] += q
            stats['bonif'] += q
        else:
            dados[k]['venda'] += q
            stats['venda'] += q
        meses.add(d.month)
        stats['lidas'] += 1
    return dados, meses, stats, None


# ============================================================
# SUPABASE (REST + JWT do admin)
# ============================================================
class Supa:
    def __init__(self, url, anon, email, senha):
        import requests
        self.s = requests.Session()
        self.url = url.rstrip('/')
        self.anon = anon
        r = self.s.post(self.url + '/auth/v1/token?grant_type=password',
                        headers={'apikey': anon, 'Content-Type': 'application/json'},
                        json={'email': email, 'password': senha}, timeout=30)
        if r.status_code != 200:
            raise RuntimeError('login recusado (%s): %s' % (r.status_code, r.text[:200]))
        self.token = r.json()['access_token']

    def h(self, extra=None):
        d = {'apikey': self.anon, 'Authorization': 'Bearer ' + self.token,
             'Content-Type': 'application/json'}
        if extra:
            d.update(extra)
        return d

    def delete(self, tabela, filtros):
        q = '&'.join('%s=%s' % (k, v) for k, v in filtros.items())
        r = self.s.delete('%s/rest/v1/%s?%s' % (self.url, tabela, q), headers=self.h(), timeout=60)
        if r.status_code >= 300:
            raise RuntimeError('DELETE %s: %s %s' % (tabela, r.status_code, r.text[:200]))

    def insert(self, tabela, linhas, upsert=False, lote=500):
        extra = {'Prefer': 'return=minimal' + (',resolution=merge-duplicates' if upsert else '')}
        for i in range(0, len(linhas), lote):
            parte = linhas[i:i + lote]
            r = self.s.post('%s/rest/v1/%s' % (self.url, tabela),
                            headers=self.h(extra), json=parte, timeout=120)
            if r.status_code >= 300:
                raise RuntimeError('INSERT %s lote %d: %s %s'
                                   % (tabela, i // lote + 1, r.status_code, r.text[:300]))

    def select_col(self, tabela, coluna, lote=1000):
        """Lista todos os valores de uma coluna, paginando."""
        out, de = [], 0
        while True:
            r = self.s.get('%s/rest/v1/%s?select=%s' % (self.url, tabela, coluna),
                           headers=self.h({'Range-Unit': 'items',
                                           'Range': '%d-%d' % (de, de + lote - 1)}), timeout=60)
            if r.status_code >= 300:
                return out
            parte = r.json()
            out += [x[coluna] for x in parte]
            if len(parte) < lote:
                break
            de += lote
        return out

    def existe(self, tabela):
        r = self.s.get('%s/rest/v1/%s?select=*&limit=1' % (self.url, tabela),
                       headers=self.h(), timeout=30)
        return r.status_code < 300


# ============================================================
# GIT
# ============================================================
def git(repo, *args):
    r = subprocess.run(['git'] + list(args), cwd=repo,
                       capture_output=True, text=True, encoding='utf-8', errors='replace')
    return r.returncode, (r.stdout or '') + (r.stderr or '')


def publicar_html(repo, destino_rel, origem, msg):
    if not os.path.isdir(os.path.join(repo, '.git')):
        return False, 'pasta do repositorio nao tem .git: ' + repo
    cod, out = git(repo, '--version')
    if cod != 0:
        return False, 'git nao encontrado no PATH. Instale o Git for Windows.'
    destino = os.path.join(repo, destino_rel.replace('/', os.sep))
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    if os.path.exists(destino):
        bkp = destino + '.bak'
        shutil.copy2(destino, bkp)
    shutil.copy2(origem, destino)
    cod, out = git(repo, 'add', '--', destino_rel)
    if cod != 0:
        return False, 'git add falhou: ' + out
    cod, out = git(repo, 'commit', '-m', msg)
    if cod != 0 and 'nothing to commit' in out.lower():
        return True, 'nada mudou no index.html (commit nao necessario)'
    if cod != 0:
        return False, 'git commit falhou: ' + out
    cod, out = git(repo, 'push')
    if cod != 0:
        return False, 'git push falhou: ' + out + \
                      '\n(o commit ficou local; rode "git push" na mao depois de resolver)'
    return True, 'index.html publicado e enviado para o GitHub'


# ============================================================
# LOG
# ============================================================
def gravar_log():
    try:
        pasta = os.path.join(AQUI, '_logs')
        os.makedirs(pasta, exist_ok=True)
        nome = 'atualizacao_%s.txt' % datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
        with io.open(os.path.join(pasta, nome), 'w', encoding='utf-8') as f:
            f.write('\n'.join(LOG_LINHAS))
    except Exception:
        pass


# ============================================================
# PRINCIPAL
# ============================================================
def main():
    cfg = carregar_config()
    ano = int(cfg.get('ano', 2026))
    horiz = str(cfg.get('horizonte', ano))
    entrada = os.path.join(AQUI, cfg.get('pasta_entrada', '_entrada'))
    processados = os.path.join(AQUI, cfg.get('pasta_processados', '_processados'))
    os.makedirs(entrada, exist_ok=True)
    os.makedirs(processados, exist_ok=True)

    titulo('TORRE DE CONTROLE - ATUALIZADOR S&OP / S&OE')
    log('Pasta de entrada : ' + entrada)
    log('Ano / horizonte  : %d / %s' % (ano, horiz))
    if DRY:
        log('MODO SIMULACAO (--dry-run): nada sera gravado.')

    checar_dependencias()

    arquivos = []
    for ext in ('*.xlsx', '*.xls', '*.xlsm'):
        arquivos += glob.glob(os.path.join(entrada, ext))
    arquivos = [a for a in arquivos if not os.path.basename(a).startswith('~$')]
    htmls = glob.glob(os.path.join(entrada, '*.html'))

    if not arquivos and not htmls:
        log('')
        log('Nenhum arquivo em _entrada.')
        log('Cole ali as planilhas (S&OP, Forecast Pedidos, Estoque, Cadastro,')
        log('relatorio FT4003) e/ou um index.html novo, e rode de novo.')
        gravar_log()
        if not AUTO_SIM:
            input('\nPressione ENTER para fechar...')
        return

    # ---------- 1. LEITURA ----------
    titulo('1. LENDO ARQUIVOS')
    plano_saida, meses_saida = {}, set()
    plano_entrada, meses_entrada = {}, set()
    estoque = {}
    cadastro = []
    fatur, meses_fat, stats_fat = {}, set(), None
    origens = []

    for arq in sorted(arquivos):
        nome = os.path.basename(arq)
        log('')
        log('> ' + nome)
        achados = classificar_arquivo(arq)
        if not achados:
            log('  (nenhuma aba reconhecida - arquivo ignorado)')
            continue
        for aba, tipo, hdr in achados:
            if tipo == 'sop':
                d, ms, err = ler_plano(arq, aba, hdr, ano, tipo)
                if err:
                    log('  aba "%s" [S&OP] -> %s' % (aba, err)); continue
                plano_saida.update(d); meses_saida |= ms
                log('  aba "%s" -> S&OP (saida): %d SKUs, meses %s'
                    % (aba, len(set(k[0] for k in d)), ','.join(MESES_ABR3[m-1] for m in sorted(ms))))
            elif tipo == 'entrada':
                d, ms, err = ler_plano(arq, aba, hdr, ano, tipo)
                if err:
                    log('  aba "%s" [Forecast] -> %s' % (aba, err)); continue
                plano_entrada.update(d); meses_entrada |= ms
                log('  aba "%s" -> Forecast Pedidos (entrada): %d SKUs, meses %s'
                    % (aba, len(set(k[0] for k in d)), ','.join(MESES_ABR3[m-1] for m in sorted(ms))))
            elif tipo == 'estoque':
                d, err = ler_estoque(arq, aba, hdr, cfg.get('campo_estoque', 'Qtd Liquida'))
                if err:
                    log('  aba "%s" [Estoque] -> %s' % (aba, err)); continue
                estoque.update(d)
                log('  aba "%s" -> Estoque Atual: %d SKUs, %s UN'
                    % (aba, len(d), format(int(sum(d.values())), ',d').replace(',', '.')))
            elif tipo == 'cadastro':
                d, err = ler_cadastro(arq, aba, hdr, cfg.get('marcas_inovalab', []))
                if err:
                    log('  aba "%s" [Cadastro] -> %s' % (aba, err)); continue
                cadastro = d
                ino = sum(1 for x in d if x['empresa'] == 'INOVALAB')
                log('  aba "%s" -> Cadastro SKU: %d itens (%d Inovalab / %d Genomma)'
                    % (aba, len(d), ino, len(d) - ino))
            elif tipo == 'faturamento':
                d, ms, st, err = ler_faturamento(arq, aba, hdr, ano,
                                                 cfg['cfop_venda'], cfg['cfop_bonificacao'])
                if err:
                    log('  aba "%s" [FT4003] -> %s' % (aba, err)); continue
                for k, v in d.items():
                    if k not in fatur:
                        fatur[k] = {'venda': 0.0, 'bonif': 0.0}
                    fatur[k]['venda'] += v['venda']
                    fatur[k]['bonif'] += v['bonif']
                meses_fat |= ms
                stats_fat = st
                log('  aba "%s" -> Faturamento (FT4003)' % aba)
                log('     linhas aproveitadas : %s' % format(st['lidas'], ',d').replace(',', '.'))
                log('     ignoradas por CFOP  : %s' % format(st['ign_cfop'], ',d').replace(',', '.'))
                log('     ignoradas por data  : %s' % format(st['ign_data'], ',d').replace(',', '.'))
                log('     venda        : %s UN' % format(int(st['venda']), ',d').replace(',', '.'))
                log('     bonificacao  : %s UN' % format(int(st['bonif']), ',d').replace(',', '.'))
        origens.append(arq)

    # ---------- 2. CONECTAR (antes do resumo, para falhar cedo) ----------
    sb = None
    skus_no_banco = set()
    if not OFFLINE:
        titulo('2. CONECTANDO NO SUPABASE')
        email = cfg['admin_email']
        log('Usuario: ' + email)
        senha = os.environ.get('TORRE_SENHA') or getpass.getpass('Senha (nao fica salva): ')
        try:
            sb = Supa(cfg['supabase_url'], cfg['supabase_anon'], email, senha)
        except Exception as e:
            parar('nao consegui autenticar.\n%s' % e)
        log('Autenticado.')
        skus_no_banco = set(sb.select_col('sop_sku', 'cod_item'))
        log('SKUs hoje no painel: %d' % len(skus_no_banco))
    else:
        log('')
        log('MODO OFFLINE: sem conexao, o resumo usa apenas os arquivos.')

    # ---------- 2b. FILTRAR CADASTRO ----------
    cadastro_fora = 0
    if cadastro and cfg.get('cadastro_somente_relevantes', True):
        relevantes = set(skus_no_banco)
        relevantes |= set(c for c, _ in plano_saida)
        relevantes |= set(c for c, _ in plano_entrada)
        relevantes |= set(estoque)
        relevantes |= set(c for c, _ in fatur)
        antes = len(cadastro)
        cadastro = [x for x in cadastro if x['cod_item'] in relevantes]
        cadastro_fora = antes - len(cadastro)

    # ---------- 3. RESUMO ----------
    titulo('3. O QUE VAI SER FEITO')
    acoes = []
    if cadastro:
        extra = ''
        novos = len([x for x in cadastro if x['cod_item'] not in skus_no_banco]) if skus_no_banco else 0
        if novos:
            extra = ', %d SKU novos entram no painel' % novos
        acoes.append('Cadastro SKU        -> sop_sku            : %d itens (upsert%s)' % (len(cadastro), extra))
    if plano_saida:
        ms = ','.join(MESES_ABR3[m-1] for m in sorted(meses_saida))
        acoes.append('S&OP (saida)        -> sop_plano_saida    : %d pontos, substitui os meses %s'
                     % (len(plano_saida), ms))
    if plano_entrada:
        ms = ','.join(MESES_ABR3[m-1] for m in sorted(meses_entrada))
        acoes.append('Forecast Pedidos    -> sop_plano_entrada  : %d pontos, substitui os meses %s'
                     % (len(plano_entrada), ms))
    if estoque:
        acoes.append('Estoque Atual       -> sop_estoque        : %d SKUs, foto de %s'
                     % (len(estoque), datetime.date.today().isoformat()))
    if fatur:
        ms = ','.join(MESES_ABR3[m-1] for m in sorted(meses_fat))
        acoes.append('Faturamento real    -> sop_faturamento_real: %d pontos, substitui os meses %s'
                     % (len(fatur), ms))
    if htmls:
        acoes.append('Painel              -> git push          : %s' % os.path.basename(htmls[0]))
    if not acoes:
        log('Nada reconhecido para atualizar.')
        gravar_log()
        if not AUTO_SIM:
            input('\nPressione ENTER para fechar...')
        return
    for a in acoes:
        log('  * ' + a)
    log('')
    log('Regra: so os meses presentes nos arquivos sao substituidos.')
    log('Os demais meses continuam intactos no banco.')

    if cadastro_fora:
        log('')
        log('Aviso: %d itens do Cadastro SKU ficaram de fora por nao terem plano,' % cadastro_fora)
        log('       estoque nem faturamento. Para trazer todos, mude')
        log('       "cadastro_somente_relevantes" para false no config.json.')

    if estoque and skus_no_banco:
        sem_foto = sorted(skus_no_banco - set(estoque))
        if sem_foto:
            log('')
            log('Aviso: %d SKUs do painel nao aparecem no arquivo de estoque e vao' % len(sem_foto))
            log('       continuar com a foto anterior (o painel usa a data mais recente')
            log('       de cada SKU). Exemplos: %s' % ', '.join(sem_foto[:8]))

    if DRY:
        log('')
        log('MODO SIMULACAO - encerrando sem gravar.')
        gravar_log()
        return

    if not AUTO_SIM:
        log('')
        resp = input('Confirma? (S/N) ').strip().lower()
        if resp not in ('s', 'sim', 'y'):
            log('Cancelado pelo usuario.')
            gravar_log()
            return

    if sb is None:
        parar('modo offline: nao da para gravar. Rode sem --offline.')

    if fatur and not sb.existe('sop_faturamento_real'):
        parar('a tabela sop_faturamento_real nao existe no Supabase.\n'
              'Rode o arquivo sop_faturamento_real.sql no SQL Editor e tente de novo.')

    # ---------- 4. GRAVACAO ----------
    titulo('4. GRAVANDO')
    try:
        if cadastro:
            sb.insert('sop_sku', cadastro, upsert=True)
            log('  ok  sop_sku (%d itens)' % len(cadastro))

        for tabela, dados, meses in (('sop_plano_saida', plano_saida, meses_saida),
                                     ('sop_plano_entrada', plano_entrada, meses_entrada)):
            if not dados:
                continue
            for m in sorted(meses):
                ym = '%d-%02d-01' % (ano, m)
                sb.delete(tabela, {'horizonte': 'eq.' + horiz, 'year_month': 'eq.' + ym})
            linhas = [{'cod_item': c, 'year_month': '%d-%02d-01' % (ano, m),
                       'qty_un': round(v, 3), 'horizonte': horiz}
                      for (c, m), v in sorted(dados.items())]
            sb.insert(tabela, linhas)
            log('  ok  %s (%d pontos)' % (tabela, len(linhas)))

        if estoque:
            hoje = datetime.date.today().isoformat()
            linhas = [{'cod_item': c, 'snapshot_date': hoje, 'qty_un': round(v, 3)}
                      for c, v in sorted(estoque.items())]
            sb.insert('sop_estoque', linhas, upsert=True)
            log('  ok  sop_estoque (%d SKUs, foto %s)' % (len(linhas), hoje))

        if fatur:
            for m in sorted(meses_fat):
                ym = '%d-%02d-01' % (ano, m)
                sb.delete('sop_faturamento_real', {'horizonte': 'eq.' + horiz, 'year_month': 'eq.' + ym})
            linhas = [{'cod_item': c, 'year_month': '%d-%02d-01' % (ano, m),
                       'horizonte': horiz, 'qty_un': round(v['venda'], 3),
                       'qty_bonif': round(v['bonif'], 3), 'fonte': 'FT4003'}
                      for (c, m), v in sorted(fatur.items())]
            sb.insert('sop_faturamento_real', linhas)
            log('  ok  sop_faturamento_real (%d pontos)' % len(linhas))
    except Exception as e:
        parar('falha ao gravar no Supabase.\n%s' % e)

    # ---------- 5. PUBLICAR HTML ----------
    if htmls:
        titulo('5. PUBLICANDO O PAINEL')
        repo = cfg['repo_local']
        if not os.path.isdir(repo):
            log('  ! pasta do repositorio nao encontrada: ' + repo)
            log('    ajuste "repo_local" no config.json')
        else:
            msg = cfg.get('mensagem_commit', 'Atualiza simulador S&OP') + \
                  ' - ' + datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
            ok, detalhe = publicar_html(repo, cfg['destino_html'], htmls[0], msg)
            log(('  ok  ' if ok else '  !   ') + detalhe)

    # ---------- 6. ARQUIVAR ----------
    titulo('6. ARQUIVANDO OS ARQUIVOS PROCESSADOS')
    carimbo = datetime.datetime.now().strftime('%Y-%m-%d_%H%M')
    destino = os.path.join(processados, carimbo)
    os.makedirs(destino, exist_ok=True)
    for a in origens + htmls:
        try:
            shutil.move(a, os.path.join(destino, os.path.basename(a)))
            log('  movido: ' + os.path.basename(a))
        except Exception as e:
            log('  ! nao consegui mover %s: %s' % (os.path.basename(a), e))

    titulo('CONCLUIDO')
    log('O painel ja esta com os dados novos. Abra e de Ctrl+Shift+R.')
    gravar_log()
    if not AUTO_SIM:
        input('\nPressione ENTER para fechar...')


if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        log('')
        log('ERRO INESPERADO:')
        log(traceback.format_exc())
        gravar_log()
        if not AUTO_SIM:
            input('\nPressione ENTER para fechar...')
        sys.exit(1)
