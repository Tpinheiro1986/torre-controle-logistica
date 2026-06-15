#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Parser do export analitico (texto pdftotext -layout) -> tabelas.json
Reconstroi o schema usado pelo simulador Torre de Controle."""
import re, json, unicodedata, sys

SRC = sys.argv[1] if len(sys.argv) > 1 else 'novo.txt'

import ftfy
def fix(s):
    """Conserta mojibake (CP1252/UTF-8) via ftfy + caso especial do 'í'
    que o pdftotext quebrou ('Ã'+hífen por perda do soft-hyphen)."""
    s = ftfy.fix_text(s)
    if 'Ã' in s:
        s = s.replace('Ã-', 'í').replace('Ã­', 'í')
    return s

def to_num(s):
    """'1.234,56' -> 1234.56 ; '300,00' -> 300.0"""
    s = s.strip().replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return None

def strip_acc(s):
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

CNPJ_RE = re.compile(r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}')
CIDUF_RE = re.compile(r'^.+-[A-Z]{2}$')

# carrega linhas (UTF-8) e conserta encoding linha a linha
raw_lines = open(SRC, encoding='utf-8').read().split('\n')
lines = [fix(l).replace('\f', '').rstrip() for l in raw_lines]

# ---- localizar blocos de tabela ----
tomador_idx = [i for i, l in enumerate(lines) if l.strip().startswith('Tomador:')]
blocks = []
for k, ti in enumerate(tomador_idx):
    # nome = primeira linha nao-vazia acima de Tomador
    j = ti - 1
    while j >= 0 and not lines[j].strip():
        j -= 1
    name = lines[j].strip()
    start = j
    end = tomador_idx[k + 1] - 1 if k + 1 < len(tomador_idx) else len(lines)
    # recuar o end ate antes do nome da proxima tabela
    if k + 1 < len(tomador_idx):
        jn = tomador_idx[k + 1] - 1
        while jn >= 0 and not lines[jn].strip():
            jn -= 1
        end = jn  # exclui a linha do nome seguinte
    blocks.append((name, start, end))

# ---------- parsers de taxa ----------
def normaliza_nome_taxa(n):
    n = n.strip()
    nu = strip_acc(n).upper()
    nu = re.sub(r'\s+', ' ', nu).strip()
    mapa = {
        'EMEX %': 'EMEX%', 'EMEX%': 'EMEX%',
        'EMEX FRACAO': 'EMEX_FRACAO',
        'FRETE VALOR': 'FRETEVALOR',
        'AD VALOREM': 'ADVALOREM', 'ADVALOREN': 'ADVALOREM', 'AD VALOREN': 'ADVALOREM',
    }
    return mapa.get(nu, nu)

# regex de spec de taxa
RE_PCT_NF = re.compile(r'([\d.,]+)\s*%\s*sobre\s*o\s*valor\s*da\s*nota\s*fiscal', re.I)
RE_PCT_FRETE = re.compile(r'([\d.,]+)\s*%\s*sobre\s*o\s*(?:total\s*)?valor\s*do\s*frete', re.I)
RE_FRACAO = re.compile(r'R\$\s*([\d.,]+)\s*por\s*fra[cç][aã]o\s*de\s*([\d.,]+)', re.I)
RE_DOC = re.compile(r'R\$\s*([\d.,]+)\s*por\s*documento', re.I)
RE_MIN = re.compile(r'Valor\s*m[ií]nimo\s*de\s*R\$\s*([\d.,]+)', re.I)
# nome da taxa: comeca em col0 com letra; nome = run de letras/%/espaco ate o 1o digito ou R$
RE_NAME = re.compile(r'^([A-Za-zÀ-ÿ%][A-Za-zÀ-ÿ %]*?)\s+(?=\d|R\$)')

def is_tax_start(ln):
    """Se a linha inicia uma taxa, retorna (nome_normalizado, spec_dict); senao None."""
    if not ln or ln[0] == ' ':
        return None  # taxa comeca na coluna 0
    if not re.match(r'[A-Za-zÀ-ÿ]', ln):
        return None  # linhas de continuacao de CNPJ comecam com digito
    sp = parse_spec(ln)
    if sp is None:
        return None  # linhas de cidade/UF/'Aplicar quando' nao tem spec
    mn = RE_NAME.match(ln)
    if not mn:
        return None
    return normaliza_nome_taxa(mn.group(1)), sp

def parse_spec(spec):
    d = None
    m = RE_PCT_NF.search(spec)
    if m:
        d = {'tipo': 'PERCENT_NF', 'percentual': to_num(m.group(1))}
    if d is None:
        m = RE_PCT_FRETE.search(spec)
        if m:
            d = {'tipo': 'PERCENT_FRETE', 'percentual': to_num(m.group(1))}
    if d is None:
        m = RE_FRACAO.search(spec)
        if m:
            d = {'tipo': 'POR_FRACAO', 'valor': to_num(m.group(1)), 'fracao_kg': to_num(m.group(2))}
    if d is None:
        m = RE_DOC.search(spec)
        if m:
            d = {'tipo': 'VALOR_DOC', 'valor': to_num(m.group(1))}
    if d is not None:
        mm = RE_MIN.search(spec)
        if mm:
            d['minimo'] = to_num(mm.group(1))
    return d

def parse_taxas_gerais(seg):
    """seg = lista de linhas da regiao 'Taxas'..(antes da 1a Rota). Retorna lista de taxas."""
    taxas = []
    i = 0
    cur = None  # taxa atual (para anexar restricoes)
    restr_mode = None  # 'cidade'|'cnpj'|'uf'
    buf_cidade, buf_cnpj, buf_uf = [], [], []

    def flush_restr():
        if cur is None:
            return
        if buf_cidade:
            cids = []
            txt = ' '.join(buf_cidade)
            for part in txt.split(','):
                p = part.strip()
                if p and CIDUF_RE.match(p):
                    cids.append(p)
            if cids:
                cur['aplicar_cidades'] = cids
        if buf_uf:
            ufs = []
            for part in ' '.join(buf_uf).split(','):
                p = part.strip()
                if re.fullmatch(r'[A-Z]{2}', p):
                    ufs.append(p)
            if ufs:
                cur['aplicar_ufs'] = ufs
        if buf_cnpj:
            cnt = len(CNPJ_RE.findall(' '.join(buf_cnpj)))
            if cnt:
                cur['aplicar_cnpjs_count'] = cnt

    while i < len(seg):
        ln = seg[i]
        s = ln.strip()
        if not s:
            i += 1
            continue
        # nova taxa?
        ts = is_tax_start(ln)
        if ts is not None:
            # fecha restricoes da taxa anterior
            flush_restr()
            buf_cidade, buf_cnpj, buf_uf = [], [], []
            restr_mode = None
            nome, spec = ts
            cur = {'nome': nome}
            cur.update(spec)
            taxas.append(cur)
            i += 1
            continue
        # bloco Aplicar quando
        if s.startswith('Aplicar quando'):
            restr_mode = None
            i += 1
            continue
        if s.startswith('- Cidade') or s.startswith('-Cidade'):
            restr_mode = 'cidade'
            buf_cidade.append(s.split(':', 1)[-1].strip())
            i += 1
            continue
        if s.startswith('- UF') or s.startswith('-UF'):
            restr_mode = 'uf'
            buf_uf.append(s.split(':', 1)[-1].strip())
            i += 1
            continue
        if s.startswith('- CNPJ') or s.startswith('-CNPJ'):
            restr_mode = 'cnpj'
            buf_cnpj.append(s.split(':', 1)[-1].strip())
            i += 1
            continue
        # continuacao de restricao
        if restr_mode == 'cidade':
            buf_cidade.append(s)
        elif restr_mode == 'cnpj':
            buf_cnpj.append(s)
        elif restr_mode == 'uf':
            buf_uf.append(s)
        i += 1
    flush_restr()
    return taxas

# ---------- parser de rotas ----------
RE_ROTA = re.compile(r'^Rota\s+(\d+):\s*(.*)$')
RE_ATE = re.compile(r'At[eé]\s*([\d.,]+)\s*Kg\s*:\s*R\$\s*([\d.,]+)', re.I)
RE_ACIMA_MULT = re.compile(r'Acima\s*de\s*([\d.,]+)\s*Kg\s*:\s*Multiplicar\s*por\s*([\d.,]+)', re.I)

def parse_lista_cidades(linhas):
    out = []
    txt = ' '.join(linhas)
    for part in txt.split(','):
        p = part.strip()
        if p and CIDUF_RE.match(p):
            out.append(p)
    return out

def parse_rotas(seg):
    rotas = []
    # indices de inicio de rota
    starts = [i for i, l in enumerate(seg) if RE_ROTA.match(l.strip())]
    for k, si in enumerate(starts):
        ei = starts[k + 1] if k + 1 < len(starts) else len(seg)
        chunk = seg[si:ei]
        m = RE_ROTA.match(chunk[0].strip())
        rid, desc = m.group(1), m.group(2).strip()
        origens, destinos, regras, taxas = [], [], [], []
        mode = None
        buf_o, buf_d = [], []
        taxa_seg = []
        for ln in chunk[1:]:
            s = ln.strip()
            if not s:
                continue
            if s.startswith('Cidades/Localidades Origem:'):
                mode = 'orig'
                buf_o.append(s.split(':', 1)[-1].strip())
                continue
            if s.startswith('Cidades/Localidades Destino:'):
                mode = 'dest'
                buf_d.append(s.split(':', 1)[-1].strip())
                continue
            if s == 'Regras':
                mode = 'regras'
                continue
            if s == 'Taxas':
                mode = 'taxas'
                continue
            if mode == 'orig':
                buf_o.append(s)
            elif mode == 'dest':
                buf_d.append(s)
            elif mode == 'regras':
                ma = RE_ATE.search(s)
                mm = RE_ACIMA_MULT.search(s)
                if ma:
                    regras.append({'tipo': 'ATE_KG', 'kg': to_num(ma.group(1)), 'valor': to_num(ma.group(2))})
                elif mm:
                    regras.append({'tipo': 'ACIMA_KG_MULT', 'kg': to_num(mm.group(1)), 'multiplicador': to_num(mm.group(2))})
                # "Acima de X Kg: R$ Y" (valor fixo) -> nao emite regra (fiel ao parser original)
            elif mode == 'taxas':
                if s.startswith('Nenhuma taxa'):
                    continue
                taxa_seg.append(ln)
        origens = parse_lista_cidades(buf_o)
        destinos = parse_lista_cidades(buf_d)
        # taxas por rota
        for ln in taxa_seg:
            ts = is_tax_start(ln)
            if ts is not None:
                nome, sp = ts
                t = {'nome': nome}
                t.update(sp)
                taxas.append(t)
        rota = {'id': rid, 'descricao': desc, 'origens': origens, 'destinos': destinos}
        if regras:
            rota['regras'] = regras
        if taxas:
            rota['taxas'] = taxas
        rotas.append(rota)
    return rotas

# ---------- header ----------
def get_field(seg, prefix):
    for l in seg:
        s = l.strip()
        if s.startswith(prefix):
            return s.split(':', 1)[-1].strip()
    return ''

tabelas = []
for name, start, end in blocks:
    seg = lines[start:end + 1]
    # acha indice de 'Taxas' e da 1a 'Rota'
    idx_taxas = next((i for i, l in enumerate(seg) if l.strip() == 'Taxas'), None)
    idx_rota1 = next((i for i, l in enumerate(seg) if RE_ROTA.match(l.strip())), len(seg))
    header_seg = seg[:idx_taxas if idx_taxas is not None else idx_rota1]
    taxas_seg = seg[(idx_taxas + 1):idx_rota1] if idx_taxas is not None else []
    rotas_seg = seg[idx_rota1:]

    tab = {
        'nome': name,
        'transportador': get_field(header_seg, 'Transportador:'),
        'tipo_calculo': get_field(header_seg, 'Tipo de Cálculo:') or get_field(header_seg, 'Tipo de Calculo:'),
        'fator_m3': to_num(get_field(header_seg, 'Fator Conversão M3:') or get_field(header_seg, 'Fator Conversao M3:') or '0'),
        'tipo_peso': get_field(header_seg, 'Tipo Peso Utilizar:'),
        'tipo_frete': get_field(header_seg, 'Tipo de Frete:'),
        'tipo_veiculo': get_field(header_seg, 'Tipo de Veículo:') or get_field(header_seg, 'Tipo de Veiculo:'),
        'status': get_field(header_seg, 'Status da Tabela:'),
        'xobs_cte': get_field(header_seg, 'xObs (CT-e):'),
        'taxas_gerais': parse_taxas_gerais(taxas_seg),
        'rotas': parse_rotas(rotas_seg),
    }
    tabelas.append(tab)

out = {'tabelas': tabelas}
json.dump(out, open('tabelas_novo.json', 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
print('OK ->', len(tabelas), 'tabelas')
