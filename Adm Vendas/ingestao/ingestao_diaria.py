# -*- coding: utf-8 -*-
"""
Ingestão diária — Torre de Controle · ADM de Vendas (Genomma)  [v2 - parser corrigido]
Layout do ESPD022.LST (RPW): colunas em posição fixa, detectadas pela linha
separadora "---- ---- ...". Ordem: Cliente | Ped Cli | Dt Implant | Entrega |
Dias Atraso | Item | Qt Pedida | Qt Alocada Embarque | Qt Atende | Qtd.Saldo |
Vl Tot Item | Vl Tot Abe | Sit.

Validado contra o painel de referência de 28/07/2026 (50.398 linhas):
todos os totais idênticos (Vl, Vl Aberto, Qt Pedida/Atendida/Saldo/Alocada e situações).
"""
import os, re, glob, json, datetime, requests

# ================= CONFIG =================
PASTA_LST = os.environ.get("PASTA_LST", r"Y:\ERP-12\rpw")
REPO_DIR  = os.environ.get("REPO_DIR", os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ennsbpibfnuwlvtodukg.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
HD = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
      "Accept-Profile": "adm_vendas", "Content-Profile": "adm_vendas",
      "Content-Type": "application/json"}
REST = SUPABASE_URL.rstrip("/") + "/rest/v1"

SITS = ["Aberto", "Atendido Parcial", "Atendido Total", "Cancelado", "Suspenso"]

# posições padrão (layout 28/07/2026) — usadas se a linha separadora não for encontrada
COLS_PADRAO = [(0,12),(13,25),(26,36),(37,47),(48,59),(60,76),(77,92),(93,112),
               (113,128),(129,145),(146,161),(162,177),(178,9999)]

def num(s):
    s = s.strip()
    return float(s.replace(".", "").replace(",", ".")) if s else 0.0

def detecta_colunas(linhas):
    """Lê a linha separadora '---- ---- ...' e devolve os ranges de cada coluna."""
    for l in linhas[:200]:
        if re.match(r"^-+ -+ -+ -+ ", l):
            cols = [(m.start(), m.end()) for m in re.finditer(r"-+", l)]
            if len(cols) >= 13:
                cols[-1] = (cols[-1][0], 9999)  # Sit. vai até o fim da linha
                return cols
    return COLS_PADRAO

def parse_lst(path):
    with open(path, encoding="iso-8859-1") as f:
        linhas = f.read().split("\n")
    C = detecta_colunas(linhas)
    ix_cli, ix_ped, ix_d1, ix_d2 = C[0], C[1], C[2], C[3]
    ix_item, ix_qp, ix_qaloc, ix_qa = C[5], C[6], C[7], C[8]
    ix_saldo, ix_vl, ix_vlabe, ix_sit = C[9], C[10], C[11], C[12]

    out, cliente = [], ""
    for l in linhas:
        d1 = l[ix_d1[0]:ix_d1[1]].strip() if len(l) > ix_d1[1] else ""
        if not re.match(r"^\d\d/\d\d/\d{4}$", d1):
            continue  # cabeçalhos, separadores, quebras de página
        c = l[ix_cli[0]:ix_cli[1]].strip()
        if c:
            cliente = c
        sit = l[ix_sit[0]:].strip()
        if sit == "Atendido Parcia":
            sit = "Atendido Parcial"
        if sit not in SITS:
            continue
        d2 = l[ix_d2[0]:ix_d2[1]].strip()
        d_imp = datetime.datetime.strptime(d1, "%d/%m/%Y").date()
        d_ent = datetime.datetime.strptime(d2, "%d/%m/%Y").date()
        out.append(dict(
            cliente=cliente,
            ped_cli=l[ix_ped[0]:ix_ped[1]].strip(),
            cod_item=l[ix_item[0]:ix_item[1]].strip(),
            situacao=sit,
            dt_implantacao=str(d_imp), dt_entrega=str(d_ent),
            qt_pedida=num(l[ix_qp[0]:ix_qp[1]]),
            qt_alocada_embarque=num(l[ix_qaloc[0]:ix_qaloc[1]]),
            qt_atendida=num(l[ix_qa[0]:ix_qa[1]]),
            qt_saldo=max(num(l[ix_saldo[0]:ix_saldo[1]]), 0.0),
            vl_tot_item=num(l[ix_vl[0]:ix_vl[1]]),
            vl_aberto=num(l[ix_vlabe[0]:ix_vlabe[1]])))
    return out

# ================= SUPABASE =================
def get_all(tabela, select):
    out, offset = [], 0
    while True:
        r = requests.get(f"{REST}/{tabela}?select={select}&limit=1000&offset={offset}", headers=HD)
        r.raise_for_status()
        chunk = r.json()
        out += chunk
        if len(chunk) < 1000:
            return out
        offset += 1000

def carrega_fato(linhas, dt_carga):
    requests.delete(f"{REST}/fato_carteira?dt_carga=eq.{dt_carga}", headers=HD).raise_for_status()
    for i in range(0, len(linhas), 1000):
        lote = [dict(l, dt_carga=str(dt_carga)) for l in linhas[i:i+1000]]
        r = requests.post(f"{REST}/fato_carteira", headers=HD, data=json.dumps(lote))
        r.raise_for_status()

def registra_pendencias(tipo, codigos):
    if not codigos:
        return
    hd = dict(HD); hd["Prefer"] = "resolution=ignore-duplicates"
    body = [{"tipo": tipo, "codigo": c} for c in sorted(codigos)]
    requests.post(f"{REST}/pendencias_cadastro?on_conflict=tipo,codigo", headers=hd, data=json.dumps(body)).raise_for_status()

# ================= PAINEL =================
def gera_painel(linhas, itens, clientes, miss_i, miss_c, dt_carga):
    ITEMS = {i["cod_item"]: [i["descricao"], i.get("marca") or ""] for i in itens}
    CLIUF = {c["abreviado"]: c.get("uf") or "" for c in clientes}
    CLIMAT = {c["abreviado"]: c.get("matriz") or "" for c in clientes}
    SIT_IDX = {s: k for k, s in enumerate(SITS)}
    rows = []
    for l in linhas:
        qp, qa, qaloc, saldo = l["qt_pedida"], l["qt_atendida"], l["qt_alocada_embarque"], l["qt_saldo"]
        sit = l["situacao"]
        aloc = 2 if (qaloc >= qp and qp > 0) else (1 if qaloc > 0 else 0)
        tem_emb = 1 if (qaloc > 0 or sit in ("Atendido Parcial", "Atendido Total")) else 0
        nf = 1 if sit == "Atendido Total" else 0
        rows.append([CLIMAT.get(l["cliente"], ""), l["cliente"], l["ped_cli"], l["cod_item"],
                     SIT_IDX[sit], aloc, tem_emb, "", "", 0, qp, qa, saldo,
                     l["vl_tot_item"], l["vl_aberto"], l["dt_implantacao"][:7], nf,
                     CLIUF.get(l["cliente"], ""), l["dt_implantacao"], qaloc])
    tpl = open(os.path.join(REPO_DIR, "adm-vendas", "torre_template.html"), encoding="utf-8").read()
    sub = f"ESPD022 de {dt_carga.strftime('%d/%m/%Y')} · {len(rows):,} linhas · fonte: LST Datasul (RPW)".replace(",", ".")
    def banner():
        if not miss_i and not miss_c:
            return ""
        p = []
        if miss_i: p.append(f"<b>{len(miss_i)} itens sem cadastro</b> (inclua na dim_item / Itens_.xlsx): " + " ".join(f"<code>{x}</code>" for x in sorted(miss_i)))
        if miss_c: p.append(f"<b>{len(miss_c)} clientes sem cadastro</b> (inclua na dim_cliente / Clientes.xls): " + " ".join(f"<code>{x}</code>" for x in sorted(miss_c)))
        return '<div class="pendbar">\u26a0 <b>Cadastros pendentes</b> \u2014 ' + " &nbsp;\u00b7&nbsp; ".join(p) + '. Ap\u00f3s cadastrar, rode <code>carga_inicial.py</code> e depois <code>ingestao_diaria.py</code>.</div>'
    html = (tpl.replace("__SUBLINE__", sub)
              .replace("__ITEMS__", json.dumps(ITEMS, ensure_ascii=False))
              .replace("__CLIUF__", json.dumps(CLIUF, ensure_ascii=False))
              .replace("__CLIMAT__", json.dumps(CLIMAT, ensure_ascii=False))
              .replace("__PENDBANNER__", banner())
              .replace("__DATA__", json.dumps(rows, ensure_ascii=False)))
    out = os.path.join(REPO_DIR, "adm-vendas", "index.html")
    open(out, "w", encoding="utf-8").write(html)
    return out

# ================= MAIN =================
def data_do_relatorio(path):
    """Lê a data impressa no cabeçalho do relatório (ex: '28/07/2026 - 06:00:04')."""
    try:
        with open(path, encoding="iso-8859-1") as f:
            topo = f.read(4000)
        m = re.search(r"(\d\d/\d\d/\d{4}) - \d\d:\d\d", topo)
        if m:
            return datetime.datetime.strptime(m.group(1), "%d/%m/%Y").date()
    except Exception:
        pass
    return None

def main():
    assert KEY, "Defina a variável de ambiente SUPABASE_SERVICE_KEY"
    cand = set(glob.glob(os.path.join(PASTA_LST, "ESPD022*.LST")) +
               glob.glob(os.path.join(PASTA_LST, "ESPD022*.lst")))
    assert cand, f"Nenhum ESPD022*.LST em {PASTA_LST}"
    # escolhe pelo par (data do cabeçalho do relatório, data de modificação) — sempre o mais novo
    def chave(p):
        d = data_do_relatorio(p)
        return (d or datetime.date.min, os.path.getmtime(p))
    arq = max(cand, key=chave)
    dt_carga = data_do_relatorio(arq) or datetime.date.fromtimestamp(os.path.getmtime(arq))
    hoje = datetime.date.today()
    print(f"Lendo {arq} (relatório de {dt_carga}) ...")
    if dt_carga != hoje:
        print(f"[AVISO] O relatório mais novo encontrado é de {dt_carga:%d/%m/%Y}, não de hoje ({hoje:%d/%m/%Y}).")
        print("        Confira se o RPW já gerou o ESPD022 do dia antes de publicar.")
    linhas = parse_lst(arq)
    print(f"{len(linhas)} linhas parseadas")
    if len(linhas) < 1000:
        print("[AVISO] Poucas linhas parseadas — confira se o layout do relatório mudou.")

    itens = get_all("dim_item", "cod_item,descricao,marca")
    clientes = get_all("dim_cliente", "abreviado,uf,matriz")
    miss_i = {l["cod_item"] for l in linhas} - {i["cod_item"] for i in itens}
    miss_c = {l["cliente"] for l in linhas} - {c["abreviado"] for c in clientes}

    carrega_fato(linhas, dt_carga)
    print(f"fato_carteira: {len(linhas)} linhas gravadas para {dt_carga}")
    registra_pendencias("item", miss_i)
    registra_pendencias("cliente", miss_c)
    if miss_i or miss_c:
        print(f"PENDÊNCIAS: {len(miss_i)} itens {sorted(miss_i)} | {len(miss_c)} clientes {sorted(miss_c)}")

    out = gera_painel(linhas, itens, clientes, miss_i, miss_c, dt_carga)
    print(f"Painel regenerado: {out}")

if __name__ == "__main__":
    main()
