#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AUDITORIA FISCAL - Publicar e atualizar (tudo em um)
====================================================
O que este programa faz quando voce roda:
  1) Le as 3 pastas (NF-e, CT-e, manifesto) - apenas 2026+
  2) Grava tudo no Supabase (tabelas) e gera o dados.json
  3) Envia o dados.json para o Supabase Storage: dashboards/auditoria/dados.json
  4) Publica a pasta auditoria/ no GitHub (git push)
  5) Abre o painel no navegador (servidor local) com o botao ATUALIZAR,
     que refaz os passos 1-3 (reprocessa as pastas) quando clicado.

Rode no PC/servidor que enxerga o drive Y:.
"""

import os, sys, glob, json, io, webbrowser, subprocess, threading
import xml.etree.ElementTree as ET
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

# ============================================================
#  CONFIGURACAO  -  edite no Bloco de Notas
# ============================================================
SUPABASE_URL  = "https://ennsbpibfnuwlvtodukg.supabase.co"
SUPABASE_ANON = "sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONyI5"
LOGIN_EMAIL   = "thiago_balao@yahoo.com.br"     # mesmo login do painel
LOGIN_SENHA   = "Genomma@2026"

PASTA_NFE       = r"Y:\ERP-12\ArqXML-MG"
PASTA_CTE       = r"Y:\ERP-12\TOTVSCOLAB20-PRD\RECEIVED"
PASTA_ROMANEIO  = r"Y:\ERP-12\TRANSP-PRD\ROMANEIO\Enviados"
PASTA_MANIFESTO = r"Y:\ERP-12\ArqXML-MG"

ANO_MINIMO   = 2026
PORTA        = 8000
PUBLICAR_GIT = True      # False = nao tenta git push
PASTA_SAIDA  = r"..\auditoria"   # grava o painel na pasta auditoria da RAIZ do repositorio
REPO_DIR     = r".."             # raiz do repositorio (onde ficam otd/, scorecard/, etc.)
BUCKET       = "dashboards"
CAMINHO_JSON = "auditoria/dados.json"   # dentro do bucket
# ============================================================

try:
    from supabase import create_client
except ImportError:
    print("Falta a biblioteca. Rode:  pip install supabase"); sys.exit(1)

# ---------- helpers XML ----------
def g(el, tag):
    if el is None: return None
    e = el.find(f".//{{*}}{tag}"); return e.text if e is not None else None
def gd(el, tag):
    if el is None: return None
    for ch in el:
        if ch.tag.split("}")[-1] == tag: return ch
    return None
def num(v):
    try: return float(v)
    except (TypeError, ValueError): return None
def ano_doc(dh, caminho):
    if dh and len(dh) >= 4 and dh[:4].isdigit(): return int(dh[:4])
    return datetime.fromtimestamp(os.path.getmtime(caminho)).year

def parse_nfe(path):
    inf = ET.parse(path).getroot().find(".//{*}infNFe")
    if inf is None: return None, None
    ide,emit,dest = gd(inf,"ide"),gd(inf,"emit"),gd(inf,"dest")
    ee,ed = gd(emit,"enderEmit"),gd(dest,"enderDest")
    tot = inf.find(".//{*}ICMSTot"); prot = ET.parse(path).getroot().find(".//{*}infProt"); adic = gd(inf,"infAdic")
    nota = {"chave":(inf.get("Id") or "").replace("NFe",""),"numero":g(ide,"nNF"),"serie":g(ide,"serie"),
        "modelo":g(ide,"mod"),"natureza_operacao":g(ide,"natOp"),
        "tipo_operacao":"Saida" if g(ide,"tpNF")=="1" else "Entrada","finalidade":g(ide,"finNFe"),
        "data_emissao":g(ide,"dhEmi"),"uf_emitente":g(ee,"UF"),"cnpj_emitente":g(emit,"CNPJ"),
        "nome_emitente":g(emit,"xNome"),"ie_emitente":g(emit,"IE"),"municipio_emitente":g(ee,"xMun"),
        "cnpj_destinatario":g(dest,"CNPJ"),"nome_destinatario":g(dest,"xNome"),"uf_destinatario":g(ed,"UF"),
        "municipio_destinatario":g(ed,"xMun"),"valor_produtos":num(g(tot,"vProd")),"valor_total":num(g(tot,"vNF")),
        "valor_icms":num(g(tot,"vICMS")),"valor_frete":num(g(tot,"vFrete")),"valor_desconto":num(g(tot,"vDesc")),
        "protocolo":g(prot,"nProt"),"status_codigo":g(prot,"cStat"),"status_motivo":g(prot,"xMotivo"),
        "data_autorizacao":g(prot,"dhRecbto"),"info_complementar":(g(adic,"infCpl") or "")[:200],
        "arquivo_origem":os.path.basename(path)}
    itens=[]
    for det in inf.findall("{*}det"):
        prod=gd(det,"prod"); iw=gd(det,"ICMS"); icms=list(iw)[0] if iw is not None and len(iw) else None
        itens.append({"num_item":int(det.get("nItem")),"codigo_produto":g(prod,"cProd"),"ean":g(prod,"cEAN"),
            "descricao":g(prod,"xProd"),"ncm":g(prod,"NCM"),"cfop":g(prod,"CFOP"),"unidade":g(prod,"uCom"),
            "quantidade":num(g(prod,"qCom")),"valor_unitario":num(g(prod,"vUnCom")),"valor_total":num(g(prod,"vProd")),
            "cst_icms":g(icms,"CST"),"aliquota_icms":num(g(icms,"pICMS")),"valor_icms_item":num(g(icms,"vICMS"))})
    return nota, itens

def parse_cte(path):
    root=ET.parse(path).getroot(); inf=root.find(".//{*}infCte")
    if inf is None: return None, None
    ide=gd(inf,"ide"); emit=gd(inf,"emit"); rem=gd(inf,"rem"); dest=gd(inf,"dest")
    vp=gd(inf,"vPrest"); carga=root.find(".//{*}infCarga"); prot=root.find(".//{*}infProt")
    refs=[]
    for nf in inf.findall(".//{*}infNFe"):
        ch=g(nf,"chave")
        if ch: refs.append({"chave_nfe":ch,"numero_nf":str(int(ch[25:34])) if len(ch)==44 else None})
    cte={"chave":(inf.get("Id") or "").replace("CTe",""),"numero":g(ide,"nCT"),"serie":g(ide,"serie"),
        "modelo":g(ide,"mod"),"tipo_cte":g(ide,"tpCTe"),"natureza_operacao":g(ide,"natOp"),"cfop":g(ide,"CFOP"),
        "data_emissao":g(ide,"dhEmi"),"uf_inicio":g(ide,"UFIni"),"uf_fim":g(ide,"UFFim"),
        "cnpj_emitente":g(emit,"CNPJ"),"nome_emitente":g(emit,"xNome"),
        "cnpj_remetente":g(rem,"CNPJ") or g(rem,"CPF"),"nome_remetente":g(rem,"xNome"),
        "cnpj_destinatario":g(dest,"CNPJ") or g(dest,"CPF"),"nome_destinatario":g(dest,"xNome"),
        "valor_total":num(g(vp,"vTPrest")),"valor_receber":num(g(vp,"vRec")),"valor_carga":num(g(carga,"vCarga")),
        "qtd_nfe":len(refs),"protocolo":g(prot,"nProt"),"status_codigo":g(prot,"cStat"),
        "status_motivo":g(prot,"xMotivo"),"data_autorizacao":g(prot,"dhRecbto"),"arquivo_origem":os.path.basename(path)}
    return cte, refs

def parse_manifesto(path):
    with open(path,encoding="latin-1") as f: linhas=[l.rstrip("\r\n") for l in f if l.strip()]
    nome=os.path.basename(path); regs=[]
    if linhas and len(linhas[0])==60 and linhas[0][0]=="0":   # formato posicional
        h=linhas[0]; d=h[13:21]
        data=f"{d[4:8]}-{d[2:4]}-{d[0:2]}" if len(d)==8 else None
        cnpj=h[25:39]; serie=nome.replace(".txt","").split("_")[-1]
        for l in linhas[1:]:
            if len(l)<60 or not l.startswith("001"): continue
            regs.append({"sequencia":l[3:12],"numero_nf":str(int(l[3:12])),"campo_aux":l[12:16],
                "chave_nfe":l[16:60],"cnpj_empresa":cnpj,"serie":serie,"data_arquivo":data,
                "arquivo_origem":nome,"codigo_evento":"210200","evento":"Confirmacao da Operacao (saida)"})
    return regs

# ---------- Supabase ----------
def conectar():
    sb=create_client(SUPABASE_URL, SUPABASE_ANON)
    sb.auth.sign_in_with_password({"email":LOGIN_EMAIL,"password":LOGIN_SENHA})
    return sb

def chunk(lst, n):
    for i in range(0, len(lst), n): yield lst[i:i+n]

def coletar(pasta, exts=None, prefixo=None, recursivo=True):
    """Varre a pasta com os.scandir (rapido), mostrando progresso, e ja descarta
       pela DATA DO ARQUIVO o que for anterior a ANO_MINIMO - sem abrir o arquivo."""
    print(f"  varrendo {pasta}  (apenas {ANO_MINIMO}+ pela data do arquivo) ...", flush=True)
    res, vistos, pilha = [], 0, [pasta]
    while pilha:
        d = pilha.pop()
        try:
            it = os.scandir(d)
        except Exception as ex:
            print("    aviso ao abrir", d, "->", ex); continue
        with it:
            for e in it:
                try:
                    if e.is_dir(follow_symlinks=False):
                        if recursivo: pilha.append(e.path)
                        continue
                except OSError:
                    continue
                vistos += 1
                if vistos % 3000 == 0:
                    print(f"    {vistos} arquivos vistos, {len(res)} de {ANO_MINIMO}+ ...", flush=True)
                nm = e.name
                if exts and not nm.lower().endswith(exts): continue
                if prefixo and not nm.upper().startswith(prefixo): continue
                try:
                    if datetime.fromtimestamp(e.stat().st_mtime).year < ANO_MINIMO: continue
                except OSError:
                    continue
                res.append(e.path)
    print(f"    -> {len(res)} arquivo(s) de {ANO_MINIMO}+ (de {vistos} no total).", flush=True)
    return res

def chaves_existentes(sb, tabela, coluna="chave"):
    """Le todas as chaves ja gravadas (paginando), para pular o que ja existe."""
    achadas, passo, ini = set(), 1000, 0
    while True:
        r = sb.table(tabela).select(coluna).range(ini, ini+passo-1).execute()
        if not r.data: break
        for row in r.data:
            if row.get(coluna): achadas.add(row[coluna])
        if len(r.data) < passo: break
        ini += passo
    return achadas

def reprocessar(sb):
    print("\n== Reprocessando pastas (somente %d+) ==" % ANO_MINIMO, flush=True)

    # ---------- NF-e ----------
    arquivos = coletar(PASTA_NFE, exts=(".xml",))
    print("  conferindo o que ja esta no banco ...", flush=True)
    ja = chaves_existentes(sb, "nfe_notas")
    novas = []
    for i, xml in enumerate(arquivos, 1):
        if i % 500 == 0: print(f"    lidos {i}/{len(arquivos)} ...", flush=True)
        try:
            nota, itens = parse_nfe(xml)
            if not nota: continue
            if nota["chave"] in ja: continue
            ja.add(nota["chave"]); novas.append((nota, itens))
        except Exception as e:
            print("    NF erro", os.path.basename(xml), e)
    print(f"  NF-e novas para enviar: {len(novas)}", flush=True)
    idmap = {}
    for lote in chunk([n for n, _ in novas], 400):
        res = sb.table("nfe_notas").upsert(lote, on_conflict="chave").execute()
        for row in res.data: idmap[row["chave"]] = row["id"]
        print(f"    notas enviadas: {len(idmap)}", flush=True)
    itens_all = []
    for nota, itens in novas:
        nid = idmap.get(nota["chave"])
        if nid:
            for it in itens: itens_all.append({**it, "nota_id": nid})
    for lote in chunk(list({idmap[k] for k in idmap}), 200):
        sb.table("nfe_itens").delete().in_("nota_id", lote).execute()
    for lote in chunk(itens_all, 1000):
        sb.table("nfe_itens").insert(lote).execute()
    nf = len(novas)

    # ---------- CT-e ----------
    arquivos = coletar(PASTA_CTE, exts=(".xml",))
    jac = chaves_existentes(sb, "cte_conhecimentos")
    novosc = []
    for i, xml in enumerate(arquivos, 1):
        if i % 500 == 0: print(f"    lidos {i}/{len(arquivos)} ...", flush=True)
        try:
            cte, refs = parse_cte(xml)
            if not cte: continue
            if cte["chave"] in jac: continue
            jac.add(cte["chave"]); novosc.append((cte, refs))
        except Exception as e:
            print("    CT erro", os.path.basename(xml), e)
    print(f"  CT-e novos para enviar: {len(novosc)}", flush=True)
    cmap = {}
    for lote in chunk([c for c, _ in novosc], 400):
        res = sb.table("cte_conhecimentos").upsert(lote, on_conflict="chave").execute()
        for row in res.data: cmap[row["chave"]] = row["id"]
    refs_all = []
    for cte, refs in novosc:
        cid = cmap.get(cte["chave"])
        if cid:
            for r in refs: refs_all.append({**r, "cte_id": cid})
    for lote in chunk(refs_all, 1000):
        sb.table("cte_nfe_ref").upsert(lote, on_conflict="cte_id,chave_nfe").execute()
    ct = len(novosc)

    # ---------- Manifestos (confirmacao de saida) ----------
    arquivos = coletar(PASTA_MANIFESTO, exts=(".txt",), prefixo="MANIFE")
    regs = []
    for txt in arquivos:
        try:
            regs += parse_manifesto(txt)
        except Exception as e:
            print("    MANIFE erro", os.path.basename(txt), e)
    mf = 0
    for lote in chunk(regs, 500):
        try:
            sb.table("nfe_manifestacoes").upsert(lote, on_conflict="chave_nfe,arquivo_origem").execute()
            mf += len(lote)
        except Exception as e:
            print("    MANIFE lote erro:", e)
    print(f"\n  RESUMO -> NF-e novas {nf} | CT-e novos {ct} | confirmacoes {mf}", flush=True)

    print("  montando dados do painel ...", flush=True)
    dados = montar_dados(sb)
    salvar_e_subir(sb, dados)
    return {"nf": nf, "ct": ct, "mf": mf, "notas": len(dados["notas"])}

def buscar_tudo(sb, tabela, colunas="*", ordem=None, desc=True):
    linhas, passo, ini = [], 1000, 0
    while True:
        q = sb.table(tabela).select(colunas)
        if ordem: q = q.order(ordem, desc=desc)
        r = q.range(ini, ini+passo-1).execute()
        if not r.data: break
        linhas += r.data
        if len(r.data) < passo: break
        ini += passo
    return linhas

def montar_dados(sb):
    notas = buscar_tudo(sb, "nfe_notas",
        "id,chave,numero,serie,natureza_operacao,data_emissao,nome_emitente,uf_emitente,nome_destinatario,uf_destinatario,valor_total",
        ordem="data_emissao")
    ctes = buscar_tudo(sb, "cte_conhecimentos", "id,chave,numero,serie,nome_emitente,uf_inicio,uf_fim,valor_total,data_emissao")
    refs = buscar_tudo(sb, "cte_nfe_ref", "cte_id,chave_nfe,numero_nf")
    manifs = buscar_tudo(sb, "nfe_manifestacoes", "numero_nf,chave_nfe,cnpj_empresa,data_arquivo")
    cte_chave = {c["id"]: c["chave"] for c in ctes}
    refs2 = [{"chave_cte": cte_chave.get(r["cte_id"]), "chave_nfe": r["chave_nfe"], "numero_nf": r.get("numero_nf")} for r in refs]
    return {"notas": notas, "ctes": ctes, "refs": refs2, "manifs": manifs,
            "gerado_em": datetime.now().isoformat(timespec="seconds")}

def salvar_e_subir(sb,dados):
    os.makedirs(PASTA_SAIDA,exist_ok=True)
    raw=json.dumps(dados,ensure_ascii=False,default=str).encode("utf-8")
    with open(os.path.join(PASTA_SAIDA,"dados.json"),"wb") as f: f.write(raw)
    try:
        sb.storage.from_(BUCKET).upload(CAMINHO_JSON, raw,
            {"content-type":"application/json","cache-control":"0","upsert":"true"})
        print("  dados.json enviado para o Storage (dashboards/auditoria).")
    except Exception:
        try:
            sb.storage.from_(BUCKET).update(CAMINHO_JSON, raw,
                {"content-type":"application/json","cache-control":"0","upsert":"true"})
            print("  dados.json atualizado no Storage.")
        except Exception as e:
            print("  Aviso: nao consegui enviar ao Storage:",e)

def publicar_github():
    if not PUBLICAR_GIT: return
    print("\n== Publicando no GitHub ==")
    try:
        subprocess.run(["git","add","-A"], cwd=REPO_DIR, check=True)
        r=subprocess.run(["git","commit","-m","auditoria: atualiza painel e dados"], cwd=REPO_DIR, capture_output=True,text=True)
        if "nothing to commit" in (r.stdout+r.stderr): print("  Nada novo para commitar.")
        subprocess.run(["git","push"], cwd=REPO_DIR, check=True)
        print("  Enviado para o GitHub.")
    except Exception as e:
        print("  Aviso no git:", e)
        print("  (Rode na raiz do repositorio uma vez: git pull origin main --no-rebase / git push)")

# ---------- Servidor local ----------
SB_GLOBAL=None
class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*a,**k): super().__init__(*a,directory=PASTA_SAIDA,**k)
    def log_message(self,*a): pass
    def do_GET(self):
        if self.path.startswith("/atualizar"):
            try:
                reprocessar(SB_GLOBAL); publicar_github()
                self.send_response(200); self.send_header("Content-Type","application/json")
                self.end_headers(); self.wfile.write(b'{"ok":true}')
            except Exception as e:
                self.send_response(500); self.end_headers(); self.wfile.write(str(e).encode())
            return
        return super().do_GET()

def servir():
    global SB_GLOBAL
    srv=ThreadingHTTPServer(("127.0.0.1",PORTA),Handler)
    url=f"http://127.0.0.1:{PORTA}/index.html"
    print(f"\n== Painel no ar: {url} ==\n   (CTRL+C para encerrar)")
    threading.Timer(1.0,lambda:webbrowser.open(url)).start()
    try: srv.serve_forever()
    except KeyboardInterrupt: print("\nEncerrado.")

# ---------- Main ----------
if __name__ == "__main__":
    if "SEU_EMAIL" in LOGIN_EMAIL:
        print("Configure LOGIN_EMAIL e LOGIN_SENHA no topo do arquivo."); sys.exit(1)
    so_painel = len(sys.argv) > 1 and sys.argv[1].lower() in ("painel", "--painel", "-p")
    SB_GLOBAL = conectar()
    if so_painel:
        print("\n== Modo painel: sem varrer pastas, so atualiza a tela com o que ja esta no banco ==")
        salvar_e_subir(SB_GLOBAL, montar_dados(SB_GLOBAL))
    else:
        reprocessar(SB_GLOBAL)
    publicar_github()
    servir()
