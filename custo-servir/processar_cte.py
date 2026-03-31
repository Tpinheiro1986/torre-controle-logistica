"""
TORRE DE CONTROLE — PROCESSADOR DE CTe XML
==========================================
Lê XMLs de CTe de uma pasta (recursivo), processa somente
CTes AUTORIZADOS (cStat=100), mantém histórico incremental,
agrega indicadores e publica o JSON no Supabase Storage.

USO:
  python processar_cte.py                    → incremental (só novos)
  python processar_cte.py --reprocessar      → reprocessa tudo do zero
  python processar_cte.py --pasta C:\ctes    → pasta alternativa

AGENDAR NO WINDOWS (diário às 06:00):
  schtasks /create /tn "Torre-CTE" /tr "C:\caminho\ATUALIZAR.bat"
           /sc daily /st 06:00 /ru SYSTEM
"""

import os, sys, glob, json, argparse, logging
import xml.etree.ElementTree as ET
from datetime import datetime
import requests

# ═══════════════════════════════════════════════════════════════
#  CONFIGURAÇÃO — edite apenas esta seção
# ═══════════════════════════════════════════════════════════════
PASTA_CTE      = r"Y:\ERP-12\TOTVSCOLAB20-PRD\RECEIVED"          # ← EDITE: pasta onde ficam os XMLs
ANOS_FILTRO    = [2025, 2026]                # anos a processar
HISTORICO_FILE = "cte_historico.json"        # cache local — NÃO APAGUE
LOG_FILE       = "cte_processador.log"

SUPABASE_URL   = "https://ennsbpibfnuwlvtodukg.supabase.co"
SUPABASE_ANON  = "sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONwI5"
BUCKET         = "dashboards"
PATH_JSON      = "custo-servir/dados.json"

CNPJ_GENOMMA   = "09080907000506"   # Genomma Laboratories do Brasil
CNPJ_INOVALAB  = "60698771000126"   # ← confirme o CNPJ da Inovalab

# ═══════════════════════════════════════════════════════════════
NS = {"cte": "http://www.portalfiscal.inf.br/cte"}

REGIOES = {
    "AC":"Norte","AM":"Norte","AP":"Norte","PA":"Norte",
    "RO":"Norte","RR":"Norte","TO":"Norte",
    "AL":"Nordeste","BA":"Nordeste","CE":"Nordeste","MA":"Nordeste",
    "PB":"Nordeste","PE":"Nordeste","PI":"Nordeste","RN":"Nordeste","SE":"Nordeste",
    "DF":"Centro-Oeste","GO":"Centro-Oeste","MS":"Centro-Oeste","MT":"Centro-Oeste",
    "ES":"Sudeste","MG":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","RS":"Sul","SC":"Sul",
}
MES_NOME = ["","Jan","Fev","Mar","Abr","Mai","Jun",
            "Jul","Ago","Set","Out","Nov","Dez"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ───────────────────────────────────────────────────────────────
#  PARSE XML
# ───────────────────────────────────────────────────────────────
def _txt(root, tag):
    el = root.find(".//" + tag, NS)
    return el.text.strip() if el is not None and el.text else None


def parse_cte(filepath: str):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except ET.ParseError:
        return None

    if _txt(root, "cte:cStat") != "100":   # somente autorizados
        return None

    chave = _txt(root, "cte:chCTe")
    if not chave:
        return None

    dhemi = _txt(root, "cte:dhEmi") or ""
    try:
        dt = datetime.fromisoformat(dhemi[:19])
    except ValueError:
        return None

    if dt.year not in ANOS_FILTRO:
        return None

    v_frete = float(_txt(root, "cte:vTPrest") or 0)
    v_merc  = float(_txt(root, "cte:vCarga")  or 0)

    emit = root.find(".//cte:emit", NS)
    transp_nome = transp_cnpj = ""
    if emit is not None:
        transp_nome = (emit.findtext("cte:xNome", namespaces=NS) or
                       emit.findtext("cte:xFant", namespaces=NS) or "").strip()[:60]
        transp_cnpj = (emit.findtext("cte:CNPJ", namespaces=NS) or "").strip()

    rem = root.find(".//cte:rem", NS)
    rem_cnpj = rem_nome = ""
    if rem is not None:
        rem_cnpj = (rem.findtext("cte:CNPJ",  namespaces=NS) or "").strip()
        rem_nome = (rem.findtext("cte:xNome", namespaces=NS) or "").strip()

    if   rem_cnpj == CNPJ_GENOMMA:           empresa = "GENOMMA"
    elif rem_cnpj == CNPJ_INOVALAB:          empresa = "INOVALAB"
    elif "GENOMMA"  in rem_nome.upper():     empresa = "GENOMMA"
    elif "INOVALAB" in rem_nome.upper():     empresa = "INOVALAB"
    else:                                    empresa = "OUTROS"

    dest = root.find(".//cte:dest", NS)
    cli_nome = cli_cnpj = ""
    if dest is not None:
        cli_nome = (dest.findtext("cte:xNome", namespaces=NS) or "").strip()[:60]
        cli_cnpj = (dest.findtext("cte:CNPJ",  namespaces=NS) or "").strip()

    uf_dest  = (_txt(root, "cte:UFFim")   or "").strip()
    mun_dest = (_txt(root, "cte:xMunFim") or "").strip()
    regiao   = REGIOES.get(uf_dest, "Outros")

    tipo_op = "CIF"
    for obs in root.findall(".//cte:ObsCont", NS):
        if obs.get("xCampo") == "obsAuxiliar":
            if "FOB" in (obs.findtext("cte:xTexto", namespaces=NS) or "").upper():
                tipo_op = "FOB"
            break

    peso = 0.0
    for q in root.findall(".//cte:infQ", NS):
        if "BASE" in (q.findtext("cte:tpMed", namespaces=NS) or "").upper():
            try: peso = float(q.findtext("cte:qCarga", namespaces=NS) or 0)
            except: pass

    return {
        "chave": chave, "nct": _txt(root, "cte:nCT") or "",
        "ano": dt.year, "mes": dt.month, "mes_nome": MES_NOME[dt.month],
        "dt_emissao": dt.strftime("%Y-%m-%d"),
        "empresa": empresa, "rem_cnpj": rem_cnpj,
        "transp_nome": transp_nome, "transp_cnpj": transp_cnpj,
        "cliente": cli_nome, "cli_cnpj": cli_cnpj,
        "uf_dest": uf_dest, "mun_dest": mun_dest, "regiao": regiao,
        "tipo_op": tipo_op,
        "v_frete": round(v_frete, 2), "v_merc": round(v_merc, 2),
        "peso": round(peso, 3),
    }


# ───────────────────────────────────────────────────────────────
#  HISTÓRICO
# ───────────────────────────────────────────────────────────────
def load_hist():
    if os.path.exists(HISTORICO_FILE):
        try: return json.load(open(HISTORICO_FILE, encoding="utf-8"))
        except: pass
    return {"chaves": [], "ctes": [], "atualizado": None}

def save_hist(h):
    with open(HISTORICO_FILE, "w", encoding="utf-8") as f:
        json.dump(h, f, ensure_ascii=False, separators=(",", ":"))


# ───────────────────────────────────────────────────────────────
#  AGREGAÇÃO
# ───────────────────────────────────────────────────────────────
_r  = lambda v: round(v, 2)
_p  = lambda f, m: round(f/m*100, 2) if m > 0 else None

def agregar(ctes):
    by_mes    = {}
    by_emp    = {e: {"f":0,"m":0,"n":0} for e in ("GENOMMA","INOVALAB","OUTROS")}
    by_tipo   = {t: {"f":0,"m":0,"n":0,"gf":0,"gm":0,"if":0,"im":0} for t in ("CIF","FOB")}
    by_transp = {}
    by_reg    = {}
    by_uf     = {}
    by_cli    = {}

    for c in ctes:
        f, m, e, tp = c["v_frete"], c["v_merc"], c["empresa"], c["tipo_op"]
        uf, rg = c["uf_dest"], c["regiao"]
        tr = c["transp_nome"] or "Sem nome"
        cl = c["cli_cnpj"] or c["cliente"]
        mk = f"{c['ano']}-{c['mes']:02d}"

        by_emp[e if e in by_emp else "OUTROS"]["f"] += f
        by_emp[e if e in by_emp else "OUTROS"]["m"] += m
        by_emp[e if e in by_emp else "OUTROS"]["n"] += 1

        if tp in by_tipo:
            bt = by_tipo[tp]; bt["f"]+=f; bt["m"]+=m; bt["n"]+=1
            if e=="GENOMMA":  bt["gf"]+=f; bt["gm"]+=m
            if e=="INOVALAB": bt["if"]+=f; bt["im"]+=m  # noqa

        if mk not in by_mes:
            by_mes[mk]={"chave":mk,"ano":c["ano"],"mes":c["mes"],"nome":c["mes_nome"],
                        "f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
        bm=by_mes[mk]; bm["f"]+=f; bm["m"]+=m; bm["n"]+=1
        if e=="GENOMMA":  bm["gf"]+=f;  bm["gm"]+=m
        if e=="INOVALAB": bm["inf"]+=f; bm["inm"]+=m

        if tr not in by_transp:
            by_transp[tr]={"nome":tr,"cnpj":c["transp_cnpj"],"f":0,"m":0,"n":0,
                           "gf":0,"gm":0,"inf":0,"inm":0}
        bt2=by_transp[tr]; bt2["f"]+=f; bt2["m"]+=m; bt2["n"]+=1
        if e=="GENOMMA":  bt2["gf"]+=f;  bt2["gm"]+=m
        if e=="INOVALAB": bt2["inf"]+=f; bt2["inm"]+=m

        if rg not in by_reg:
            by_reg[rg]={"regiao":rg,"f":0,"m":0,"n":0}
        by_reg[rg]["f"]+=f; by_reg[rg]["m"]+=m; by_reg[rg]["n"]+=1

        if uf:
            if uf not in by_uf:
                by_uf[uf]={"uf":uf,"regiao":rg,"f":0,"m":0,"n":0}
            by_uf[uf]["f"]+=f; by_uf[uf]["m"]+=m; by_uf[uf]["n"]+=1

        if cl not in by_cli:
            by_cli[cl]={"nome":c["cliente"],"cnpj":c["cli_cnpj"],"empresa":e,
                        "regiao":rg,"uf":uf,"f":0,"m":0,"n":0}
        by_cli[cl]["f"]+=f; by_cli[cl]["m"]+=m; by_cli[cl]["n"]+=1

    meses_out = sorted([{
        "chave":v["chave"],"ano":v["ano"],"mes":v["mes"],"nome":v["nome"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "genomma_frete":_r(v["gf"]),"genomma_merc":_r(v["gm"]),
        "inovalab_frete":_r(v["inf"]),"inovalab_merc":_r(v["inm"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
    } for v in by_mes.values()], key=lambda x:(x["ano"],x["mes"]))

    qmap={}
    for m in meses_out:
        qk=f"Q{((m['mes']-1)//3)+1}/{str(m['ano'])[2:]}"
        if qk not in qmap: qmap[qk]={"q":qk,"ano":m["ano"],"f":0,"m":0,"n":0,"gf":0,"inf":0}
        qmap[qk]["f"]+=m["frete"]; qmap[qk]["m"]+=m["v_merc"]; qmap[qk]["n"]+=m["ctes"]
        qmap[qk]["gf"]+=m["genomma_frete"]; qmap[qk]["inf"]+=m["inovalab_frete"]
    quarters_out=sorted([{"q":v["q"],"ano":v["ano"],"frete":_r(v["f"]),"v_merc":_r(v["m"]),
        "ctes":v["n"],"pct_cts":_p(v["f"],v["m"]),
        "genomma_frete":_r(v["gf"]),"inovalab_frete":_r(v["inf"]),
    } for v in qmap.values()], key=lambda x:(x["ano"],x["q"]))

    transp_out=sorted([{"nome":v["nome"],"cnpj":v["cnpj"],"frete":_r(v["f"]),
        "v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
        "genomma_frete":_r(v["gf"]),"inovalab_frete":_r(v["inf"]),
    } for v in by_transp.values()],key=lambda x:x["frete"],reverse=True)

    regs_out=sorted([{"regiao":v["regiao"],"frete":_r(v["f"]),"v_merc":_r(v["m"]),
        "ctes":v["n"],"pct_cts":_p(v["f"],v["m"]),
    } for v in by_reg.values()],key=lambda x:x["frete"],reverse=True)

    ufs_out=sorted([{"uf":v["uf"],"regiao":v["regiao"],"frete":_r(v["f"]),
        "v_merc":_r(v["m"]),"ctes":v["n"],"pct_cts":_p(v["f"],v["m"]),
    } for v in by_uf.values()],key=lambda x:x["frete"],reverse=True)

    clis_out=sorted([{"nome":v["nome"],"cnpj":v["cnpj"],"empresa":v["empresa"],
        "regiao":v["regiao"],"uf":v["uf"],"frete":_r(v["f"]),"v_merc":_r(v["m"]),
        "ctes":v["n"],"pct_cts":_p(v["f"],v["m"]),
    } for v in by_cli.values()],key=lambda x:x["frete"],reverse=True)[:200]

    emps_out={e:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
        "pct_cts":_p(d["f"],d["m"])} for e,d in by_emp.items()}

    tipos_out={tp:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
        "pct_cts":_p(d["f"],d["m"]),
        "pct_genomma":_p(d["gf"],d["gm"]),
        "pct_inovalab":_p(d["if"],d["im"]),  # noqa
    } for tp,d in by_tipo.items()}

    tf=sum(c["v_frete"] for c in ctes); tm=sum(c["v_merc"] for c in ctes)
    return {
        "totais":{"frete":_r(tf),"v_merc":_r(tm),"ctes":len(ctes),
            "pct_cts":_p(tf,tm),
            "genomma_frete":emps_out["GENOMMA"]["frete"],
            "inovalab_frete":emps_out["INOVALAB"]["frete"],
            "pct_genomma":emps_out["GENOMMA"]["pct_cts"],
            "pct_inovalab":emps_out["INOVALAB"]["pct_cts"]},
        "empresas":emps_out,"tipos":tipos_out,
        "meses":meses_out,"quarters":quarters_out,
        "transportadoras":transp_out,"regioes":regs_out,
        "ufs":ufs_out,"clientes":clis_out,
    }


# ───────────────────────────────────────────────────────────────
#  SUPABASE
# ───────────────────────────────────────────────────────────────
def publicar(payload):
    data = json.dumps(payload, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    url  = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{PATH_JSON}"
    hdrs = {"Authorization":f"Bearer {SUPABASE_ANON}","Content-Type":"application/json",
            "x-upsert":"true","cache-control":"no-cache"}
    try:
        r = requests.post(url, data=data, headers=hdrs, timeout=30)
        if r.status_code in (200,201,204):
            log.info("  ✓ Publicado no Supabase"); return True
        log.error(f"  ✗ Supabase {r.status_code}: {r.text[:300]}"); return False
    except Exception as e:
        log.error(f"  ✗ Conexão: {e}"); return False


# ───────────────────────────────────────────────────────────────
#  MAIN
# ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reprocessar", action="store_true")
    parser.add_argument("--pasta", default=PASTA_CTE)
    args = parser.parse_args()
    pasta = args.pasta

    print("\n" + "═"*58)
    print("  TORRE DE CONTROLE — PROCESSADOR DE CTe XML")
    print(f"  Pasta : {pasta}")
    print(f"  Anos  : {ANOS_FILTRO}")
    print("═"*58)

    if not os.path.exists(pasta):
        log.error(f"Pasta não encontrada: {pasta}")
        print(f"\n  ✗ Pasta não encontrada: {pasta}")
        print("  Edite PASTA_CTE no início do script.")
        input("\n  Pressione Enter..."); sys.exit(1)

    hist = {"chaves":[],"ctes":[],"atualizado":None} if args.reprocessar else load_hist()
    if args.reprocessar: log.info("Modo --reprocessar: histórico zerado")
    else: log.info(f"Histórico: {len(hist['chaves']):,} CTes já processados")

    chaves_ok = set(hist["chaves"])

    log.info(f"Buscando XMLs...")
    xmls = list(set(
        glob.glob(os.path.join(pasta,"**","*.xml"),recursive=True) +
        glob.glob(os.path.join(pasta,"**","*.XML"),recursive=True)
    ))
    log.info(f"  {len(xmls):,} arquivos encontrados")

    if not xmls:
        print("\n  Nenhum XML encontrado.")
        input("  Pressione Enter..."); sys.exit(0)

    novos = []
    for i, fp in enumerate(xmls, 1):
        if i % 1000 == 0:
            log.info(f"  {i:,}/{len(xmls):,} verificados — {len(novos):,} novos")
        r = parse_cte(fp)
        if r and r["chave"] not in chaves_ok:
            novos.append(r)
            chaves_ok.add(r["chave"])

    log.info(f"  Novos CTes válidos: {len(novos):,}")

    if not novos and not args.reprocessar:
        print("\n  Nenhum CTe novo. Dashboard já atualizado!")
        input("\n  Pressione Enter..."); sys.exit(0)

    todos = hist.get("ctes", []) + novos
    log.info(f"Agregando {len(todos):,} CTes...")

    dados = agregar(todos)
    dados["atualizado"]           = datetime.now().strftime("%d/%m/%Y %H:%M")
    dados["pasta_origem"]         = pasta
    dados["anos_filtro"]          = ANOS_FILTRO
    dados["novos_nesta_execucao"] = len(novos)
    dados["total_xmls_lidos"]     = len(xmls)

    with open("cte_dados.json","w",encoding="utf-8") as f:
        json.dump(dados,f,ensure_ascii=False,indent=2)
    log.info("  ✓ cte_dados.json salvo localmente")

    hist["chaves"]     = list(chaves_ok)
    hist["ctes"]       = todos
    hist["atualizado"] = dados["atualizado"]
    save_hist(hist)
    log.info(f"  ✓ Histórico atualizado: {len(todos):,} CTes")

    ok = publicar(dados)

    t = dados["totais"]
    print("\n" + "═"*58 + "\n  RESUMO\n" + "═"*58)
    print(f"  CTes total        : {t['ctes']:>10,}")
    print(f"  Novos hoje        : {len(novos):>10,}")
    print(f"  Frete total       : R$ {t['frete']:>14,.2f}")
    print(f"  Mercadoria        : R$ {t['v_merc']:>14,.2f}")
    print(f"  %CTS              : {(str(t['pct_cts'])+'%') if t['pct_cts'] else '—':>11}")
    print(f"  Genomma %CTS      : {(str(t['pct_genomma'])+'%') if t.get('pct_genomma') else '—':>11}")
    print(f"  Inovalab %CTS     : {(str(t['pct_inovalab'])+'%') if t.get('pct_inovalab') else '—':>11}")
    print(f"  Transportadoras   : {len(dados['transportadoras']):>10,}")
    print(f"  Clientes únicos   : {len(dados['clientes']):>10,}")
    print("═"*58)
    print(f"\n  {'✓ Dashboard atualizado!' if ok else '⚠  Publicação falhou — veja o log'}")
    print("  https://tpinheiro1986.github.io/torre-controle-logistica/custo-servir/\n")
    input("  Pressione Enter para fechar...")

if __name__ == "__main__":
    main()
