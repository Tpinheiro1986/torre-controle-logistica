"""
TORRE DE CONTROLE — PROCESSADOR DE CTe XML  v4
===============================================
REGRAS DE ARQUIVO:
  265xxxxx.xml  → CTe emitido — processa normalmente
  383xxxxx.xml  → Cancelamento de CTe — remove o CTe do histórico

MODO INCREMENTAL ULTRARRÁPIDO:
  - Só verifica mtime do arquivo (sem abrir XMLs antigos)
  - Processa somente arquivos modificados após a última execução
  - Cancelamentos removem o CTe correspondente da agregação

USO:
  python processar_cte.py          → incremental (só novos/cancelamentos)
  python processar_cte.py --tudo   → reprocessa tudo do zero
  python processar_cte.py --pasta X
"""

import os, sys, glob, json, argparse, logging
import xml.etree.ElementTree as ET
from datetime import datetime
import requests

# ═══════════════════════════════════════════════════════════════
#  CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════
PASTA_CTE      = r"Y:\ERP-12\TOTVSCOLAB20-PRD\RECEIVED"
ANOS_FILTRO    = [2025, 2026]
ESTADO_FILE    = "cte_estado.json"
LOG_FILE       = "cte_processador.log"

SUPABASE_URL   = "https://ennsbpibfnuwlvtodukg.supabase.co"
SUPABASE_ANON  = "sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONwI5"
BUCKET         = "dashboards"
PATH_JSON      = "custo-servir/dados.json"

CNPJ_GENOMMA   = "09080907000506"
CNPJ_INOVALAB  = "05510999000167"

# Prefixos de arquivo que serão processados
PREFIXO_CTE        = "214"   # CTe emitido
PREFIXO_CANCELAMENTO = "383" # Evento de cancelamento de CTe

# ═══════════════════════════════════════════════════════════════
NS_CTE   = {"cte":   "http://www.portalfiscal.inf.br/cte"}
NS_EVENT = {"cte":   "http://www.portalfiscal.inf.br/cte",
            "ev":    "http://www.portalfiscal.inf.br/cte"}

REGIOES = {
    "AC":"Norte","AM":"Norte","AP":"Norte","PA":"Norte","RO":"Norte","RR":"Norte","TO":"Norte",
    "AL":"Nordeste","BA":"Nordeste","CE":"Nordeste","MA":"Nordeste","PB":"Nordeste",
    "PE":"Nordeste","PI":"Nordeste","RN":"Nordeste","SE":"Nordeste",
    "DF":"Centro-Oeste","GO":"Centro-Oeste","MS":"Centro-Oeste","MT":"Centro-Oeste",
    "ES":"Sudeste","MG":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","RS":"Sul","SC":"Sul",
}
MNOME = ["","Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

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
#  ESTADO
# ───────────────────────────────────────────────────────────────
def load_estado():
    # tenta estado v4
    if os.path.exists(ESTADO_FILE):
        try:
            return json.load(open(ESTADO_FILE, encoding="utf-8"))
        except Exception:
            pass
    # migra do histórico antigo se existir
    for fname in ("cte_historico.json",):
        if os.path.exists(fname):
            try:
                h = json.load(open(fname, encoding="utf-8"))
                log.info(f"  Migrando {fname} para {ESTADO_FILE}...")
                return {
                    "chaves":       h.get("chaves", []),
                    "cancelados":   [],
                    "ctes":         h.get("ctes", []),
                    "ultima_execucao": None,
                    "atualizado":   h.get("atualizado"),
                }
            except Exception:
                pass
    return {"chaves": [], "cancelados": [], "ctes": [], "ultima_execucao": None, "atualizado": None}

def save_estado(e):
    with open(ESTADO_FILE, "w", encoding="utf-8") as f:
        json.dump(e, f, ensure_ascii=False, separators=(",", ":"))


# ───────────────────────────────────────────────────────────────
#  PARSE CTe (265)
# ───────────────────────────────────────────────────────────────
def _txt(root, tag, ns=None):
    ns = ns or NS_CTE
    el = root.find(".//" + tag, ns)
    return el.text.strip() if el is not None and el.text else None

def parse_cte(filepath: str):
    """Processa arquivo 265 (CTe emitido). Retorna dict ou None."""
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except ET.ParseError:
        return None

    if _txt(root, "cte:cStat") != "100":
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

    emit = root.find(".//cte:emit", NS_CTE)
    transp_nome = transp_cnpj = ""
    if emit is not None:
        transp_nome = (emit.findtext("cte:xNome", namespaces=NS_CTE) or
                       emit.findtext("cte:xFant", namespaces=NS_CTE) or "").strip()[:60]
        transp_cnpj = (emit.findtext("cte:CNPJ",  namespaces=NS_CTE) or "").strip()

    rem = root.find(".//cte:rem", NS_CTE)
    rem_cnpj = rem_nome = ""
    if rem is not None:
        rem_cnpj = (rem.findtext("cte:CNPJ",  namespaces=NS_CTE) or "").strip()
        rem_nome = (rem.findtext("cte:xNome", namespaces=NS_CTE) or "").strip()

    if   rem_cnpj == CNPJ_GENOMMA:        empresa = "GENOMMA"
    elif rem_cnpj == CNPJ_INOVALAB:       empresa = "INOVALAB"
    elif "GENOMMA"  in rem_nome.upper():  empresa = "GENOMMA"
    elif "INOVALAB" in rem_nome.upper():  empresa = "INOVALAB"
    else:                                 empresa = "OUTROS"

    dest = root.find(".//cte:dest", NS_CTE)
    cli_nome = cli_cnpj = ""
    if dest is not None:
        cli_nome = (dest.findtext("cte:xNome", namespaces=NS_CTE) or "").strip()[:60]
        cli_cnpj = (dest.findtext("cte:CNPJ",  namespaces=NS_CTE) or "").strip()

    uf_dest  = (_txt(root, "cte:UFFim")   or "").strip()
    mun_dest = (_txt(root, "cte:xMunFim") or "").strip()
    regiao   = REGIOES.get(uf_dest, "Outros")

    tipo_op = "CIF"
    for obs in root.findall(".//cte:ObsCont", NS_CTE):
        if obs.get("xCampo") == "obsAuxiliar":
            if "FOB" in (obs.findtext("cte:xTexto", namespaces=NS_CTE) or "").upper():
                tipo_op = "FOB"
            break

    peso = 0.0
    for q in root.findall(".//cte:infQ", NS_CTE):
        if "BASE" in (q.findtext("cte:tpMed", namespaces=NS_CTE) or "").upper():
            try: peso = float(q.findtext("cte:qCarga", namespaces=NS_CTE) or 0)
            except: pass

    return {
        "chave":       chave,
        "ano":         dt.year,
        "mes":         dt.month,
        "mes_nome":    MNOME[dt.month],
        "dt_emissao":  dt.strftime("%Y-%m-%d"),
        "empresa":     empresa,
        "transp_nome": transp_nome,
        "transp_cnpj": transp_cnpj,
        "cliente":     cli_nome,
        "cli_cnpj":    cli_cnpj,
        "uf_dest":     uf_dest,
        "mun_dest":    mun_dest,
        "regiao":      regiao,
        "tipo_op":     tipo_op,
        "v_frete":     round(v_frete, 2),
        "v_merc":      round(v_merc,  2),
        "peso":        round(peso, 3),
    }


# ───────────────────────────────────────────────────────────────
#  PARSE CANCELAMENTO (383)
# ───────────────────────────────────────────────────────────────
def parse_cancelamento(filepath: str):
    """
    Processa arquivo 383 (evento de cancelamento).
    Retorna a chave do CTe cancelado, ou None se inválido.
    Tenta múltiplos namespaces pois o XML de evento pode variar.
    """
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except ET.ParseError:
        return None

    # Tenta encontrar chCTe em qualquer namespace
    for el in root.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        if tag == "chCTe" and el.text:
            chave = el.text.strip()
            if len(chave) == 44:
                # Confirma que é cancelamento buscando tpEvento ou cStat de cancelamento
                for el2 in root.iter():
                    tag2 = el2.tag.split("}")[-1] if "}" in el2.tag else el2.tag
                    if tag2 == "tpEvento" and el2.text in ("110111", "110112"):
                        return chave
                    if tag2 == "cStat" and el2.text == "135":
                        return chave
                # Se não confirmou mas tem chave de 44 dígitos, assume cancelamento
                return chave

    return None


# ───────────────────────────────────────────────────────────────
#  AGREGAÇÃO
# ───────────────────────────────────────────────────────────────
_r = lambda v: round(v, 2)
_p = lambda f, m: round(f/m*100, 2) if m > 0 else None

def agregar(ctes: list) -> dict:
    by_mes    = {}
    by_emp    = {e: {"f":0,"m":0,"n":0} for e in ("GENOMMA","INOVALAB","OUTROS")}
    by_tipo   = {t: {"f":0,"m":0,"n":0,"gf":0,"gm":0,"if_":0,"im":0} for t in ("CIF","FOB")}
    by_transp = {}
    by_reg    = {}
    by_uf     = {}
    by_cli    = {}

    for c in ctes:
        f, m, e, tp = c["v_frete"], c["v_merc"], c["empresa"], c["tipo_op"]
        uf, rg = c["uf_dest"], c["regiao"]
        tr = c.get("transp_nome") or "Sem nome"
        cl = c.get("cli_cnpj") or c.get("cliente") or "—"
        mk = f"{c['ano']}-{c['mes']:02d}"

        bep = by_emp.get(e, by_emp["OUTROS"])
        bep["f"]+=f; bep["m"]+=m; bep["n"]+=1

        if tp in by_tipo:
            bt=by_tipo[tp]; bt["f"]+=f; bt["m"]+=m; bt["n"]+=1
            if e=="GENOMMA":  bt["gf"]+=f; bt["gm"]+=m
            if e=="INOVALAB": bt["if_"]+=f; bt["im"]+=m

        if mk not in by_mes:
            by_mes[mk]={"chave":mk,"ano":c["ano"],"mes":c["mes"],"nome":c["mes_nome"],
                        "f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
        bm=by_mes[mk]; bm["f"]+=f; bm["m"]+=m; bm["n"]+=1
        if e=="GENOMMA":  bm["gf"]+=f;  bm["gm"]+=m
        if e=="INOVALAB": bm["inf"]+=f; bm["inm"]+=m

        if tr not in by_transp:
            by_transp[tr]={"nome":tr,"cnpj":c.get("transp_cnpj",""),
                           "f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
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
            by_cli[cl]={"nome":c.get("cliente",""),"cnpj":c.get("cli_cnpj",""),
                        "empresa":e,"regiao":rg,"uf":uf,"f":0,"m":0,"n":0}
        by_cli[cl]["f"]+=f; by_cli[cl]["m"]+=m; by_cli[cl]["n"]+=1

    meses_out = sorted([{
        "chave":v["chave"],"ano":v["ano"],"mes":v["mes"],"nome":v["nome"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":    _p(v["f"],  v["m"]),
        "genomma_frete":  _r(v["gf"]),  "genomma_merc":  _r(v["gm"]),
        "inovalab_frete": _r(v["inf"]), "inovalab_merc": _r(v["inm"]),
        "pct_genomma":  _p(v["gf"],  v["gm"]),
        "pct_inovalab": _p(v["inf"], v["inm"]),
    } for v in by_mes.values()], key=lambda x:(x["ano"],x["mes"]))

    qmap={}
    for m in meses_out:
        qk=f"Q{((m['mes']-1)//3)+1}/{str(m['ano'])[2:]}"
        if qk not in qmap: qmap[qk]={"q":qk,"ano":m["ano"],"f":0,"m":0,"n":0,"gf":0,"inf":0}
        qmap[qk]["f"]+=m["frete"]; qmap[qk]["m"]+=m["v_merc"]; qmap[qk]["n"]+=m["ctes"]
        qmap[qk]["gf"]+=m["genomma_frete"]; qmap[qk]["inf"]+=m["inovalab_frete"]
    quarters_out=sorted([{"q":v["q"],"ano":v["ano"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "genomma_frete":_r(v["gf"]),"inovalab_frete":_r(v["inf"]),
    } for v in qmap.values()],key=lambda x:(x["ano"],x["q"]))

    transp_out=sorted([{"nome":v["nome"],"cnpj":v["cnpj"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
        "genomma_frete":_r(v["gf"]),"inovalab_frete":_r(v["inf"]),
    } for v in by_transp.values()],key=lambda x:x["frete"],reverse=True)

    regs_out=sorted([{"regiao":v["regiao"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
    } for v in by_reg.values()],key=lambda x:x["frete"],reverse=True)

    ufs_out=sorted([{"uf":v["uf"],"regiao":v["regiao"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
    } for v in by_uf.values()],key=lambda x:x["frete"],reverse=True)

    clis_out=sorted([{"nome":v["nome"],"cnpj":v["cnpj"],"empresa":v["empresa"],
        "regiao":v["regiao"],"uf":v["uf"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
    } for v in by_cli.values()],key=lambda x:x["frete"],reverse=True)[:200]

    emps_out={e:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
        "pct_cts":_p(d["f"],d["m"])} for e,d in by_emp.items()}

    tipos_out={tp:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
        "pct_cts":_p(d["f"],d["m"]),
        "pct_genomma":_p(d["gf"],d["gm"]),
        "pct_inovalab":_p(d["if_"],d["im"]),
    } for tp,d in by_tipo.items()}

    tf=sum(c["v_frete"] for c in ctes)
    tm=sum(c["v_merc"]  for c in ctes)

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
def publicar(payload: dict) -> bool:
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
    parser = argparse.ArgumentParser(description="Torre de Controle — Processador CTe v4")
    parser.add_argument("--tudo",  action="store_true", help="Reprocessa tudo do zero")
    parser.add_argument("--pasta", default=PASTA_CTE)
    args = parser.parse_args()
    pasta = args.pasta

    ts_inicio = datetime.now()
    print("\n" + "═"*58)
    print("  TORRE DE CONTROLE — PROCESSADOR DE CTe  v4")
    print(f"  Pasta  : {pasta}")
    print(f"  Filtro : arquivos 265* (CTe) e 383* (Cancelamento)")
    print(f"  Modo   : {'COMPLETO (--tudo)' if args.tudo else 'INCREMENTAL (só novos)'}")
    print("═"*58)

    if not os.path.exists(pasta):
        log.error(f"Pasta não encontrada: {pasta}")
        print(f"\n  ✗ Pasta não encontrada: {pasta}")
        input("\n  Pressione Enter..."); sys.exit(1)

    # ── carrega estado ─────────────────────────────────────
    if args.tudo:
        estado = {"chaves":[],"cancelados":[],"ctes":[],"ultima_execucao":None,"atualizado":None}
        log.info("Modo --tudo: estado zerado")
    else:
        estado = load_estado()
        if "cancelados" not in estado:
            estado["cancelados"] = []

    chaves_ok      = set(estado.get("chaves", []))
    chaves_cancel  = set(estado.get("cancelados", []))
    ultima_ts      = None

    if not args.tudo and estado.get("ultima_execucao"):
        try:
            ultima_ts = datetime.fromisoformat(estado["ultima_execucao"]).timestamp()
            log.info(f"Histórico: {len(chaves_ok):,} CTes | última execução: {estado['ultima_execucao']}")
            log.info(f"  → só arquivos modificados após essa data")
        except Exception:
            ultima_ts = None

    # ── busca arquivos 265* e 383* ─────────────────────────
    log.info("Buscando XMLs...")
    ts_busca = datetime.now()

    xmls_265 = []
    xmls_383 = []
    for fp in glob.glob(os.path.join(pasta, "**", "*.xml"), recursive=True):
        nome = os.path.basename(fp)
        if nome.startswith(PREFIXO_CTE):
            xmls_265.append(fp)
        elif nome.startswith(PREFIXO_CANCELAMENTO):
            xmls_383.append(fp)
    # Também testa maiúsculas
    for fp in glob.glob(os.path.join(pasta, "**", "*.XML"), recursive=True):
        nome = os.path.basename(fp)
        if nome.startswith(PREFIXO_CTE):
            xmls_265.append(fp)
        elif nome.startswith(PREFIXO_CANCELAMENTO):
            xmls_383.append(fp)

    elapsed_busca = (datetime.now()-ts_busca).seconds
    log.info(f"  {len(xmls_265):,} arquivos CTe (265*) encontrados")
    log.info(f"  {len(xmls_383):,} arquivos Cancelamento (383*) encontrados")
    log.info(f"  Busca concluída em {elapsed_busca}s")

    # ── filtra por mtime ───────────────────────────────────
    if ultima_ts:
        cands_265 = [fp for fp in xmls_265 if os.path.getmtime(fp) > ultima_ts]
        cands_383 = [fp for fp in xmls_383 if os.path.getmtime(fp) > ultima_ts]
        log.info(f"  Novos desde última execução: {len(cands_265):,} CTes | {len(cands_383):,} Cancelamentos")
    else:
        cands_265 = xmls_265
        cands_383 = xmls_383

    # ── processa cancelamentos primeiro ────────────────────
    novos_cancelados = 0
    ts_proc = datetime.now()
    for fp in cands_383:
        chave_cancelada = parse_cancelamento(fp)
        if chave_cancelada and chave_cancelada not in chaves_cancel:
            chaves_cancel.add(chave_cancelada)
            novos_cancelados += 1
            log.debug(f"  Cancelamento: {chave_cancelada}")

    if novos_cancelados:
        log.info(f"  {novos_cancelados:,} cancelamentos novos processados")

    # ── processa CTes novos (265) ──────────────────────────
    novos_ctes = []
    for i, fp in enumerate(cands_265, 1):
        if i % 2000 == 0:
            elapsed = (datetime.now()-ts_proc).seconds
            log.info(f"  {i:,}/{len(cands_265):,} — {len(novos_ctes):,} novos — {elapsed}s")

        resultado = parse_cte(fp)
        if resultado is None:
            continue
        if resultado["chave"] in chaves_ok:
            continue
        if resultado["chave"] in chaves_cancel:
            log.debug(f"  CTe cancelado ignorado: {resultado['chave']}")
            continue

        novos_ctes.append(resultado)
        chaves_ok.add(resultado["chave"])

    log.info(f"  ✓ {len(novos_ctes):,} CTes novos | {novos_cancelados:,} cancelamentos em {(datetime.now()-ts_proc).seconds}s")

    # ── aplica cancelamentos ao histórico existente ────────
    historico_anterior = estado.get("ctes", [])
    cancelados_removidos = 0
    if novos_cancelados and historico_anterior:
        antes = len(historico_anterior)
        historico_anterior = [c for c in historico_anterior if c["chave"] not in chaves_cancel]
        cancelados_removidos = antes - len(historico_anterior)
        if cancelados_removidos:
            log.info(f"  {cancelados_removidos:,} CTes removidos do histórico por cancelamento")

    if not novos_ctes and not novos_cancelados and not args.tudo:
        estado["ultima_execucao"] = ts_inicio.isoformat()
        save_estado(estado)
        print(f"\n  Nenhuma novidade. Dashboard já atualizado!")
        input("\n  Pressione Enter..."); sys.exit(0)

    # ── agrega ─────────────────────────────────────────────
    todos_ctes = historico_anterior + novos_ctes
    log.info(f"Agregando {len(todos_ctes):,} CTes ativos...")
    ts_agg = datetime.now()
    dados = agregar(todos_ctes)
    log.info(f"  ✓ Agregação em {(datetime.now()-ts_agg).seconds}s")

    dados["atualizado"]           = ts_inicio.strftime("%d/%m/%Y %H:%M")
    dados["pasta_origem"]         = pasta
    dados["anos_filtro"]          = ANOS_FILTRO
    dados["novos_nesta_execucao"] = len(novos_ctes)
    dados["cancelamentos"]        = novos_cancelados
    dados["total_265"]            = len(xmls_265)
    dados["total_383"]            = len(xmls_383)

    # ── salva local ────────────────────────────────────────
    with open("cte_dados.json","w",encoding="utf-8") as f:
        json.dump(dados,f,ensure_ascii=False,indent=2)
    log.info("  ✓ cte_dados.json salvo")

    # ── atualiza estado ────────────────────────────────────
    estado["chaves"]          = list(chaves_ok)
    estado["cancelados"]      = list(chaves_cancel)
    estado["ctes"]            = todos_ctes
    estado["ultima_execucao"] = ts_inicio.isoformat()
    estado["atualizado"]      = dados["atualizado"]
    save_estado(estado)
    log.info(f"  ✓ Estado salvo: {len(todos_ctes):,} CTes ativos | {len(chaves_cancel):,} cancelados")

    # ── publica ────────────────────────────────────────────
    ok = publicar(dados)

    # ── resumo ─────────────────────────────────────────────
    t = dados["totais"]
    total_tempo = (datetime.now()-ts_inicio).seconds
    print("\n" + "═"*58 + "\n  RESUMO\n" + "═"*58)
    print(f"  Tempo total       : {total_tempo}s")
    print(f"  Arquivos 265*     : {len(xmls_265):>10,}")
    print(f"  Arquivos 383*     : {len(xmls_383):>10,}")
    print(f"  CTes novos        : {len(novos_ctes):>10,}")
    print(f"  Cancelamentos     : {novos_cancelados:>10,}")
    print(f"  Removidos do hist : {cancelados_removidos:>10,}")
    print(f"  CTes ativos       : {len(todos_ctes):>10,}")
    print(f"  Frete total       : R$ {t['frete']:>14,.2f}")
    print(f"  Mercadoria        : R$ {t['v_merc']:>14,.2f}")
    print(f"  %CTS              : {(str(t['pct_cts'])+'%') if t['pct_cts'] else '—':>11}")
    print(f"  Genomma %CTS      : {(str(t['pct_genomma'])+'%') if t.get('pct_genomma') else '—':>11}")
    print(f"  Inovalab %CTS     : {(str(t['pct_inovalab'])+'%') if t.get('pct_inovalab') else '—':>11}")
    print(f"  Transportadoras   : {len(dados['transportadoras']):>10,}")
    print(f"  Clientes          : {len(dados['clientes']):>10,}")
    print("═"*58)
    print(f"\n  {'✓ Dashboard atualizado!' if ok else '⚠  Publicação falhou — veja o log'}")
    print("  https://tpinheiro1986.github.io/torre-controle-logistica/custo-servir/\n")
    input("  Pressione Enter para fechar...")

if __name__ == "__main__":
    main()
