"""
TORRE DE CONTROLE -- PROCESSADOR DE CTe XML  v6
================================================
CLASSIFICACAO CORRIGIDA:
  1. rem vazio OU dest vazio -> IGNORAR (descartar)
  2. rem = Genomma/Inovalab -> OUTBOUND
  3. rem = fornecedor da lista INBOUND -> INBOUND (independente do dest)
  4. dest = Genomma/Inovalab + rem nao identificado -> REVERSA
  5. Demais -> OUTBOUND

ARQUIVOS:
  214xxxxx.xml -> CTe emitido
  383xxxxx.xml -> Cancelamento de CTe

USO:
  python processar_cte.py                -> incremental (so novos)
  python processar_cte.py --tudo         -> reprocessa tudo do zero
  python processar_cte.py --reclassificar -> reclassifica historico sem reler XMLs
  python processar_cte.py --pasta X      -> pasta alternativa
"""

import os, sys, json, argparse, logging
import xml.etree.ElementTree as ET
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import requests

# ===============================================================
#  CONFIGURACAO -- edite apenas esta secao
# ===============================================================
PASTA_CTE      = r"Y:\ERP-12\TOTVSCOLAB20-PRD\RECEIVED"
ANOS_FILTRO    = [2025, 2026]
ESTADO_FILE    = "cte_estado.json"
LOG_FILE       = "cte_processador.log"

SUPABASE_URL   = "https://ennsbpibfnuwlvtodukg.supabase.co"
SUPABASE_KEY   = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVubnNicGliZm51d2x2dG9kdWtnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDM4NDgzNywiZXhwIjoyMDg5OTYwODM3fQ.gnCLe-XvoWJoiVEG4jRCPCdX8OsevXACk0TISgo9S04"   # <- chave Legacy service_role
BUCKET         = "dashboards"
PATH_JSON      = "custo-servir/dados.json"

CNPJ_GENOMMA   = "09080907000506"
CNPJ_INOVALAB  = "05510999000167"

PREFIXO_CTE          = "214"
PREFIXO_CANCELAMENTO = "383"

# CNPJs para DESCARTAR completamente (nao entram em nenhuma operacao)
CNPJS_DESCARTAR = {
    "05882643000120", "11253589000156", "23850588000178", "09494467000100",
    "50569035000114", "27752626000100", "28545604000132", "36274721000218",
    "19700976000103", "10730615000127", "19479536000160", "00833219000252",
    "05530576001903", "33962636000173", "05777666000174", "27406208000161",
    "36043292000197", "04814618000146", "15245792000131", "20872887000115",
    "35856333000100", "05057690000139", "20275520000114", "26153828000173",
}

# CNPJs de remetentes INBOUND (fornecedores que entregam para Genomma/Inovalab)
CNPJS_INBOUND = {
    "04660567000145", "04656253000179", "01233103000326", "02029746000315",
    "06215096000191", "19426695000104", "44363661000580", "46179008000320",
    "60665981001270", "61517397000188", "60665981000975", "87375952000178",
    "01858973000129", "09601564000154", "21378906001277", "04581264000137",
    "01287021000100", "04334187000110",
}

# Nomes parciais como fallback (quando CNPJ nao estiver na lista mas nome bate)
INBOUND_NOMES = [
    "TBC", "MARIOL", "BELLA PLUS", "CRA MAIS", "SEAL LACRES",
    "GLENMARK", "BRASTERAPIC", "INOVAT GUARU", "THERASKIN",
    "GRECO E GUERREIRO", "ANOVIS", "AIRELA", "LEBON", "UNITHER",
    "UNIAO QUIMICA", "MAPPEL", "NANOVETORES", "CONNECTA",
    "PORTO SECO SUL", "ATHENAS", "FIRMO CAVALCANTI",
    "RCR REPRESENTACOES", "DELL COMPUTADORES", "GOOXXY",
]

# ===============================================================
NS = {"cte": "http://www.portalfiscal.inf.br/cte"}
CNPJS_GI = None  # preenchido em runtime

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
#  CLASSIFICACAO
# ───────────────────────────────────────────────────────────────
def classificar_op(rem_cnpj, rem_nome, dest_cnpj, dest_nome):
    """
    Regras em ordem de prioridade:
    1. rem CNPJ na lista DESCARTAR -> None (ignorar)
    2. rem vazio -> None (descartar)
    3. rem = Genomma/Inovalab -> OUTBOUND
    4. rem CNPJ na lista INBOUND -> INBOUND
    4b. rem nome contem termo INBOUND (fallback) -> INBOUND
    5. dest = Genomma/Inovalab -> REVERSA
    6. Demais -> OUTBOUND
    """
    ru = (rem_nome  or "").strip().upper()
    du = (dest_nome or "").strip().upper()
    rc = (rem_cnpj  or "").strip().replace(".", "").replace("/", "").replace("-", "")
    dc = (dest_cnpj or "").strip().replace(".", "").replace("/", "").replace("-", "")

    # Regra 1: CNPJ na lista de descarte -> ignorar
    if rc in CNPJS_DESCARTAR or dc in CNPJS_DESCARTAR:
        return None

    # Regra 2: remetente vazio -> descartar
    if not ru and not rc:
        return None

    # Regra 3: Genomma/Inovalab como remetente -> OUTBOUND
    if rc in {CNPJ_GENOMMA, CNPJ_INOVALAB} or "GENOMMA" in ru or "INOVALAB" in ru:
        return "OUTBOUND"

    # Regra 4: CNPJ do remetente na lista INBOUND -> INBOUND
    if rc in CNPJS_INBOUND:
        return "INBOUND"

    # Regra 4b: nome do remetente contem termo INBOUND (fallback)
    for nome in INBOUND_NOMES:
        if nome in ru:
            return "INBOUND"

    # Regra 5: Genomma/Inovalab como destinatario -> REVERSA
    if dc in {CNPJ_GENOMMA, CNPJ_INOVALAB} or "GENOMMA" in du or "INOVALAB" in du:
        return "REVERSA"

    # Regra 6: default
    return "OUTBOUND"


# ───────────────────────────────────────────────────────────────
#  ESTADO
# ───────────────────────────────────────────────────────────────
def load_estado():
    if os.path.exists(ESTADO_FILE):
        try:
            return json.load(open(ESTADO_FILE, encoding="utf-8"))
        except Exception:
            pass
    for fname in ("cte_historico.json",):
        if os.path.exists(fname):
            try:
                h = json.load(open(fname, encoding="utf-8"))
                log.info(f"  Migrando {fname} -> {ESTADO_FILE}")
                return {"chaves": h.get("chaves",[]), "cancelados": [],
                        "ctes": h.get("ctes",[]), "ultima_execucao": None}
            except Exception:
                pass
    return {"chaves": [], "cancelados": [], "ctes": [], "ultima_execucao": None}

def save_estado(e):
    with open(ESTADO_FILE, "w", encoding="utf-8") as f:
        json.dump(e, f, ensure_ascii=False, separators=(",",":"))


# ───────────────────────────────────────────────────────────────
#  PARSE CTe
# ───────────────────────────────────────────────────────────────
def _txt(root, tag):
    el = root.find(".//" + tag, NS)
    return el.text.strip() if el is not None and el.text else None

def parse_cte(filepath: str):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError, OSError):
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

    # Transportadora (emitente do CTe)
    emit = root.find(".//cte:emit", NS)
    transp_nome = transp_cnpj = ""
    if emit is not None:
        transp_nome = (emit.findtext("cte:xNome", namespaces=NS) or
                       emit.findtext("cte:xFant", namespaces=NS) or "").strip()[:60]
        transp_cnpj = (emit.findtext("cte:CNPJ", namespaces=NS) or "").strip()

    # Remetente (expedidor — quem manda a mercadoria)
    rem = root.find(".//cte:rem", NS)
    rem_cnpj = rem_nome = ""
    if rem is not None:
        rem_cnpj = (rem.findtext("cte:CNPJ",  namespaces=NS) or "").strip()
        rem_nome = (rem.findtext("cte:xNome", namespaces=NS) or "").strip()

    # Destinatario
    dest = root.find(".//cte:dest", NS)
    cli_nome = cli_cnpj = ""
    if dest is not None:
        cli_nome = (dest.findtext("cte:xNome", namespaces=NS) or "").strip()[:60]
        cli_cnpj = (dest.findtext("cte:CNPJ",  namespaces=NS) or "").strip()

    # Classifica operacao (None = descartar)
    operacao = classificar_op(rem_cnpj, rem_nome, cli_cnpj, cli_nome)
    if operacao is None:
        return None  # remetente vazio -> descarta

    # Empresa = quem eh Genomma/Inovalab no CTe
    ru = rem_nome.upper()
    du = cli_nome.upper()
    if rem_cnpj == CNPJ_GENOMMA or "GENOMMA" in ru:
        empresa = "GENOMMA"
    elif rem_cnpj == CNPJ_INOVALAB or "INOVALAB" in ru:
        empresa = "INOVALAB"
    elif cli_cnpj == CNPJ_GENOMMA or "GENOMMA" in du:
        empresa = "GENOMMA"
    elif cli_cnpj == CNPJ_INOVALAB or "INOVALAB" in du:
        empresa = "INOVALAB"
    else:
        empresa = "OUTROS"

    uf_dest  = (_txt(root, "cte:UFFim") or "").strip()
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
        "chave":       chave,
        "ano":         dt.year,
        "mes":         dt.month,
        "mes_nome":    MNOME[dt.month],
        "dt_emissao":  dt.strftime("%Y-%m-%d"),
        "empresa":     empresa,
        "operacao":    operacao,
        "rem_nome":    rem_nome[:60],
        "rem_cnpj":    rem_cnpj,
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
#  PARSE CANCELAMENTO
# ───────────────────────────────────────────────────────────────
def parse_cancelamento(filepath: str):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError, OSError):
        return None
    for el in root.iter():
        tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
        if tag == "chCTe" and el.text and len(el.text.strip()) == 44:
            chave = el.text.strip()
            for el2 in root.iter():
                tag2 = el2.tag.split("}")[-1] if "}" in el2.tag else el2.tag
                if tag2 == "tpEvento" and el2.text in ("110111","110112"):
                    return chave
                if tag2 == "cStat" and el2.text == "135":
                    return chave
            return chave
    return None


# ───────────────────────────────────────────────────────────────
#  AGREGACAO
# ───────────────────────────────────────────────────────────────
_r = lambda v: round(v, 2)
_p = lambda f, m: round(f/m*100, 2) if m > 0 else None

def _agg_op(ctes):
    by_mes    = {}
    by_emp    = {e: {"f":0,"m":0,"n":0} for e in ("GENOMMA","INOVALAB","OUTROS")}
    by_tipo   = {t: {"f":0,"m":0,"n":0,"gf":0,"gm":0,"if_":0,"im":0} for t in ("CIF","FOB")}
    by_transp = {}
    by_reg    = {}
    by_uf     = {}
    by_rem    = {}
    by_cli    = {}

    for c in ctes:
        f  = c["v_frete"]
        m  = c["v_merc"]
        e  = c["empresa"]
        tp = c["tipo_op"]
        uf = c["uf_dest"]
        rg = c["regiao"]
        tr = c.get("transp_nome") or "Sem nome"
        cl = c.get("cli_cnpj")   or c.get("cliente") or "?"
        rk = c.get("rem_cnpj")   or c.get("rem_nome") or "?"
        mk = f"{c['ano']}-{c['mes']:02d}"

        # empresa
        bep = by_emp.get(e, by_emp["OUTROS"])
        bep["f"]+=f; bep["m"]+=m; bep["n"]+=1

        # tipo CIF/FOB
        if tp in by_tipo:
            bt = by_tipo[tp]
            bt["f"]+=f; bt["m"]+=m; bt["n"]+=1
            if e=="GENOMMA":  bt["gf"]+=f; bt["gm"]+=m
            if e=="INOVALAB": bt["if_"]+=f; bt["im"]+=m

        # mes
        if mk not in by_mes:
            by_mes[mk] = {"chave":mk,"ano":c["ano"],"mes":c["mes"],"nome":c["mes_nome"],
                          "f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
        bm = by_mes[mk]
        bm["f"]+=f; bm["m"]+=m; bm["n"]+=1
        if e=="GENOMMA":  bm["gf"]+=f;  bm["gm"]+=m
        if e=="INOVALAB": bm["inf"]+=f; bm["inm"]+=m

        # transportadora
        if tr not in by_transp:
            by_transp[tr] = {"nome":tr,"cnpj":c.get("transp_cnpj",""),
                             "f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
        bt2 = by_transp[tr]
        bt2["f"]+=f; bt2["m"]+=m; bt2["n"]+=1
        if e=="GENOMMA":  bt2["gf"]+=f;  bt2["gm"]+=m
        if e=="INOVALAB": bt2["inf"]+=f; bt2["inm"]+=m

        # regiao
        if rg not in by_reg:
            by_reg[rg] = {"regiao":rg,"f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
        by_reg[rg]["f"]+=f; by_reg[rg]["m"]+=m; by_reg[rg]["n"]+=1
        if e=="GENOMMA":  by_reg[rg]["gf"]+=f; by_reg[rg]["gm"]+=m
        if e=="INOVALAB": by_reg[rg]["inf"]+=f; by_reg[rg]["inm"]+=m

        # UF
        if uf:
            if uf not in by_uf:
                by_uf[uf] = {"uf":uf,"regiao":rg,"f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
            by_uf[uf]["f"]+=f; by_uf[uf]["m"]+=m; by_uf[uf]["n"]+=1
            if e=="GENOMMA":  by_uf[uf]["gf"]+=f; by_uf[uf]["gm"]+=m
            if e=="INOVALAB": by_uf[uf]["inf"]+=f; by_uf[uf]["inm"]+=m

        # remetente (inbound/reversa)
        if rk not in by_rem:
            by_rem[rk] = {"nome":c.get("rem_nome",""),"cnpj":c.get("rem_cnpj",""),
                          "f":0,"m":0,"n":0}
        by_rem[rk]["f"]+=f; by_rem[rk]["m"]+=m; by_rem[rk]["n"]+=1

        # cliente/destinatario
        if cl not in by_cli:
            by_cli[cl] = {"nome":c.get("cliente",""),"cnpj":c.get("cli_cnpj",""),
                          "empresa":e,"regiao":rg,"uf":uf,"f":0,"m":0,"n":0,"anos":{}}
        by_cli[cl]["f"]+=f; by_cli[cl]["m"]+=m; by_cli[cl]["n"]+=1
        ano_k = str(c["ano"])
        if ano_k not in by_cli[cl]["anos"]:
            by_cli[cl]["anos"][ano_k] = {"f":0,"m":0,"n":0}
        by_cli[cl]["anos"][ano_k]["f"]+=f
        by_cli[cl]["anos"][ano_k]["m"]+=m
        by_cli[cl]["anos"][ano_k]["n"]+=1

    # meses
    meses_out = sorted([{
        "chave":v["chave"],"ano":v["ano"],"mes":v["mes"],"nome":v["nome"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "genomma_frete":_r(v["gf"]),"genomma_merc":_r(v["gm"]),
        "inovalab_frete":_r(v["inf"]),"inovalab_merc":_r(v["inm"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
    } for v in by_mes.values()], key=lambda x:(x["ano"],x["mes"]))

    # quarters
    qmap = {}
    for m2 in meses_out:
        qk = f"Q{((m2['mes']-1)//3)+1}/{str(m2['ano'])[2:]}"
        if qk not in qmap:
            qmap[qk] = {"q":qk,"ano":m2["ano"],"f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0}
        qmap[qk]["f"]  += m2["frete"];          qmap[qk]["m"]  += m2["v_merc"]
        qmap[qk]["n"]  += m2["ctes"]
        qmap[qk]["gf"] += m2["genomma_frete"];   qmap[qk]["gm"] += m2["genomma_merc"]
        qmap[qk]["inf"]+= m2["inovalab_frete"];  qmap[qk]["inm"]+= m2["inovalab_merc"]
    quarters_out = sorted([{
        "q":v["q"],"ano":v["ano"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "genomma_frete":_r(v["gf"]),"genomma_merc":_r(v["gm"]),
        "inovalab_frete":_r(v["inf"]),"inovalab_merc":_r(v["inm"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
    } for v in qmap.values()], key=lambda x:(x["ano"],x["q"]))

    # transportadoras
    transp_out = sorted([{
        "nome":v["nome"],"cnpj":v["cnpj"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
        "genomma_frete":_r(v["gf"]),"inovalab_frete":_r(v["inf"]),
    } for v in by_transp.values()], key=lambda x:x["frete"], reverse=True)

    # regioes
    regs_out = sorted([{
        "regiao":v["regiao"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "genomma_frete":_r(v["gf"]),"genomma_merc":_r(v["gm"]),
        "inovalab_frete":_r(v["inf"]),"inovalab_merc":_r(v["inm"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
    } for v in by_reg.values()], key=lambda x:x["frete"], reverse=True)

    # UFs
    ufs_out = sorted([{
        "uf":v["uf"],"regiao":v["regiao"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "genomma_frete":_r(v["gf"]),"genomma_merc":_r(v["gm"]),
        "inovalab_frete":_r(v["inf"]),"inovalab_merc":_r(v["inm"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
    } for v in by_uf.values()], key=lambda x:x["frete"], reverse=True)

    # remetentes top 100
    rems_out = sorted([{
        "nome":v["nome"],"cnpj":v["cnpj"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
    } for v in by_rem.values()], key=lambda x:x["frete"], reverse=True)[:100]

    # clientes top 200
    clis_out = sorted([{
        "nome":v["nome"],"cnpj":v["cnpj"],"empresa":v["empresa"],
        "regiao":v["regiao"],"uf":v["uf"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "anos":{ano:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
                     "pct_cts":_p(d["f"],d["m"])}
                for ano,d in v["anos"].items()},
    } for v in by_cli.values()], key=lambda x:x["v_merc"], reverse=True)[:200]

    emps_out = {e: {"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
        "pct_cts":_p(d["f"],d["m"])} for e,d in by_emp.items()}

    tipos_out = {tp: {"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
        "pct_cts":_p(d["f"],d["m"]),
        "pct_genomma":_p(d["gf"],d["gm"]),
        "pct_inovalab":_p(d["if_"],d["im"]),
    } for tp,d in by_tipo.items()}

    tf = sum(c["v_frete"] for c in ctes)
    tm = sum(c["v_merc"]  for c in ctes)

    return {
        "totais": {
            "frete":_r(tf),"v_merc":_r(tm),"ctes":len(ctes),
            "pct_cts":_p(tf,tm),
            "genomma_frete":emps_out["GENOMMA"]["frete"],
            "inovalab_frete":emps_out["INOVALAB"]["frete"],
            "pct_genomma":emps_out["GENOMMA"]["pct_cts"],
            "pct_inovalab":emps_out["INOVALAB"]["pct_cts"],
        },
        "empresas":emps_out, "tipos":tipos_out,
        "meses":meses_out, "quarters":quarters_out,
        "transportadoras":transp_out, "regioes":regs_out,
        "ufs":ufs_out, "clientes":clis_out, "remetentes":rems_out,
    }


def agregar(ctes: list) -> dict:
    out_c = [c for c in ctes if c.get("operacao")=="OUTBOUND"]
    in_c  = [c for c in ctes if c.get("operacao")=="INBOUND"]
    rev_c = [c for c in ctes if c.get("operacao")=="REVERSA"]
    return {
        "outbound": _agg_op(out_c),
        "inbound":  _agg_op(in_c),
        "reversa":  _agg_op(rev_c),
        "resumo": {
            "outbound_ctes":  len(out_c),
            "inbound_ctes":   len(in_c),
            "reversa_ctes":   len(rev_c),
            "total_ctes":     len(ctes),
            "outbound_frete": _r(sum(c["v_frete"] for c in out_c)),
            "inbound_frete":  _r(sum(c["v_frete"] for c in in_c)),
            "reversa_frete":  _r(sum(c["v_frete"] for c in rev_c)),
            "total_frete":    _r(sum(c["v_frete"] for c in ctes)),
        }
    }


# ───────────────────────────────────────────────────────────────
#  SUPABASE
# ───────────────────────────────────────────────────────────────
def publicar(payload: dict) -> bool:
    data = json.dumps(payload, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    url  = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{PATH_JSON}"
    hdrs = {"Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "x-upsert": "true", "cache-control": "no-cache"}
    try:
        r = requests.post(url, data=data, headers=hdrs, timeout=60)
        if r.status_code in (200,201,204):
            log.info("  OK Publicado no Supabase"); return True
        log.error(f"  ERRO Supabase {r.status_code}: {r.text[:300]}"); return False
    except Exception as e:
        log.error(f"  ERRO conexao: {e}"); return False


# ───────────────────────────────────────────────────────────────
#  BUSCA DE ARQUIVOS (rapida via scandir)
# ───────────────────────────────────────────────────────────────
def scan_xmls(pasta_raiz, ultima_ts=None):
    """
    Percorre diretorios via os.scandir (muito mais rapido que glob em rede).
    Retorna (xmls_214, xmls_383, cands_214, cands_383).
    cands = arquivos novos (mtime > ultima_ts) ou todos se ultima_ts=None.
    """
    xmls_214, xmls_383, cands_214, cands_383 = [], [], [], []
    stack = [pasta_raiz]
    while stack:
        cur = stack.pop()
        try:
            with os.scandir(cur) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            n = entry.name
                            if not n.lower().endswith('.xml'):
                                continue
                            if n.startswith(PREFIXO_CTE):
                                xmls_214.append(entry.path)
                                if ultima_ts is None or entry.stat().st_mtime > ultima_ts:
                                    cands_214.append(entry.path)
                            elif n.startswith(PREFIXO_CANCELAMENTO):
                                xmls_383.append(entry.path)
                                if ultima_ts is None or entry.stat().st_mtime > ultima_ts:
                                    cands_383.append(entry.path)
                    except (PermissionError, OSError):
                        continue
        except (PermissionError, OSError):
            continue
    return xmls_214, xmls_383, cands_214, cands_383


# ───────────────────────────────────────────────────────────────
#  MAIN
# ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tudo",          action="store_true",
                        help="Reprocessa todos os XMLs do zero (~15 min)")
    parser.add_argument("--reclassificar", action="store_true",
                        help="Reclassifica historico sem reler XMLs (<1 min)")
    parser.add_argument("--pasta",         default=PASTA_CTE)
    args = parser.parse_args()
    pasta = args.pasta
    ts_inicio = datetime.now()

    # ── MODO RECLASSIFICAR (rapido) ─────────────────────────────
    if args.reclassificar:
        print("\n" + "="*58)
        print("  TORRE DE CONTROLE -- RECLASSIFICACAO RAPIDA  v6")
        print("  Reclassifica historico sem reler XMLs")
        print("="*58)
        estado = load_estado()
        ctes = estado.get("ctes", [])
        if not ctes:
            print("\n  Historico vazio. Rode REPROCESSAR_TUDO primeiro.")
            input("  Pressione Enter..."); sys.exit(0)

        log.info(f"Reclassificando {len(ctes):,} CTes...")
        alterados = descartados = 0
        ctes_validos = []
        for c in ctes:
            op_nova = classificar_op(
                c.get("rem_cnpj",""), c.get("rem_nome",""),
                c.get("cli_cnpj",""), c.get("cliente","")
            )
            if op_nova is None:
                descartados += 1
                continue
            if c.get("operacao") != op_nova:
                c["operacao"] = op_nova
                alterados += 1
            ctes_validos.append(c)

        log.info(f"  {alterados:,} reclassificados | {descartados:,} descartados (rem vazio)")

        log.info("Agregando...")
        dados = agregar(ctes_validos)
        dados["atualizado"]           = ts_inicio.strftime("%d/%m/%Y %H:%M")
        dados["pasta_origem"]         = pasta
        dados["anos_filtro"]          = ANOS_FILTRO
        dados["novos_nesta_execucao"] = 0
        dados["cancelamentos"]        = 0

        with open("cte_dados.json","w",encoding="utf-8") as f:
            json.dump(dados,f,ensure_ascii=False,indent=2)

        estado["chaves"]          = [c["chave"] for c in ctes_validos]
        estado["ctes"]            = ctes_validos
        estado["ultima_execucao"] = ts_inicio.isoformat()
        estado["atualizado"]      = dados["atualizado"]
        save_estado(estado)

        ok = publicar(dados)
        r  = dados["resumo"]
        print("\n" + "="*58 + "\n  RESUMO\n" + "="*58)
        print(f"  CTes ativos          : {len(ctes_validos):,}")
        print(f"  Reclassificados      : {alterados:,}")
        print(f"  Descartados (rem vazio): {descartados:,}")
        print(f"  Outbound : {r['outbound_ctes']:,} CTes | R$ {r['outbound_frete']:,.2f}")
        print(f"  Inbound  : {r['inbound_ctes']:,} CTes | R$ {r['inbound_frete']:,.2f}")
        print(f"  Reversa  : {r['reversa_ctes']:,} CTes | R$ {r['reversa_frete']:,.2f}")
        print("="*58)
        print(f"  {'OK Dashboard atualizado!' if ok else 'ERRO Publicacao falhou'}")
        input("  Pressione Enter..."); sys.exit(0)
    # ────────────────────────────────────────────────────────────

    print("\n" + "="*58)
    print("  TORRE DE CONTROLE -- PROCESSADOR DE CTe  v6")
    print(f"  Pasta  : {pasta}")
    print(f"  Filtro : {PREFIXO_CTE}* (CTe) e {PREFIXO_CANCELAMENTO}* (Cancelamento)")
    print(f"  Modo   : {'COMPLETO (--tudo)' if args.tudo else 'INCREMENTAL (so novos)'}")
    print("="*58)

    if not os.path.exists(pasta):
        log.error(f"Pasta nao encontrada: {pasta}")
        input("\n  Pressione Enter..."); sys.exit(1)

    if args.tudo:
        estado = {"chaves":[],"cancelados":[],"ctes":[],"ultima_execucao":None}
    else:
        estado = load_estado()
        if "cancelados" not in estado: estado["cancelados"] = []

    chaves_ok     = set(estado.get("chaves",    []))
    chaves_cancel = set(estado.get("cancelados",[]))
    ultima_ts     = None

    if not args.tudo and estado.get("ultima_execucao"):
        try:
            ultima_ts = datetime.fromisoformat(estado["ultima_execucao"]).timestamp()
            log.info(f"Historico: {len(chaves_ok):,} CTes | ultima exec: {estado['ultima_execucao']}")
        except Exception:
            ultima_ts = None

    # busca arquivos
    log.info("Buscando XMLs...")
    ts_b = datetime.now()
    xmls_214, xmls_383, cands_214, cands_383 = scan_xmls(pasta, ultima_ts)
    elapsed_b = (datetime.now()-ts_b).seconds
    log.info(f"  {len(xmls_214):,} CTe | {len(xmls_383):,} Canc. em {elapsed_b}s")
    if ultima_ts:
        log.info(f"  Novos: {len(cands_214):,} CTe | {len(cands_383):,} Cancelamento")

    # cancelamentos
    novos_cancel = 0
    ts_p = datetime.now()
    for fp in cands_383:
        ch = parse_cancelamento(fp)
        if ch and ch not in chaves_cancel:
            chaves_cancel.add(ch); novos_cancel += 1
    if novos_cancel:
        log.info(f"  {novos_cancel:,} cancelamentos novos")

    # CTes novos — paralelo
    novos_ctes = []
    n_workers = max(1, min(multiprocessing.cpu_count()-1, 8))
    log.info(f"  Processando com {n_workers} workers...")
    BATCH = 5000
    total_proc = 0
    for bs in range(0, len(cands_214), BATCH):
        batch = cands_214[bs:bs+BATCH]
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            futs = {ex.submit(parse_cte, fp): fp for fp in batch}
            for fut in as_completed(futs):
                total_proc += 1
                if total_proc % 2000 == 0:
                    elapsed = (datetime.now()-ts_p).seconds
                    log.info(f"  {total_proc:,}/{len(cands_214):,} -- {len(novos_ctes):,} novos -- {elapsed}s")
                try:
                    res = fut.result()
                except Exception:
                    continue
                if res is None:
                    continue
                if res["chave"] in chaves_ok or res["chave"] in chaves_cancel:
                    continue
                novos_ctes.append(res)
                chaves_ok.add(res["chave"])

    log.info(f"  {len(novos_ctes):,} CTes novos em {(datetime.now()-ts_p).seconds}s")

    # aplica cancelamentos ao historico
    hist = estado.get("ctes", [])
    rem_hist = 0
    if novos_cancel and hist:
        antes = len(hist)
        hist = [c for c in hist if c["chave"] not in chaves_cancel]
        rem_hist = antes - len(hist)
        if rem_hist: log.info(f"  {rem_hist:,} removidos por cancelamento")

    # migra CTes antigos sem campo operacao / reclassifica com nova logica
    for c in hist:
        op = classificar_op(c.get("rem_cnpj",""), c.get("rem_nome",""),
                            c.get("cli_cnpj",""), c.get("cliente",""))
        c["operacao"] = op or "OUTBOUND"

    # descarta CTes com remetente vazio do historico
    hist_valido = [c for c in hist if classificar_op(
        c.get("rem_cnpj",""), c.get("rem_nome",""),
        c.get("cli_cnpj",""), c.get("cliente","")) is not None]

    if not novos_ctes and not novos_cancel and not args.tudo:
        estado["ultima_execucao"] = ts_inicio.isoformat()
        save_estado(estado)
        print("\n  Nenhum CTe novo. Dashboard ja atualizado!")
        input("\n  Pressione Enter..."); sys.exit(0)

    todos = hist_valido + novos_ctes
    log.info(f"Agregando {len(todos):,} CTes...")
    dados = agregar(todos)
    dados["atualizado"]           = ts_inicio.strftime("%d/%m/%Y %H:%M")
    dados["pasta_origem"]         = pasta
    dados["anos_filtro"]          = ANOS_FILTRO
    dados["novos_nesta_execucao"] = len(novos_ctes)
    dados["cancelamentos"]        = novos_cancel

    with open("cte_dados.json","w",encoding="utf-8") as f:
        json.dump(dados,f,ensure_ascii=False,indent=2)
    log.info("  OK cte_dados.json salvo")

    estado["chaves"]          = list(chaves_ok)
    estado["cancelados"]      = list(chaves_cancel)
    estado["ctes"]            = todos
    estado["ultima_execucao"] = ts_inicio.isoformat()
    save_estado(estado)

    ok = publicar(dados)

    r = dados["resumo"]
    tt = (datetime.now()-ts_inicio).seconds
    print("\n" + "="*58 + "\n  RESUMO\n" + "="*58)
    print(f"  Tempo total     : {tt}s")
    print(f"  CTes novos      : {len(novos_ctes):,}")
    print(f"  CTes ativos     : {len(todos):,}")
    print(f"  OUTBOUND        : {r['outbound_ctes']:,} CTes | R$ {r['outbound_frete']:>12,.2f}")
    t = dados["outbound"]["totais"]
    print(f"    Mercadoria    : R$ {t['v_merc']:>12,.2f}")
    print(f"    %CTS          : {str(t['pct_cts'])+'%' if t['pct_cts'] else '—'}")
    print(f"    Genomma %CTS  : {str(t['pct_genomma'])+'%' if t.get('pct_genomma') else '—'}")
    print(f"    Inovalab %CTS : {str(t['pct_inovalab'])+'%' if t.get('pct_inovalab') else '—'}")
    print(f"  INBOUND         : {r['inbound_ctes']:,} CTes | R$ {r['inbound_frete']:>12,.2f}")
    print(f"  REVERSA         : {r['reversa_ctes']:,} CTes | R$ {r['reversa_frete']:>12,.2f}")
    print(f"  Transportadoras : {len(dados['outbound']['transportadoras']):,}")
    print(f"  Clientes        : {len(dados['outbound']['clientes']):,}")
    print("="*58)
    print(f"\n  {'OK Dashboard atualizado!' if ok else 'ERRO Publicacao falhou -- rode REPUBLICAR.bat'}")
    print("  https://tpinheiro1986.github.io/torre-controle-logistica/custo-servir/\n")
    input("  Pressione Enter para fechar...")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
