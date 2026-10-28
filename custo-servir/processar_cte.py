"""
TORRE DE CONTROLE -- PROCESSADOR DE CTe XML  v9.1
================================================
v9.1 (28/out/2026): chave do CT-e lida de infCte/@Id (corrige complementares
  tratados como duplicados) e suporte a chave nova sb_secret_ (header apikey).
v9 (28/out/2026):
  - SEGURANCA: chave do Supabase sai do codigo. Lida de variavel de ambiente
    (SUPABASE_SERVICE_KEY ou SUPABASE_KEY) ou do arquivo .env (pasta do
    script ou pasta-mae). Nunca mais versionar chave no Git.
  - ANOS_FILTRO passa a incluir 2024 (rode REPROCESSAR_TUDO.bat uma vez
    para carregar o historico de 2024, se os XMLs existirem no Y:)
  - CNPJ da Inovalab corrigido (60.698.771/0001-26; o antigo e mantido)
  - Novos campos por CT-e: tp_cte (0 normal, 1 complementar, 2 anulacao,
    3 substituto), tomador (REM/EXP/RECEB/DEST/OUTROS), tomador_cnpj e
    tomador_gi (1 = Genomma/Inovalab paga o frete)
  - COMPLEMENTAR passa a ser identificado tambem pelo tpCTe = 1
  - Peso por transportadora agora sai por ano e por mes (antes so total)
  - ctes_raw.json ganha: tpc, tom, tgi, mz (matriz), ab (abrev), rg (regiao)
  - Conferencia de carga: processados / aceitos / rejeitados (por motivo e
    ano) / duplicados / cancelados -> cte_conferencia.json e log
  - --sem-publicar: roda tudo sem enviar ao Supabase (para testes)
v8 (11/set/2026):
  - Nova coluna "Veiculo" extraida do xObs completo (carreta, truck, toco,
    3/4, vuc, van, fiorino, bitrem, rodotrem...)
  - Nova coluna "Restricao Veiculo" para exigencias do cliente
    (ex.: "VEICULO SOMENTE ACIMA DE 3/4") -- nao e o veiculo que rodou
  - IMPORTANTE: rode uma vez com --tudo para preencher o historico 2025/2026
    (o estado antigo guarda o xObs cortado em 100 caracteres)
v7 (26/mai/2026):
  - Pools de custo (rk_frete / pc_frete) na aba Custo do dashboard
  - Identificacao da ATIVA e FL Brasil por CNPJ raiz (matriz + filiais)
  - Lista de 23 transportadoras a excluir (CNPJS_TRANSP_EXCLUIR)

Para confirmar que esta executando esta versao:
  No log do RECLASSIFICAR.bat deve aparecer "descartados (transp excluida)"
  No cte_dados.json gerado, o nivel de cada regiao deve ter rk_frete e pc_frete

CLASSIFICACAO:
  1. rem vazio OU dest vazio -> IGNORAR (descartar)
  2. transportadora em CNPJS_TRANSP_EXCLUIR -> IGNORAR
  3. rem = Genomma/Inovalab -> OUTBOUND
  4. rem = fornecedor da lista INBOUND -> INBOUND (independente do dest)
  5. dest = Genomma/Inovalab + rem nao identificado -> REVERSA
  6. Demais -> OUTBOUND

ARQUIVOS:
  214xxxxx.xml -> CTe emitido
  383xxxxx.xml -> Cancelamento de CTe

USO:
  python processar_cte.py                -> incremental (so novos)
  python processar_cte.py --tudo         -> reprocessa tudo do zero
  python processar_cte.py --reclassificar -> reclassifica historico sem reler XMLs
  python processar_cte.py --pasta X      -> pasta alternativa
"""

import os, sys, json, argparse, logging, re, unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import requests

# ===============================================================
#  CONFIGURACAO -- edite apenas esta secao
# ===============================================================
PASTA_CTE      = r"Y:\ERP-12\TOTVSCOLAB20-PRD\RECEIVED"
ANOS_FILTRO    = [2024, 2025, 2026]
ESTADO_FILE    = "cte_estado.json"
LOG_FILE       = "cte_processador.log"

SUPABASE_URL   = "https://ennsbpibfnuwlvtodukg.supabase.co"
SUPABASE_KEY   = None  # v9: lida de .env / variavel de ambiente (ver _carregar_chave)
BUCKET         = "dashboards"
PATH_JSON      = "custo-servir/dados.json"
PATH_RAW       = "custo-servir/ctes_raw.json"

CNPJ_GENOMMA   = "09080907000506"
CNPJ_INOVALAB  = "60698771000126"           # v9: CNPJ real visto nos CT-es
CNPJS_INOVALAB = {"60698771000126", "05510999000167"}  # atual + antigo do script v8
CNPJS_EMPRESA  = {CNPJ_GENOMMA, "09080907000182"} | CNPJS_INOVALAB

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

# CNPJs de TRANSPORTADORAS a excluir do relatorio
# (colocaram Genomma como tomadora mas o frete nao e nosso)
CNPJS_TRANSP_EXCLUIR = {
    "04281818000180",  # DEL LOGISTIC TRANSPORTES LTDA
    "23850588000178",  # FAZENDA SAO JUDAS LOGISTICA LTDA
    "05882643000120",  # MADRI EXPRESS LOGISTICA LTDA
    "15769911000155",  # MKT LOG LOGISTICA INTEGRADA LTDA
    "18485037000112",  # SLIM LOG SERVICOS LOGISTICOS LTDA
    "11253589000156",  # TRANSUL ENCOMENDAS LTDA ME
    "74155052000173",  # UPS DO BRASIL REMESSAS EXPRESSAS LTDA
    # 2a leva (mai/2026)
    "05777666000174",  # DIOR DE AZEVEDO TRANSPORTES LTDA
    "20872887000115",  # G LOG TRANSPORTES RODOVIARIO LTDA
    "33962636000173",  # LIBFARMA DISTRIBUIDORA DE MEDICAMENTOS E PRODUTOS HOSPITALAR
    "02317708000287",  # QUASAR TRANSPORTE E LOGISTICA LTDA
    "19700976000103",  # SOLUCIONA LOGISTICA E TRANSPORTE LTDA
    "26153828000173",  # LL TRANSPORTES E LOGISTICA LTDA
    "50569035000114",  # HLN TRANSPORTES E LOGISTICA LTDA
    "20275520000114",  # COMAR LOGISTICA LTDA
    "05530576001903",  # LOGFAR LOGISTICA LTDA
    "19479536000160",  # EPOCA TRANSPORTE E LOGISTICA LTDA
    "27752626000100",  # NTM EXPRESS TRANSPORTES EIRELI ME
    "00833219000252",  # SOLIDEZ TRANSPORTES LTDA
    "27406208000161",  # D.M.X LOGISTICA E SERVICOS AUXILIARES LTDA
    "35856333000100",  # R&R ISA'S TRANSPORTES LTDA
    "28545604000132",  # TSG LOG TRANSPORTES E ARM. LTDA
    "10730615000127",  # FAMILY FIGUEIREDO E CUNHA LTDA
}

def _carregar_chave():
    """Le a chave do Supabase SEM deixa-la no codigo.
    Ordem: variavel SUPABASE_SERVICE_KEY, variavel SUPABASE_KEY,
    arquivo .env na pasta do script e depois na pasta-mae."""
    for var in ("SUPABASE_SERVICE_KEY", "SUPABASE_KEY"):
        v = (os.environ.get(var) or "").strip().strip('"').strip("'")
        if v:
            return v
    base = os.path.dirname(os.path.abspath(__file__))
    for pasta in (base, os.path.dirname(base)):
        fp = os.path.join(pasta, ".env")
        if not os.path.exists(fp):
            continue
        try:
            for linha in open(fp, encoding="utf-8-sig"):
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                k, v = linha.split("=", 1)
                if k.strip() in ("SUPABASE_SERVICE_KEY", "SUPABASE_KEY"):
                    v = v.strip().strip('"').strip("'")
                    if v:
                        return v
        except OSError:
            pass
    return None


def transp_excluida(transp_cnpj):
    """True se a transportadora deve ser excluida do relatorio."""
    tc = (transp_cnpj or "").strip().replace(".", "").replace("/", "").replace("-", "")
    return tc in CNPJS_TRANSP_EXCLUIR

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
    if rc in CNPJS_EMPRESA or "GENOMMA" in ru or "INOVALAB" in ru:
        return "OUTBOUND"

    # Regra 4: CNPJ do remetente na lista INBOUND -> INBOUND
    if rc in CNPJS_INBOUND:
        return "INBOUND"

    # Regra 4b: nome do remetente contem termo INBOUND (fallback)
    for nome in INBOUND_NOMES:
        if nome in ru:
            return "INBOUND"

    # Regra 5: Genomma/Inovalab como destinatario -> REVERSA
    if dc in CNPJS_EMPRESA or "GENOMMA" in du or "INOVALAB" in du:
        return "REVERSA"

    # Regra 6: default
    return "OUTBOUND"


# ───────────────────────────────────────────────────────────────
#  VEICULO A PARTIR DO xObs
# ───────────────────────────────────────────────────────────────
# Le o xObs COMPLETO (antes do truncamento) e devolve:
#   veiculo     -> tipo canonico do veiculo que rodou ("CARRETA", "TRUCK", ...)
#   restr_veic  -> exigencia de veiculo feita pelo CLIENTE (nao e o veiculo usado)
#
# Regra de ouro: trechos do tipo "VEICULO SOMENTE ACIMA DE 3/4" ou
# "CLIENTE SO RECEBE C/ VEIC ..." sao RESTRICAO de recebimento e nunca
# alimentam a coluna Veiculo.

def _norm(s):
    """Maiusculas, sem acento, espacos colapsados."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s.upper()).strip()

# --- trechos que sao RESTRICAO do cliente (removidos antes de procurar veiculo)
_RE_RESTRICAO = [
    re.compile(r"VEICULO[S]?\s+SOMENTE(?:(?!\s[-/]\s)[^|;.]){0,70}"),
    re.compile(r"SOMENTE\s+VEICULO[S]?(?:(?!\s[-/]\s)[^|;.]){0,70}"),
    re.compile(r"(?:SO|SOMENTE|APENAS)\s+RECEBE(?:(?!\s[-/]\s)[^|;.]){0,70}"),
    re.compile(r"RECEBE\s+(?:SOMENTE|APENAS|SO)\b(?:(?!\s[-/]\s)[^|;.]){0,70}"),
    re.compile(r"NAO\s+(?:RECEBE|ACEITA|ENTRA|DESCARREGA)\b(?:(?!\s[-/]\s)[^|;.]){0,70}"),
    re.compile(r"(?:PROIBIDO|VEDADO|NAO\s+PERMITIDO)\b(?:(?!\s[-/]\s)[^|;.]){0,70}"),
    re.compile(r"(?:ACIMA|ABAIXO)\s+DE\s+(?:3\s*/\s*4|TOCO|TRUCK|CARRETA|VUC|VAN)(?:(?!\s[-/]\s)[^|;.]){0,40}"),
]

# --- vocabulario de veiculos: (regex, rotulo canonico) em ordem de prioridade
_VEICULOS = [
    (re.compile(r"\bRODOTREN?S?\b|\bRODOTREM\b"),                 "RODOTREM"),
    (re.compile(r"\bBI\s*-?\s*TREM\b|\bBITREN\b"),                "BITREM"),
    (re.compile(r"\bCARRETA\w*\b|\bCARRETAS\b|\bCAVALO\b|\bSEMI\s*-?\s*REBOQUE\b"), "CARRETA"),
    (re.compile(r"\bTRUCK\w*\b|\bTRUCADO\b"),                     "TRUCK"),
    (re.compile(r"\bTOCO\b|\bTOCCO\b"),                           "TOCO"),
    (re.compile(r"(?<!\d)3\s*/\s*4(?!\d)|\bTRES\s+QUARTOS\b"),    "3/4"),
    (re.compile(r"\bVUC\b"),                                      "VUC"),
    (re.compile(r"\bSPRINTER\b|\bMASTER\b|\bDUCATO\b|\bJUMPER\b"),"VAN"),
    (re.compile(r"\bVAN\b|\bVANS\b"),                             "VAN"),
    (re.compile(r"\bFIORINO\b|\bDOBLO\b|\bKANGOO\b|\bPARTNER\b"), "FIORINO"),
    (re.compile(r"\bKOMBI\b"),                                    "KOMBI"),
    (re.compile(r"\bUTILITARIO\w*\b"),                            "UTILITARIO"),
    (re.compile(r"\bMOTO\b|\bMOTOCICLETA\b|\bMOTOBOY\b"),         "MOTO"),
    (re.compile(r"\bCARRO\b|\bPASSEIO\b"),                        "CARRO"),
]

# --- contextos que anunciam explicitamente o veiculo (tem prioridade)
_RE_CONTEXTO = [
    re.compile(r"TIPO\s+DE\s+VEICULO\s*[:\-]?\s*([A-Z0-9/ ]{2,25})"),
    re.compile(r"VEICULO\s*[:\-]\s*([A-Z0-9/ ]{2,25})"),
    re.compile(r"SERVICO\s+DE\s+([A-Z0-9/ ]{2,25})"),
    re.compile(r"MODAL\s*[:\-]?\s*([A-Z0-9/ ]{2,25})"),
]


def _match_veiculo(texto):
    """Devolve (rotulo, posicao) do primeiro veiculo encontrado, ou (None, -1)."""
    achados = []
    for rx, rotulo in _VEICULOS:
        m = rx.search(texto)
        if m:
            achados.append((m.start(), rotulo))
    if not achados:
        return None, -1
    achados.sort()                      # o que aparece primeiro no texto vence
    return achados[0][1], achados[0][0]


def extrair_veiculo(x_obs):
    """(veiculo, restricao_veiculo) a partir do xObs completo."""
    t = _norm(x_obs)
    if not t:
        return "", ""

    # 1) isola e remove os trechos de restricao
    restr = []
    for rx in _RE_RESTRICAO:
        for m in rx.finditer(t):
            restr.append(m.group(0).strip())
    limpo = t
    for r in restr:
        limpo = limpo.replace(r, " ")

    # mantem na restricao so o que realmente fala de veiculo
    restr_txt = ""
    for r in restr:
        if _match_veiculo(r)[0] or "VEICULO" in r or "VEIC" in r:
            restr_txt = r[:80]
            break

    # 2) contexto explicito ("Tipo de Veiculo - Truck")
    for rx in _RE_CONTEXTO:
        m = rx.search(limpo)
        if m:
            v, _ = _match_veiculo(m.group(1))
            if v:
                return v, restr_txt

    # 3) varredura livre no texto limpo
    v, _ = _match_veiculo(limpo)
    return (v or ""), restr_txt


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

def _rej(motivo, ano=None):
    """v9: rejeicao com motivo (alimenta a conferencia de carga)."""
    return {"_rej": motivo, "ano": ano}


def parse_cte(filepath: str):
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
    except (ET.ParseError, FileNotFoundError, OSError):
        return _rej("xml_invalido")

    _ano = None
    dhemi = _txt(root, "cte:dhEmi") or ""
    try:
        dt = datetime.fromisoformat(dhemi[:19])
        _ano = dt.year
    except ValueError:
        dt = None

    if _txt(root, "cte:cStat") != "100":
        return _rej("nao_autorizado", _ano)

    # v9.1: a chave do PROPRIO CT-e vem do atributo Id de infCte ("CTe"+44 digitos)
    # ou do protocolo. Antes usava o primeiro chCTe do XML, que num CT-e
    # complementar/substituto e a chave do CT-e ORIGINAL -> o complementar
    # era descartado como "duplicado" (ou tomava o lugar do original).
    chave = None
    _inf = root.find(".//cte:infCte", NS)
    if _inf is not None:
        _id = (_inf.get("Id") or "").strip()
        if _id.upper().startswith("CTE") and len(_id) == 47:
            chave = _id[3:]
    if not chave:
        chave = _txt(root, "cte:infProt/cte:chCTe") or _txt(root, "cte:protCTe/cte:infProt/cte:chCTe")
    if not chave:
        return _rej("sem_chave", _ano)

    if dt is None:
        return _rej("data_invalida")

    if dt.year not in ANOS_FILTRO:
        return _rej("fora_do_periodo", _ano)

    v_frete = float(_txt(root, "cte:vTPrest") or 0)
    v_merc  = float(_txt(root, "cte:vCarga")  or 0)

    # Transportadora (emitente do CTe)
    emit = root.find(".//cte:emit", NS)
    transp_nome = transp_cnpj = ""
    if emit is not None:
        transp_nome = (emit.findtext("cte:xNome", namespaces=NS) or
                       emit.findtext("cte:xFant", namespaces=NS) or "").strip()[:60]
        transp_cnpj = (emit.findtext("cte:CNPJ", namespaces=NS) or "").strip()

    # Descarta CTes de transportadoras excluidas (Genomma como tomadora indevida)
    if transp_excluida(transp_cnpj):
        return _rej("transp_excluida", _ano)

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
        return _rej("descartado_classificacao", _ano)  # rem vazio / lista de descarte

    # Empresa = quem eh Genomma/Inovalab no CTe
    ru = rem_nome.upper()
    du = cli_nome.upper()
    if rem_cnpj.startswith(CNPJ_GENOMMA[:8]) or "GENOMMA" in ru:
        empresa = "GENOMMA"
    elif rem_cnpj in CNPJS_INOVALAB or "INOVALAB" in ru:
        empresa = "INOVALAB"
    elif cli_cnpj.startswith(CNPJ_GENOMMA[:8]) or "GENOMMA" in du:
        empresa = "GENOMMA"
    elif cli_cnpj in CNPJS_INOVALAB or "INOVALAB" in du:
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

    # Numero do CTe, serie e caracteristicas
    ide = root.find(".//cte:ide", NS)
    n_cte = serie_cte = cfop = ""
    if ide is not None:
        n_cte    = ide.findtext("cte:nCT",   namespaces=NS) or ""
        serie_cte= ide.findtext("cte:serie", namespaces=NS) or ""
        cfop     = ide.findtext("cte:CFOP",  namespaces=NS) or ""

    # xCaracAd e xCaracSer (caracteristicas adicionais)
    x_carac_ad  = _txt(root, "cte:xCaracAd")  or ""
    x_carac_ser = _txt(root, "cte:xCaracSer") or ""
    x_obs       = _txt(root, "cte:xObs")      or ""

    # Tipo de veiculo -- lido do xObs COMPLETO (antes de cortar em 100 chars)
    veiculo, restr_veic = extrair_veiculo(x_obs)

    # v9: tipo do CT-e (0 normal, 1 complementar, 2 anulacao, 3 substituto)
    tp_cte = (ide.findtext("cte:tpCTe", namespaces=NS) or "").strip() if ide is not None else ""

    # v9: tomador do servico (quem paga o frete)
    #   toma3/toma: 0=remetente 1=expedidor 2=recebedor 3=destinatario
    #   toma4: 4=outros (com CNPJ proprio)
    _TOMA = {"0": "REM", "1": "EXP", "2": "RECEB", "3": "DEST", "4": "OUTROS"}
    tomador = tomador_cnpj = ""
    if ide is not None:
        t3 = ide.find("cte:toma3", NS)
        if t3 is None:
            t3 = ide.find("cte:toma", NS)  # leiaute 4.00 (CT-e OS / simplificado)
        t4 = ide.find("cte:toma4", NS)
        if t4 is not None:
            tomador = _TOMA.get((t4.findtext("cte:toma", namespaces=NS) or "4").strip(), "OUTROS")
            tomador_cnpj = (t4.findtext("cte:CNPJ", namespaces=NS) or "").strip()
        elif t3 is not None:
            tomador = _TOMA.get((t3.findtext("cte:toma", namespaces=NS) or "").strip(), "")
    if not tomador_cnpj:
        _papel = {"REM": "cte:rem", "EXP": "cte:exped", "RECEB": "cte:receb", "DEST": "cte:dest"}.get(tomador)
        if _papel:
            _el = root.find(".//" + _papel, NS)
            if _el is not None:
                tomador_cnpj = (_el.findtext("cte:CNPJ", namespaces=NS) or "").strip()
    tomador_gi = 1 if (tomador_cnpj.startswith(CNPJ_GENOMMA[:8]) or tomador_cnpj in CNPJS_INOVALAB) else (0 if tomador_cnpj else None)

    # Sub-operacao: detecta REENTREGA ou COMPLEMENTAR em xCaracAd ou xCaracSer
    _carac_all = (x_carac_ad + " " + x_carac_ser).upper()
    if "REENTREGA"    in _carac_all: sub_operacao = "REENTREGA"
    elif "COMPLEMENTAR" in _carac_all or tp_cte == "1": sub_operacao = "COMPLEMENTAR"
    else: sub_operacao = ""

    # Protocolo e status descritivo
    prot = root.find(".//cte:infProt", NS)
    n_prot = status_desc = ""
    if prot is not None:
        n_prot      = prot.findtext("cte:nProt",   namespaces=NS) or ""
        status_desc = prot.findtext("cte:xMotivo", namespaces=NS) or ""

    # Notas Fiscais referenciadas (extrai numero da chave NF-e posicao 25:34)
    nfs = []
    for nfe in root.findall(".//cte:infNFe", NS):
        ch = nfe.findtext("cte:chave", namespaces=NS) or ""
        if len(ch) == 44:
            nf_num = str(int(ch[25:34]))  # remove zeros a esquerda
            nfs.append(nf_num)
    # NF modelo 1/1A
    for nf in root.findall(".//cte:infNF", NS):
        num = nf.findtext("cte:nNF", namespaces=NS) or ""
        if num: nfs.append(num)
    nfs_str = "; ".join(nfs[:10])  # max 10 NFs por CTe

    return {
        "chave":        chave,
        "n_cte":        n_cte,
        "serie":        serie_cte,
        "cfop":         cfop,
        "n_prot":       n_prot,
        "status_desc":  status_desc[:60],
        "sub_op":       sub_operacao,
        "tp_cte":       tp_cte,
        "tomador":      tomador,
        "tomador_cnpj": tomador_cnpj,
        "tomador_gi":   tomador_gi,
        "x_carac_ad":   x_carac_ad[:40],
        "x_carac_ser":  x_carac_ser[:40],
        "x_obs":        x_obs[:100],
        "veiculo":      veiculo,
        "restr_veic":   restr_veic,
        "nfs":          nfs_str,
        "ano":          dt.year,
        "mes":          dt.month,
        "mes_nome":     MNOME[dt.month],
        "dt_emissao":   dt.strftime("%Y-%m-%d"),
        "empresa":      empresa,
        "operacao":     operacao,
        "rem_nome":     rem_nome[:60],
        "rem_cnpj":     rem_cnpj,
        "transp_nome":  transp_nome,
        "transp_cnpj":  transp_cnpj,
        "cliente":      cli_nome,
        "cli_cnpj":     cli_cnpj,
        "uf_dest":      uf_dest,
        "mun_dest":     mun_dest,
        "regiao":       regiao,
        "tipo_op":      tipo_op,
        "v_frete":      round(v_frete, 2),
        "v_merc":       round(v_merc,  2),
        "peso":         round(peso, 3),
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

# ── DE-PARA CNPJ -> MATRIZ (agrupa clientes por matriz) ─────────────
# Carrega cnpj_para_matriz.json (mesma pasta do script). Se faltar,
# o agrupamento cai de volta para CNPJ (comportamento antigo).
_MATRIZ_MAP = None
def _digits(s):
    return "".join(ch for ch in (s or "") if ch.isdigit())
def _load_matriz_map():
    from pathlib import Path
    p = Path(__file__).parent / "cnpj_para_matriz.json"
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        m = {_digits(k): v for k, v in raw.items() if len(_digits(k)) == 14}
        log.info(f"  de-para matriz: {len(m):,} CNPJs carregados")
        return m
    except Exception as ex:
        log.warning(f"  de-para matriz NAO carregado ({ex}) - clientes por CNPJ")
        return {}
def _grupo_cliente(cli_cnpj, cli_nome):
    """(chave_grupo, matriz, abrev, nome) para agrupar cliente por matriz."""
    global _MATRIZ_MAP
    if _MATRIZ_MAP is None:
        _MATRIZ_MAP = _load_matriz_map()
    info = _MATRIZ_MAP.get(_digits(cli_cnpj))
    if info:
        mz = (info.get("matriz") or info.get("nome") or cli_nome or "").strip()
        return ("MZ:" + mz, mz, (info.get("abrev") or "").strip(),
                (info.get("nome") or cli_nome or "").strip())
    return (cli_cnpj or cli_nome or "?", "", "", cli_nome or "")

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
        _clgrp, _mz, _cabrev, _cnome = _grupo_cliente(c.get("cli_cnpj"), c.get("cliente"))
        cl = _clgrp
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

        ano_k = str(c["ano"])
        mes_k = f"{c['ano']}-{c['mes']:02d}"

        kg = c.get("peso", 0) or 0
        transp_nome  = (c.get("transp_nome","") or "").upper()
        transp_cnpj  = (c.get("transp_cnpj","") or "").strip()
        transp_raiz  = transp_cnpj[:8] if len(transp_cnpj) >= 8 else ""

        # Pools para a aba Custo:
        # R$/kg: transportadoras com peso confiavel (ATIVA + FL Brasil) & peso > 0
        # %merc: demais transportadoras                                 & v_merc > 0
        #
        # Identificacao por CNPJ raiz (8 primeiros digitos = matriz),
        # cobre todas as filiais sem precisar listar cada uma.
        # Fallback por nome cobre CTes legados sem CNPJ.
        ATIVA_FL_RAIZES = {
            "01125797",  # ATIVA DISTRIBUICAO E LOGISTICA (matriz + 17 filiais)
            "18233211",  # FL BRASIL HOLDING LOGISTICA E TRANSPORTE (matriz + 18 filiais)
        }
        eh_ativa_fl = (
            (transp_raiz in ATIVA_FL_RAIZES) or
            ("ATIVA DISTR" in transp_nome) or
            ("FL BRASIL HOLDING" in transp_nome)
        )
        in_rkg_pool = eh_ativa_fl and kg > 0
        in_pct_pool = (not eh_ativa_fl) and m > 0

        def _inc(d, f, m, e, kg=0):
            d["f"]+=f; d["m"]+=m; d["n"]+=1
            if "kg" in d: d["kg"]+=kg
            if e=="GENOMMA":  d["gf"]+=f; d["gm"]+=m
            if e=="INOVALAB": d["inf"]+=f; d["inm"]+=m
            # Pool R$/kg (ATIVA + FL Brasil, peso > 0) — só atualiza se o dict aceita campos custo
            if in_rkg_pool and "rkf" in d:
                d["rkf"]+=f; d["rkkg"]+=kg; d["rkn"]+=1
                if e=="GENOMMA":  d["rkgf"]+=f; d["rkkgg"]+=kg; d["rkgn"]+=1
                if e=="INOVALAB": d["rkinf"]+=f; d["rkkgi"]+=kg; d["rkinn"]+=1
            # Pool %merc (demais transportadoras, v_merc > 0)
            if in_pct_pool and "pf" in d:
                d["pf"]+=f; d["pm"]+=m; d["pn"]+=1
                if e=="GENOMMA":  d["pgf"]+=f; d["pgm"]+=m; d["pgn"]+=1
                if e=="INOVALAB": d["pinf"]+=f; d["pinm"]+=m; d["pinn"]+=1

        def _get_or_create(dic, key, proto):
            if key not in dic: dic[key] = dict(proto)
            return dic[key]

        _COST = {"rkf":0,"rkkg":0,"rkn":0,"rkgf":0,"rkkgg":0,"rkgn":0,"rkinf":0,"rkkgi":0,"rkinn":0,
                 "pf":0,"pm":0,"pn":0,"pgf":0,"pgm":0,"pgn":0,"pinf":0,"pinm":0,"pinn":0}
        _GI       = {"f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0, **_COST}
        _GI_KG    = {"f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0,"kg":0, **_COST}

        # transportadora
        if tr not in by_transp:
            by_transp[tr] = {"nome":tr,"cnpj":c.get("transp_cnpj",""),
                             "f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0,
                             "kg":0,
                             "ree_f":0,"ree_n":0,"com_f":0,"com_n":0,
                             "regs":{},"anos":{},"meses":{}}
        _inc(by_transp[tr], f, m, e)
        _inc(_get_or_create(by_transp[tr]["anos"], ano_k, _GI_KG), f, m, e, kg)   # v9: com peso
        _inc(_get_or_create(by_transp[tr]["meses"], mes_k, _GI_KG), f, m, e, kg)
        # sub-operacao reentrega/complementar
        sop = c.get("sub_op","")
        if sop == "REENTREGA":    by_transp[tr]["ree_f"]+=f; by_transp[tr]["ree_n"]+=1
        elif sop == "COMPLEMENTAR": by_transp[tr]["com_f"]+=f; by_transp[tr]["com_n"]+=1
        # peso e custo por regiao
        by_transp[tr]["kg"] += c.get("peso",0)
        if rg not in by_transp[tr]["regs"]:
            by_transp[tr]["regs"][rg] = {"f":0,"kg":0,"n":0}
        by_transp[tr]["regs"][rg]["f"]  += f
        by_transp[tr]["regs"][rg]["kg"] += c.get("peso",0)
        by_transp[tr]["regs"][rg]["n"]  += 1

        # regiao
        if rg not in by_reg:
            by_reg[rg] = {"regiao":rg,"f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0,"kg":0,"anos":{},"meses":{},**_COST}
        _inc(by_reg[rg], f, m, e, kg)
        _inc(_get_or_create(by_reg[rg]["anos"],  ano_k, _GI_KG), f, m, e, kg)
        _inc(_get_or_create(by_reg[rg]["meses"], mes_k, _GI_KG), f, m, e, kg)

        # UF
        if uf:
            if uf not in by_uf:
                by_uf[uf] = {"uf":uf,"regiao":rg,"f":0,"m":0,"n":0,"gf":0,"gm":0,"inf":0,"inm":0,"kg":0,"anos":{},"meses":{},**_COST}
            _inc(by_uf[uf], f, m, e, kg)
            _inc(_get_or_create(by_uf[uf]["anos"],  ano_k, _GI_KG), f, m, e, kg)
            _inc(_get_or_create(by_uf[uf]["meses"], mes_k, _GI_KG), f, m, e, kg)

        # remetente
        _SR = {"f":0,"m":0,"n":0}
        if rk not in by_rem:
            by_rem[rk] = {"nome":c.get("rem_nome",""),"cnpj":c.get("rem_cnpj",""),
                          "f":0,"m":0,"n":0,"anos":{},"meses":{}}
        by_rem[rk]["f"]+=f; by_rem[rk]["m"]+=m; by_rem[rk]["n"]+=1
        rm_a=_get_or_create(by_rem[rk]["anos"],  ano_k, _SR); rm_a["f"]+=f; rm_a["m"]+=m; rm_a["n"]+=1
        rm_m=_get_or_create(by_rem[rk]["meses"], mes_k, _SR); rm_m["f"]+=f; rm_m["m"]+=m; rm_m["n"]+=1

        # cliente (agrupado por MATRIZ quando o CNPJ esta no de-para)
        if cl not in by_cli:
            by_cli[cl] = {"nome":(_mz or c.get("cliente","")),"cnpj":c.get("cli_cnpj",""),
                          "matriz":_mz,"abrev":_cabrev,
                          "empresa":e,"regiao":rg,"uf":uf,
                          "f":0,"m":0,"n":0,"anos":{},"meses":{},"filiais":{}}
        by_cli[cl]["f"]+=f; by_cli[cl]["m"]+=m; by_cli[cl]["n"]+=1
        cl_a=_get_or_create(by_cli[cl]["anos"],  ano_k, _SR); cl_a["f"]+=f; cl_a["m"]+=m; cl_a["n"]+=1
        cl_m=_get_or_create(by_cli[cl]["meses"], mes_k, _SR); cl_m["f"]+=f; cl_m["m"]+=m; cl_m["n"]+=1
        # filial (para drill-down matriz -> filial)
        _fk = c.get("cli_cnpj") or c.get("cliente") or "?"
        _fil = by_cli[cl]["filiais"].get(_fk)
        if _fil is None:
            _fil = {"cnpj":c.get("cli_cnpj",""),"nome":(_cnome or c.get("cliente","")),
                    "abrev":_cabrev,"regiao":rg,"uf":uf,"cidade":c.get("mun_dest",""),
                    "f":0,"m":0,"n":0}
            by_cli[cl]["filiais"][_fk] = _fil
        _fil["f"]+=f; _fil["m"]+=m; _fil["n"]+=1

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
    def _agg_anos(anos_raw):
        return {ano:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],
                     "pct_cts":_p(d["f"],d["m"]),
                     "peso":_r(d.get("kg",0)),
                     "genomma_frete":_r(d["gf"]),"inovalab_frete":_r(d["inf"]),
                     "pct_genomma":_p(d["gf"],d["gm"]),
                     "pct_inovalab":_p(d["inf"],d["inm"]),
                     # pool R$/kg (ATIVA + FL Brasil)
                     "rk_frete":_r(d.get("rkf",0)),"rk_peso":_r(d.get("rkkg",0)),"rk_ctes":d.get("rkn",0),
                     "rk_g_frete":_r(d.get("rkgf",0)),"rk_g_peso":_r(d.get("rkkgg",0)),"rk_g_ctes":d.get("rkgn",0),
                     "rk_i_frete":_r(d.get("rkinf",0)),"rk_i_peso":_r(d.get("rkkgi",0)),"rk_i_ctes":d.get("rkinn",0),
                     # pool % mercadoria (demais transportadoras)
                     "pc_frete":_r(d.get("pf",0)),"pc_merc":_r(d.get("pm",0)),"pc_ctes":d.get("pn",0),
                     "pc_g_frete":_r(d.get("pgf",0)),"pc_g_merc":_r(d.get("pgm",0)),"pc_g_ctes":d.get("pgn",0),
                     "pc_i_frete":_r(d.get("pinf",0)),"pc_i_merc":_r(d.get("pinm",0)),"pc_i_ctes":d.get("pinn",0)}
                for ano,d in anos_raw.items()}
    transp_out = sorted([{
        "nome":v["nome"],"cnpj":v["cnpj"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
        "genomma_frete":_r(v["gf"]),"inovalab_frete":_r(v["inf"]),
        "reentrega_frete":_r(v["ree_f"]),"reentrega_ctes":v["ree_n"],
        "complementar_frete":_r(v["com_f"]),"complementar_ctes":v["com_n"],
        "peso_total":_r(v["kg"]),
        "regioes":{rg:{"frete":_r(d["f"]),"peso":_r(d["kg"]),"ctes":d["n"],
                       "custo_kg":_r(d["f"]/d["kg"]) if d["kg"]>0 else None}
                   for rg,d in v["regs"].items()},
        "anos":_agg_anos(v.get("anos",{})),
        "meses":_agg_anos(v.get("meses",{})),
    } for v in by_transp.values()], key=lambda x:x["frete"], reverse=True)

    # regioes
    def _cost_fields(v):
        return {
            "rk_frete":_r(v.get("rkf",0)),"rk_peso":_r(v.get("rkkg",0)),"rk_ctes":v.get("rkn",0),
            "rk_g_frete":_r(v.get("rkgf",0)),"rk_g_peso":_r(v.get("rkkgg",0)),"rk_g_ctes":v.get("rkgn",0),
            "rk_i_frete":_r(v.get("rkinf",0)),"rk_i_peso":_r(v.get("rkkgi",0)),"rk_i_ctes":v.get("rkinn",0),
            "pc_frete":_r(v.get("pf",0)),"pc_merc":_r(v.get("pm",0)),"pc_ctes":v.get("pn",0),
            "pc_g_frete":_r(v.get("pgf",0)),"pc_g_merc":_r(v.get("pgm",0)),"pc_g_ctes":v.get("pgn",0),
            "pc_i_frete":_r(v.get("pinf",0)),"pc_i_merc":_r(v.get("pinm",0)),"pc_i_ctes":v.get("pinn",0),
        }
    regs_out = sorted([{
        "regiao":v["regiao"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "peso":_r(v.get("kg",0)),
        "genomma_frete":_r(v["gf"]),"genomma_merc":_r(v["gm"]),
        "inovalab_frete":_r(v["inf"]),"inovalab_merc":_r(v["inm"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
        **_cost_fields(v),
        "anos":_agg_anos(v.get("anos",{})),
        "meses":_agg_anos(v.get("meses",{})),
    } for v in by_reg.values()], key=lambda x:x["frete"], reverse=True)

    # UFs
    ufs_out = sorted([{
        "uf":v["uf"],"regiao":v["regiao"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "peso":_r(v.get("kg",0)),
        "genomma_frete":_r(v["gf"]),"genomma_merc":_r(v["gm"]),
        "inovalab_frete":_r(v["inf"]),"inovalab_merc":_r(v["inm"]),
        "pct_genomma":_p(v["gf"],v["gm"]),
        "pct_inovalab":_p(v["inf"],v["inm"]),
        **_cost_fields(v),
        "anos":_agg_anos(v.get("anos",{})),
        "meses":_agg_anos(v.get("meses",{})),
    } for v in by_uf.values()], key=lambda x:x["frete"], reverse=True)

    # remetentes top 100
    rems_out = sorted([{
        "nome":v["nome"],"cnpj":v["cnpj"],
        "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
        "pct_cts":_p(v["f"],v["m"]),
        "anos":{ano:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],"pct_cts":_p(d["f"],d["m"])} for ano,d in v.get("anos",{}).items()},
        "meses":{mes:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],"pct_cts":_p(d["f"],d["m"])} for mes,d in v.get("meses",{}).items()},
    } for v in by_rem.values()], key=lambda x:x["frete"], reverse=True)[:100]

    # clientes agrupados por MATRIZ (top 200 por v_merc)
    def _cli_row(v):
        fil = sorted([{
            "cnpj":ff["cnpj"],"nome":ff["nome"],"abrev":ff.get("abrev",""),
            "regiao":ff.get("regiao",""),"uf":ff.get("uf",""),"cidade":ff.get("cidade",""),
            "frete":_r(ff["f"]),"v_merc":_r(ff["m"]),"ctes":ff["n"],
            "pct_cts":_p(ff["f"],ff["m"]),
        } for ff in v.get("filiais",{}).values()], key=lambda x:x["v_merc"], reverse=True)
        cnpj_disp = fil[0]["cnpj"] if len(fil)==1 else ""
        return {
            "nome":v["nome"],"matriz":v.get("matriz",""),"abrev":v.get("abrev",""),
            "cnpj":cnpj_disp,"n_filiais":len(fil),"empresa":v["empresa"],
            "regiao":v["regiao"],"uf":v["uf"],
            "frete":_r(v["f"]),"v_merc":_r(v["m"]),"ctes":v["n"],
            "pct_cts":_p(v["f"],v["m"]),
            "anos":{ano:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],"pct_cts":_p(d["f"],d["m"])} for ano,d in v.get("anos",{}).items()},
            "meses":{mes:{"frete":_r(d["f"]),"v_merc":_r(d["m"]),"ctes":d["n"],"pct_cts":_p(d["f"],d["m"])} for mes,d in v.get("meses",{}).items()},
            "filiais":fil,
        }
    clis_out = sorted([_cli_row(v) for v in by_cli.values()],
                      key=lambda x:x["v_merc"], reverse=True)[:200]

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
SEM_PUBLICAR = False  # v9: ligado por --sem-publicar


def _hdrs():
    chave = _carregar_chave()
    if not chave:
        log.error("  ERRO: chave do Supabase nao encontrada. Coloque SUPABASE_SERVICE_KEY=... "
                  "no arquivo .env (pasta torre-controle-logistica) ou defina a variavel de ambiente.")
        return None
    # Chave nova (sb_secret_...) vai SO no header apikey; chave legada (JWT eyJ...) vai nos dois.
    h = {"apikey": chave, "Content-Type": "application/json",
         "x-upsert": "true", "cache-control": "no-cache"}
    if chave.startswith("eyJ"):
        h["Authorization"] = f"Bearer {chave}"
    return h


def publicar(payload: dict) -> bool:
    if SEM_PUBLICAR:
        log.info("  (--sem-publicar) dados.json NAO enviado"); return True
    hdrs = _hdrs()
    if hdrs is None:
        return False
    data = json.dumps(payload, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    url  = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{PATH_JSON}"
    try:
        r = requests.post(url, data=data, headers=hdrs, timeout=60)
        if r.status_code in (200,201,204):
            log.info("  OK Publicado no Supabase"); return True
        log.error(f"  ERRO Supabase {r.status_code}: {r.text[:300]}")
        if "JWS" in r.text or "JWT" in r.text or "nauthorized" in r.text:
            log.error("  -> A chave do Supabase foi recusada. Cole a chave secreta NOVA (sb_secret_...) "
                      "em SUPABASE_SERVICE_KEY no arquivo .env e rode REPUBLICAR.bat.")
        return False
    except Exception as e:
        log.error(f"  ERRO conexao: {e}"); return False


# ───────────────────────────────────────────────────────────────
#  BUSCA DE ARQUIVOS (rapida via scandir)
# ───────────────────────────────────────────────────────────────
def _mz_raw(c):
    """v9: matriz/abreviacao do cliente no registro bruto (so quando ha de-para)."""
    grp, mz, ab, _ = _grupo_cliente(c.get("cli_cnpj"), c.get("cliente"))
    out = {}
    if mz: out["mz"] = mz[:50]
    if ab: out["ab"] = ab[:30]
    # v9.1: matriz do CLIENTE da operacao. Na Reversa o cliente e o REMETENTE
    # (quem devolve); no Outbound/Inbound e o destinatario.
    if c.get("operacao") == "REVERSA":
        _, mzc, abc, _ = _grupo_cliente(c.get("rem_cnpj"), c.get("rem_nome"))
    else:
        mzc, abc = mz, ab
    if mzc: out["mzc"] = mzc[:50]
    if abc: out["abc"] = abc[:30]
    return out


def publicar_raw(ctes: list) -> bool:
    """Publica dados brutos para exportacao Excel no dashboard."""
    EMP = {"GENOMMA":"G","INOVALAB":"I","OUTROS":"O"}
    OP  = {"OUTBOUND":"OUT","INBOUND":"IN","REVERSA":"REV"}
    raw = [{"k":c["chave"],
             "nct":c.get("n_cte",""),"ser":c.get("serie",""),
             "cf":c.get("cfop",""),"np":c.get("n_prot",""),
             "st":c.get("status_desc",""),
             "sop":c.get("sub_op",""),
             "xca":c.get("x_carac_ad",""),"xcs":c.get("x_carac_ser",""),
             "xob":c.get("x_obs",""),
             "vei":c.get("veiculo",""),"rve":c.get("restr_veic",""),
             "nfs":c.get("nfs",""),
             "d":c["dt_emissao"],
             "e":EMP.get(c["empresa"],"O"),"o":OP.get(c.get("operacao","OUTBOUND"),"OUT"),
             "rn":c.get("rem_nome","")[:50],"rc":c.get("rem_cnpj",""),
             "tn":c.get("transp_nome","")[:50],"tc":c.get("transp_cnpj",""),
             "cn":c.get("cliente","")[:50],"cc":c.get("cli_cnpj",""),
             "uf":c.get("uf_dest",""),"mu":c.get("mun_dest","")[:30],
             "tp":c.get("tipo_op","CIF"),
             "vf":round(c.get("v_frete",0),2),
             "vm":round(c.get("v_merc",0),2),
             "kg":round(c.get("peso",0),3),
             "ano":c.get("ano",0),"mes":c.get("mes",0),
             # v9
             "rg":c.get("regiao",""),
             "tpc":c.get("tp_cte",""),"tom":c.get("tomador",""),"tgi":c.get("tomador_gi"),
             **_mz_raw(c)} for c in ctes]
    data = json.dumps(raw, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    if SEM_PUBLICAR:
        with open("ctes_raw_local.json", "wb") as _f:
            _f.write(data)
        log.info(f"  (--sem-publicar) ctes_raw.json NAO enviado; copia em ctes_raw_local.json ({len(data)//1024}KB)")
        return True
    hdrs = _hdrs()
    if hdrs is None:
        return False
    url  = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{PATH_RAW}"
    try:
        r = requests.post(url, data=data, headers=hdrs, timeout=90)
        if r.status_code in (200,201,204):
            log.info(f"  OK ctes_raw.json ({len(data)//1024}KB)"); return True
        log.error(f"  ERRO raw {r.status_code}: {r.text[:200]}"); return False
    except Exception as e:
        log.error(f"  ERRO raw: {e}"); return False


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
def _pausa():
    try:
        input("  Pressione Enter...")
    except EOFError:
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tudo",          action="store_true",
                        help="Reprocessa todos os XMLs do zero (~15 min)")
    parser.add_argument("--reclassificar", action="store_true",
                        help="Reclassifica historico sem reler XMLs (<1 min)")
    parser.add_argument("--pasta",         default=PASTA_CTE)
    parser.add_argument("--sem-publicar",  action="store_true",
                        help="v9: processa e grava local, sem enviar ao Supabase")
    args = parser.parse_args()
    global SEM_PUBLICAR
    SEM_PUBLICAR = args.sem_publicar
    pasta = args.pasta
    ts_inicio = datetime.now()

    # ── MODO RECLASSIFICAR (rapido) ─────────────────────────────
    if args.reclassificar:
        print("\n" + "="*58)
        print("  TORRE DE CONTROLE -- RECLASSIFICACAO RAPIDA  v9")
        print("  Reclassifica historico sem reler XMLs")
        print("="*58)
        estado = load_estado()
        ctes = estado.get("ctes", [])
        if not ctes:
            print("\n  Historico vazio. Rode REPROCESSAR_TUDO primeiro.")
            _pausa(); sys.exit(0)

        log.info(f"Reclassificando {len(ctes):,} CTes...")
        alterados = descartados = excluidos_transp = 0
        ctes_validos = []
        for c in ctes:
            # Descarta CTes de transportadoras excluidas
            if transp_excluida(c.get("transp_cnpj","")):
                excluidos_transp += 1
                continue
            op_nova = classificar_op(
                c.get("rem_cnpj",""), c.get("rem_nome",""),
                c.get("cli_cnpj",""), c.get("cliente","")
            )
            if op_nova is None:
                descartados += 1
                continue
            if not c.get("veiculo"):
                c["veiculo"], c["restr_veic"] = extrair_veiculo(c.get("x_obs",""))
            if c.get("operacao") != op_nova:
                c["operacao"] = op_nova
                alterados += 1
            ctes_validos.append(c)

        log.info(f"  {alterados:,} reclassificados | {descartados:,} descartados (rem vazio) | {excluidos_transp:,} descartados (transp excluida)")

        log.info("Agregando...")
        dados = agregar(ctes_validos)
        dados["atualizado"]           = ts_inicio.strftime("%d/%m/%Y %H:%M")
        dados["pasta_origem"]         = pasta
        dados["anos_filtro"]          = ANOS_FILTRO
        dados["novos_nesta_execucao"] = 0
        dados["cancelamentos"]        = 0
        dados["versao_pipeline"]      = "v9"

        with open("cte_dados.json","w",encoding="utf-8") as f:
            json.dump(dados,f,ensure_ascii=False,indent=2)

        estado["chaves"]          = [c["chave"] for c in ctes_validos]
        estado["ctes"]            = ctes_validos
        estado["ultima_execucao"] = ts_inicio.isoformat()
        estado["atualizado"]      = dados["atualizado"]
        save_estado(estado)

        ok = publicar(dados)
        publicar_raw(ctes_validos)
        r  = dados["resumo"]
        print("\n" + "="*58 + "\n  RESUMO\n" + "="*58)
        print(f"  CTes ativos          : {len(ctes_validos):,}")
        print(f"  Reclassificados      : {alterados:,}")
        print(f"  Descartados (rem vazio): {descartados:,}")
        print(f"  Outbound : {r['outbound_ctes']:,} CTes | R$ {r['outbound_frete']:,.2f}")
        print(f"  Inbound  : {r['inbound_ctes']:,} CTes | R$ {r['inbound_frete']:,.2f}")
        print(f"  Reversa  : {r['reversa_ctes']:,} CTes | R$ {r['reversa_frete']:,.2f}")
        print("="*58)
        print(f"  {'(teste --sem-publicar: nada foi enviado)' if SEM_PUBLICAR else ('OK Dashboard atualizado!' if ok else 'ERRO Publicacao falhou')}")
        _pausa(); sys.exit(0)
    # ────────────────────────────────────────────────────────────

    print("\n" + "="*58)
    print("  TORRE DE CONTROLE -- PROCESSADOR DE CTe  v9")
    print(f"  Pasta  : {pasta}")
    print(f"  Filtro : {PREFIXO_CTE}* (CTe) e {PREFIXO_CANCELAMENTO}* (Cancelamento)")
    print(f"  Modo   : {'COMPLETO (--tudo)' if args.tudo else 'INCREMENTAL (so novos)'}")
    print("="*58)

    if not os.path.exists(pasta):
        log.error(f"Pasta nao encontrada: {pasta}")
        _pausa(); sys.exit(1)

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
    # v9: conferencia de carga
    conf = {"execucao": ts_inicio.isoformat(), "modo": "tudo" if args.tudo else "incremental",
            "xml_cte_encontrados": len(xmls_214), "xml_cte_lidos": len(cands_214),
            "aceitos": 0, "duplicados": 0, "cancelados_ignorados": 0,
            "rejeitados": {}, "rejeitados_por_ano": {}, "aceitos_por_ano": {}}
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
                    conf["rejeitados"]["sem_retorno"] = conf["rejeitados"].get("sem_retorno", 0) + 1
                    continue
                if "_rej" in res:
                    mot = res["_rej"]; a = str(res.get("ano") or "?")
                    conf["rejeitados"][mot] = conf["rejeitados"].get(mot, 0) + 1
                    rpa = conf["rejeitados_por_ano"].setdefault(a, {})
                    rpa[mot] = rpa.get(mot, 0) + 1
                    continue
                if res["chave"] in chaves_cancel:
                    conf["cancelados_ignorados"] += 1
                    continue
                if res["chave"] in chaves_ok:
                    conf["duplicados"] += 1
                    continue
                novos_ctes.append(res)
                chaves_ok.add(res["chave"])
                conf["aceitos"] += 1
                a = str(res["ano"]); conf["aceitos_por_ano"][a] = conf["aceitos_por_ano"].get(a, 0) + 1

    log.info(f"  {len(novos_ctes):,} CTes novos em {(datetime.now()-ts_p).seconds}s")
    _rej_tot = sum(conf["rejeitados"].values())
    log.info(f"  CONFERENCIA: lidos {len(cands_214):,} = aceitos {conf['aceitos']:,} + duplicados "
             f"{conf['duplicados']:,} + cancelados {conf['cancelados_ignorados']:,} + rejeitados {_rej_tot:,}")
    for mot, q in sorted(conf["rejeitados"].items(), key=lambda x: -x[1]):
        log.info(f"    rejeitado {mot:<26}: {q:,}")
    if conf["rejeitados_por_ano"].get("2024") or conf["aceitos_por_ano"].get("2024"):
        log.info(f"    2024 -> aceitos {conf['aceitos_por_ano'].get('2024',0):,} | rejeitados {conf['rejeitados_por_ano'].get('2024',{})}")
    try:
        with open("cte_conferencia.json", "w", encoding="utf-8") as _f:
            json.dump(conf, _f, ensure_ascii=False, indent=2)
    except OSError:
        pass

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
        if not c.get("veiculo"):
            c["veiculo"], c["restr_veic"] = extrair_veiculo(c.get("x_obs",""))
        op = classificar_op(c.get("rem_cnpj",""), c.get("rem_nome",""),
                            c.get("cli_cnpj",""), c.get("cliente",""))
        c["operacao"] = op or "OUTBOUND"

    # descarta CTes com remetente vazio ou de transportadora excluida do historico
    hist_valido = [c for c in hist
                   if not transp_excluida(c.get("transp_cnpj",""))
                   and classificar_op(
                       c.get("rem_cnpj",""), c.get("rem_nome",""),
                       c.get("cli_cnpj",""), c.get("cliente","")) is not None]

    if not novos_ctes and not novos_cancel and not args.tudo:
        estado["ultima_execucao"] = ts_inicio.isoformat()
        save_estado(estado)
        print("\n  Nenhum CTe novo. Dashboard ja atualizado!")
        _pausa(); sys.exit(0)

    todos = hist_valido + novos_ctes
    log.info(f"Agregando {len(todos):,} CTes...")
    dados = agregar(todos)
    dados["atualizado"]           = ts_inicio.strftime("%d/%m/%Y %H:%M")
    dados["pasta_origem"]         = pasta
    dados["anos_filtro"]          = ANOS_FILTRO
    dados["novos_nesta_execucao"] = len(novos_ctes)
    dados["cancelamentos"]        = novos_cancel
    dados["conferencia"]          = {k: conf[k] for k in ("execucao","modo","xml_cte_lidos","aceitos",
                                     "duplicados","cancelados_ignorados","rejeitados")}
    dados["removidos_por_cancelamento"] = rem_hist
    dados["versao_pipeline"]      = "v9"

    with open("cte_dados.json","w",encoding="utf-8") as f:
        json.dump(dados,f,ensure_ascii=False,indent=2)
    log.info("  OK cte_dados.json salvo")

    estado["chaves"]          = list(chaves_ok)
    estado["cancelados"]      = list(chaves_cancel)
    estado["ctes"]            = todos
    estado["ultima_execucao"] = ts_inicio.isoformat()
    save_estado(estado)

    ok = publicar(dados)
    publicar_raw(todos)

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
    print(f"\n  {'(teste --sem-publicar: nada foi enviado)' if SEM_PUBLICAR else ('OK Dashboard atualizado!' if ok else 'ERRO Publicacao falhou -- rode REPUBLICAR.bat')}")
    print("  https://tpinheiro1986.github.io/torre-controle-logistica/custo-servir/\n")
    _pausa()

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
