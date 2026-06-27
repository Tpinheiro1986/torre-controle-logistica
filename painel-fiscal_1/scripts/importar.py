#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROBO DE IMPORTACAO — Torre de Controle / Modulo Fiscal
Le NF-e, CT-e e Romaneios das pastas de rede e envia para o Supabase.

Rode no PC/servidor que tem o drive Y: mapeado.
Pode rodar quantas vezes quiser: documentos repetidos sao atualizados, nao duplicados.
"""

import os, sys, glob
import xml.etree.ElementTree as ET
from datetime import datetime

# ============================================================
#  CONFIGURACAO  — edite as linhas abaixo no Bloco de Notas
# ============================================================
SUPABASE_URL  = "https://ennsbpibfnuwlvtodukg.supabase.co"
SUPABASE_ANON = "sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONyI5"
LOGIN_EMAIL   = "SEU_EMAIL_AQUI"      # o mesmo login criado no painel
LOGIN_SENHA   = "SUA_SENHA_AQUI"

PASTA_NFE       = r"Y:\ERP-12\ArqXML-MG"
PASTA_CTE       = r"Y:\ERP-12\TOTVSCOLAB20-PRD\RECEIVED"
PASTA_ROMANEIO  = r"Y:\ERP-12\TRANSP-PRD\ROMANEIO\Enviados"   # tem subpastas
PASTA_MANIFESTO = r"Y:\ERP-12\ArqXML-MG"   # onde ficam os arquivos MANIFE*.txt (confirmar)

ANO_MINIMO = 2026   # importa apenas documentos de 2026 em diante
# ============================================================

try:
    from supabase import create_client
except ImportError:
    print("Falta a biblioteca supabase. Rode:  pip install supabase")
    sys.exit(1)

def g(el, tag):
    """Busca um campo pelo nome, ignorando o namespace do XML."""
    if el is None:
        return None
    e = el.find(f".//{{*}}{tag}")
    return e.text if e is not None else None

def gd(el, tag):
    """Busca direta (1 nivel) ignorando namespace."""
    if el is None:
        return None
    for ch in el:
        if ch.tag.split("}")[-1] == tag:
            return ch
    return None

def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def ano_do_doc(dh, caminho):
    """Ano pela data de emissao do documento; se nao houver, pela data do arquivo."""
    if dh and len(dh) >= 4 and dh[:4].isdigit():
        return int(dh[:4])
    return datetime.fromtimestamp(os.path.getmtime(caminho)).year

# ----------------------------------------------------------------
def parse_nfe(path):
    root = ET.parse(path).getroot()
    inf = root.find(".//{*}infNFe")
    if inf is None:
        return None, None
    ide  = gd(inf, "ide"); emit = gd(inf, "emit"); dest = gd(inf, "dest")
    ee   = gd(emit, "enderEmit"); ed = gd(dest, "enderDest")
    total= root.find(".//{*}ICMSTot")
    prot = root.find(".//{*}infProt")
    adic = gd(inf, "infAdic")
    nota = {
        "chave": (inf.get("Id") or "").replace("NFe", ""),
        "numero": g(ide,"nNF"), "serie": g(ide,"serie"), "modelo": g(ide,"mod"),
        "natureza_operacao": g(ide,"natOp"),
        "tipo_operacao": "Saida" if g(ide,"tpNF")=="1" else "Entrada",
        "finalidade": g(ide,"finNFe"), "data_emissao": g(ide,"dhEmi"),
        "uf_emitente": g(ee,"UF"), "cnpj_emitente": g(emit,"CNPJ"),
        "nome_emitente": g(emit,"xNome"), "ie_emitente": g(emit,"IE"),
        "municipio_emitente": g(ee,"xMun"),
        "cnpj_destinatario": g(dest,"CNPJ"), "nome_destinatario": g(dest,"xNome"),
        "uf_destinatario": g(ed,"UF"), "municipio_destinatario": g(ed,"xMun"),
        "valor_produtos": num(g(total,"vProd")), "valor_total": num(g(total,"vNF")),
        "valor_icms": num(g(total,"vICMS")), "valor_frete": num(g(total,"vFrete")),
        "valor_desconto": num(g(total,"vDesc")),
        "protocolo": g(prot,"nProt"), "status_codigo": g(prot,"cStat"),
        "status_motivo": g(prot,"xMotivo"), "data_autorizacao": g(prot,"dhRecbto"),
        "info_complementar": g(adic,"infCpl"), "arquivo_origem": os.path.basename(path),
    }
    itens = []
    for det in inf.findall("{*}det"):
        prod = gd(det,"prod"); icms_wrap = gd(det,"ICMS")
        icms = list(icms_wrap)[0] if icms_wrap is not None and len(icms_wrap) else None
        itens.append({
            "num_item": int(det.get("nItem")), "codigo_produto": g(prod,"cProd"),
            "ean": g(prod,"cEAN"), "descricao": g(prod,"xProd"), "ncm": g(prod,"NCM"),
            "cfop": g(prod,"CFOP"), "unidade": g(prod,"uCom"),
            "quantidade": num(g(prod,"qCom")), "valor_unitario": num(g(prod,"vUnCom")),
            "valor_total": num(g(prod,"vProd")), "cst_icms": g(icms,"CST"),
            "aliquota_icms": num(g(icms,"pICMS")), "valor_icms_item": num(g(icms,"vICMS")),
        })
    return nota, itens

# ----------------------------------------------------------------
def parse_cte(path):
    """Leitor de CT-e padrao 4.00 — VALIDAR contra um arquivo real."""
    root = ET.parse(path).getroot()
    inf = root.find(".//{*}infCte")
    if inf is None:
        return None
    ide  = gd(inf,"ide")
    emit = gd(inf,"emit"); rem = gd(inf,"rem"); dest = gd(inf,"dest")
    vprest = gd(inf,"vPrest")
    carga  = root.find(".//{*}infCarga")
    prot   = root.find(".//{*}infProt")
    nfes   = inf.findall(".//{*}infNFe") + inf.findall(".//{*}infNF") + inf.findall(".//{*}infOutros")
    refs = []
    for nf in inf.findall(".//{*}infNFe"):
        ch = g(nf, "chave")
        if ch:
            refs.append({"chave_nfe": ch, "numero_nf": str(int(ch[25:34])) if len(ch) == 44 else None})
    cte = {
        "chave": (inf.get("Id") or "").replace("CTe",""),
        "numero": g(ide,"nCT"), "serie": g(ide,"serie"), "modelo": g(ide,"mod"),
        "tipo_cte": g(ide,"tpCTe"), "natureza_operacao": g(ide,"natOp"), "cfop": g(ide,"CFOP"),
        "data_emissao": g(ide,"dhEmi"), "uf_inicio": g(ide,"UFIni"), "uf_fim": g(ide,"UFFim"),
        "cnpj_emitente": g(emit,"CNPJ"), "nome_emitente": g(emit,"xNome"),
        "cnpj_remetente": g(rem,"CNPJ") or g(rem,"CPF"), "nome_remetente": g(rem,"xNome"),
        "cnpj_destinatario": g(dest,"CNPJ") or g(dest,"CPF"), "nome_destinatario": g(dest,"xNome"),
        "valor_total": num(g(vprest,"vTPrest")), "valor_receber": num(g(vprest,"vRec")),
        "valor_carga": num(g(carga,"vCarga")), "qtd_nfe": len(nfes),
        "protocolo": g(prot,"nProt"), "status_codigo": g(prot,"cStat"),
        "status_motivo": g(prot,"xMotivo"), "data_autorizacao": g(prot,"dhRecbto"),
        "arquivo_origem": os.path.basename(path),
    }
    return cte, refs

# ----------------------------------------------------------------
def parse_manifesto(path):
    """Le manifestacao do destinatario. Suporta 2 formatos:
       - Antigo: linhas com espacos  (1 cab / 2 detalhe / 3 fim)
       - Novo (posicional, 60 col): 001 + nNF(9) + campo(4) + chave(44)
    """
    with open(path, encoding="latin-1") as f:
        linhas = [l.rstrip("\r\n") for l in f if l.strip()]
    nome = os.path.basename(path)
    regs = []

    # Detecta formato pelo cabecalho/primeira linha
    primeira = linhas[0] if linhas else ""
    posicional = len(primeira) == 60 and primeira[:1] == "0"

    if posicional:
        h = linhas[0]
        d = h[13:21]  # ddmmyyyy
        data_iso = f"{d[4:8]}-{d[2:4]}-{d[0:2]}" if len(d) == 8 else None
        cnpj_empresa = h[25:39]
        serie_arq = nome.replace(".txt", "").split("_")[-1]  # 50 / 200
        for l in linhas[1:]:
            if len(l) < 60 or not l.startswith("001"):
                continue
            regs.append({
                "numero_nf": str(int(l[3:12])),
                "campo_aux": l[12:16],
                "chave_nfe": l[16:60],
                "sequencia": l[3:12],
                "cnpj_empresa": cnpj_empresa,
                "serie": serie_arq,
                "data_arquivo": data_iso,
                "arquivo_origem": nome,
                "codigo_evento": "210200",
                "evento": "Confirmacao da Operacao (saida)",
            })
    else:
        header = {}
        for l in linhas:
            c = l.split()
            if not c:
                continue
            if c[0] == "1":
                header = {"data": c[2] if len(c) > 2 else None,
                          "lote": c[3] if len(c) > 3 else None,
                          "serie": c[4] if len(c) > 4 else None}
            elif c[0] == "2" and len(c) >= 3:
                regs.append({"sequencia": c[1], "codigo_evento": c[2],
                             "evento": EVENTOS.get(c[2], f"Codigo {c[2]}"),
                             "lote": header.get("lote"), "serie": header.get("serie"),
                             "arquivo_origem": nome})
        d = header.get("data")
        data_iso = f"{d[4:8]}-{d[2:4]}-{d[0:2]}" if d and len(d) == 8 else None
        for r in regs:
            r["data_arquivo"] = data_iso
    return regs

EVENTOS = {"0": "Ciencia da Operacao", "1": "Confirmacao da Operacao",
           "2": "Desconhecimento / Op. nao realizada", "3": "Operacao nao Realizada"}

def importar_manifestacoes(sb):
    n_ok = n_pula = 0
    arquivos = glob.glob(os.path.join(PASTA_MANIFESTO, "**", "MANIFE*.txt"), recursive=True)
    print(f"\n[Manifesto] {len(arquivos)} arquivos MANIFE encontrados")
    for txt in arquivos:
        try:
            if datetime.fromtimestamp(os.path.getmtime(txt)).year < ANO_MINIMO:
                n_pula += 1; continue
            regs = parse_manifesto(txt)
            for r in regs:
                if r.get("chave_nfe"):
                    sb.table("nfe_manifestacoes").upsert(r, on_conflict="chave_nfe,arquivo_origem").execute()
                else:
                    sb.table("nfe_manifestacoes").insert(r).execute()
            n_ok += len(regs)
            print(f"  OK {os.path.basename(txt)} ({len(regs)} eventos)")
        except Exception as e:
            print(f"  ERRO {os.path.basename(txt)}: {e}")
    print(f"[Manifesto] eventos: {n_ok}")

def inventariar_romaneio(path):
    """Sem formato confirmado: registra o arquivo e guarda o conteudo bruto."""
    st = os.stat(path)
    raw = None
    if st.st_size < 200_000:  # so guarda conteudo de arquivos pequenos (txt/xml)
        try:
            with open(path, encoding="latin-1") as f:
                raw = f.read()
        except Exception:
            raw = None
    return {
        "identificador": os.path.splitext(os.path.basename(path))[0],
        "arquivo_origem": os.path.basename(path),
        "caminho": os.path.dirname(path),
        "tipo_arquivo": os.path.splitext(path)[1].lower().lstrip("."),
        "data_arquivo": datetime.fromtimestamp(st.st_mtime).isoformat(),
        "tamanho_bytes": st.st_size,
        "conteudo_raw": raw,
    }

# ----------------------------------------------------------------
def conectar():
    sb = create_client(SUPABASE_URL, SUPABASE_ANON)
    sb.auth.sign_in_with_password({"email": LOGIN_EMAIL, "password": LOGIN_SENHA})
    return sb

def importar_nfe(sb):
    n_ok = n_pula = 0
    arquivos = glob.glob(os.path.join(PASTA_NFE, "**", "*.xml"), recursive=True)
    print(f"\n[NF-e] {len(arquivos)} XML encontrados em {PASTA_NFE}")
    for xml in arquivos:
        try:
            nota, itens = parse_nfe(xml)
            if nota is None:  # nao e NF-e (pode ser CT-e nessa pasta)
                continue
            if ano_do_doc(nota["data_emissao"], xml) < ANO_MINIMO:
                n_pula += 1; continue
            res = sb.table("nfe_notas").upsert(nota, on_conflict="chave").execute()
            nid = res.data[0]["id"]
            sb.table("nfe_itens").delete().eq("nota_id", nid).execute()
            if itens:
                sb.table("nfe_itens").insert([{**it, "nota_id": nid} for it in itens]).execute()
            n_ok += 1
            print(f"  OK NF-e {nota['numero']}")
        except Exception as e:
            print(f"  ERRO {os.path.basename(xml)}: {e}")
    print(f"[NF-e] importadas: {n_ok} | fora de {ANO_MINIMO}+: {n_pula}")

def importar_cte(sb):
    n_ok = n_pula = 0
    arquivos = glob.glob(os.path.join(PASTA_CTE, "**", "*.xml"), recursive=True)
    print(f"\n[CT-e] {len(arquivos)} XML encontrados em {PASTA_CTE}")
    for xml in arquivos:
        try:
            cte, refs = parse_cte(xml)
            if cte is None:
                continue
            if ano_do_doc(cte["data_emissao"], xml) < ANO_MINIMO:
                n_pula += 1; continue
            res = sb.table("cte_conhecimentos").upsert(cte, on_conflict="chave").execute()
            cid = res.data[0]["id"]
            sb.table("cte_nfe_ref").delete().eq("cte_id", cid).execute()
            if refs:
                sb.table("cte_nfe_ref").upsert(
                    [{**r, "cte_id": cid} for r in refs], on_conflict="cte_id,chave_nfe").execute()
            n_ok += 1
            print(f"  OK CT-e {cte['numero']} ({len(refs)} NF-e vinculadas)")
        except Exception as e:
            print(f"  ERRO {os.path.basename(xml)}: {e}")
    print(f"[CT-e] importados: {n_ok} | fora de {ANO_MINIMO}+: {n_pula}")

def importar_romaneios(sb):
    n_ok = n_pula = 0
    padrao = os.path.join(PASTA_ROMANEIO, "**", "*.*")
    arquivos = [a for a in glob.glob(padrao, recursive=True) if os.path.isfile(a)]
    print(f"\n[Romaneio] {len(arquivos)} arquivos encontrados (com subpastas) em {PASTA_ROMANEIO}")
    for arq in arquivos:
        try:
            if datetime.fromtimestamp(os.path.getmtime(arq)).year < ANO_MINIMO:
                n_pula += 1; continue
            reg = inventariar_romaneio(arq)
            sb.table("romaneios").upsert(reg, on_conflict="caminho,arquivo_origem").execute()
            n_ok += 1
        except Exception as e:
            print(f"  ERRO {os.path.basename(arq)}: {e}")
    print(f"[Romaneio] registrados: {n_ok} | fora de {ANO_MINIMO}+: {n_pula}")

if __name__ == "__main__":
    if "SEU_EMAIL" in LOGIN_EMAIL:
        print("Configure LOGIN_EMAIL e LOGIN_SENHA no topo do arquivo antes de rodar.")
        sys.exit(1)
    sb = conectar()
    importar_nfe(sb)
    importar_cte(sb)
    importar_manifestacoes(sb)
    importar_romaneios(sb)
    print("\nConcluido. Abra o painel para ver os dados.")
