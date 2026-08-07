# -*- coding: utf-8 -*-
"""
ingestao_api.py — Torre de Controle ADM de Vendas
Ingestão da API LOG023 (Datasul REST /pedidovenda) para o Supabase (adm_vendas).

Modelo de carga:
  - A API só filtra por DT DE IMPLANTAÇÃO (data_ini/data_fim), sem paginação.
  - Estratégia: janelas MENSAIS. O backfill "vai comendo" pra trás, mês a mês,
    a partir do mês atual até MES_INICIO_HISTORICO. Cada janela concluída é
    registrada em adm_vendas.controle_janela_api.
  - O modo diário recarrega o mês atual + todas as janelas que ainda têm
    pedidos em situação aberta (qtd_abertos > 0), atualizando status/NF-e.
  - Upsert por chave natural (estabel, ped_cli, cod_item, nr_embarque).
  - Chaves que sumiram da origem no re-fetch da janela são marcadas ativo=false.

Uso:
  python ingestao_api.py --backfill        # processa a próxima janela pendente (pra trás)
  python ingestao_api.py --backfill --tudo # processa TODAS as janelas pendentes de uma vez
  python ingestao_api.py --diario          # mês atual + janelas com pedidos abertos
  python ingestao_api.py --janela 2026-05  # força reprocessamento de uma janela específica

Variáveis de ambiente obrigatórias (NUNCA commitar credenciais no repo!):
  LOG023_API_BASE   ex.: http://10.3.0.95:8083/datasul-teste/dts/datasul-rest/resources/prg/esp/v1
  LOG023_API_USER   usuário Datasul (ideal: usuário de serviço só-leitura)
  LOG023_API_PASS   senha Datasul
  SUPABASE_URL      ex.: https://ennsbpibfnuwlvtodukg.supabase.co
  SUPABASE_KEY      service_role key (as tabelas só aceitam escrita via service_role)

Opcional:
  LOG023_MES_INICIO  primeiro mês do histórico, formato YYYY-MM (default: 2025-01)
  LOG023_TIMEOUT     timeout da chamada em segundos (default: 600)
"""

import argparse
import json
import os
import sys
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta

import requests

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

API_BASE = os.environ.get("LOG023_API_BASE", "").rstrip("/")
API_USER = os.environ.get("LOG023_API_USER", "")
API_PASS = os.environ.get("LOG023_API_PASS", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
MES_INICIO = os.environ.get("LOG023_MES_INICIO", "2025-01")
TIMEOUT = int(os.environ.get("LOG023_TIMEOUT", "600"))

BATCH_SIZE = 250  # padrão já usado nas cargas do projeto

SITUACOES_ABERTAS = {
    "Aberto", "Atendido Parcial", "Pendente", "Suspenso", "Faturado Parcial",
}

REST = f"{SUPABASE_URL}/rest/v1"
HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    # Schema não-público: headers de profile obrigatórios (lição aprendida!)
    "Accept-Profile": "adm_vendas",
    "Content-Profile": "adm_vendas",
    "Content-Type": "application/json",
}


def falhar(msg):
    print(f"[ERRO] {msg}")
    sys.exit(1)


def checar_config():
    faltando = [n for n, v in [
        ("LOG023_API_BASE", API_BASE), ("LOG023_API_USER", API_USER),
        ("LOG023_API_PASS", API_PASS), ("SUPABASE_URL", SUPABASE_URL),
        ("SUPABASE_KEY", SUPABASE_KEY),
    ] if not v]
    if faltando:
        falhar("Variáveis de ambiente ausentes: " + ", ".join(faltando))


# ---------------------------------------------------------------------------
# Janelas mensais
# ---------------------------------------------------------------------------

def limites_mes(ano, mes):
    ini = date(ano, mes, 1)
    fim = (date(ano + 1, 1, 1) if mes == 12 else date(ano, mes + 1, 1)) - timedelta(days=1)
    return ini, min(fim, date.today())


def janela_str(d):
    return f"{d.year:04d}-{d.month:02d}"


def todas_janelas():
    """Do mês atual pra trás até MES_INICIO (ordem regressiva: 'comendo' o histórico)."""
    try:
        ano_i, mes_i = (int(x) for x in MES_INICIO.split("-"))
    except ValueError:
        falhar(f"LOG023_MES_INICIO inválido: {MES_INICIO} (use YYYY-MM)")
    hoje = date.today()
    ano, mes = hoje.year, hoje.month
    janelas = []
    while (ano, mes) >= (ano_i, mes_i):
        ini, fim = limites_mes(ano, mes)
        janelas.append((janela_str(ini), ini, fim))
        ano, mes = (ano - 1, 12) if mes == 1 else (ano, mes - 1)
    return janelas  # [ (yyyy-mm, ini, fim), ... ] do mais recente ao mais antigo


# ---------------------------------------------------------------------------
# Chamada à API LOG023
# ---------------------------------------------------------------------------

def fmt_data_api(d):
    """Formato dd/mm/aaaa (formato de sessão esperado do AppServer pt-BR).
    A guarda de validação abaixo detecta se o servidor interpretou errado."""
    return d.strftime("%d/%m/%Y")


def chamar_api(ini, fim):
    """Retorna (registros, duracao_seg). Lança RuntimeError com motivo em caso de falha.
    Trata: 400 = janela vazia (retorna []), datas-eco fora da janela = aborta."""
    url = f"{API_BASE}/pedidovenda"
    params = {"data_ini": fmt_data_api(ini), "data_fim": fmt_data_api(fim)}
    t0 = time.time()
    try:
        r = requests.get(url, params=params, auth=(API_USER, API_PASS), timeout=TIMEOUT)
    except requests.RequestException as e:
        raise RuntimeError(f"Falha de rede/timeout: {e}")
    dur = time.time() - t0

    if r.status_code == 400:
        return [], dur  # "pedido venda não disponíveis" => janela legitimamente vazia
    if r.status_code == 401:
        raise RuntimeError("401: usuário/senha Datasul inválidos")
    if r.status_code == 500:
        raise RuntimeError(
            "500 na rotina Progress (provável item com lote-mulven = 0 na janela). "
            "A chamada inteira cai — reportar ao Andrey/ERP o item problemático."
        )
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")

    # Encoding: fonte em CP850, codepage efetivo depende do AppServer.
    # Tenta o encoding declarado; se o JSON quebrar, tenta latin-1.
    try:
        payload = r.json()
    except ValueError:
        try:
            payload = json.loads(r.content.decode("latin-1"))
        except Exception as e:
            raise RuntimeError(f"Resposta não é JSON válido (encoding?): {e}")

    registros = payload.get("ttpedidovenda", [])
    if not isinstance(registros, list):
        raise RuntimeError("Resposta sem a coleção 'ttpedidovenda' esperada")

    # ---- GUARDA CRÍTICA: eco das datas ----
    # A API converte data inválida silenciosamente para HOJE. Se o formato enviado
    # não bateu com a sessão do servidor, os registros voltam fora da janela pedida.
    implantacoes = [parse_data(x.get("Dt Implantação")) for x in registros]
    implantacoes = [d for d in implantacoes if d]
    if implantacoes:
        mn, mx = min(implantacoes), max(implantacoes)
        if mn < ini or mx > fim:
            raise RuntimeError(
                f"Datas retornadas ({mn}..{mx}) fora da janela pedida ({ini}..{fim}). "
                "Provável formato de data rejeitado pelo servidor (caiu em TODAY). ABORTANDO."
            )
    return registros, dur


def parse_data(v):
    if not v:
        return None
    v = str(v).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(v[:10], fmt).date()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Transformação
# ---------------------------------------------------------------------------

def num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def transformar(registros):
    """Converte as chaves com espaço/acento da API para colunas do banco e
    AGREGA linhas duplicadas na mesma chave natural (a API não expõe
    nr-sequencia: itens repetidos no pedido somam quantidades/valores)."""
    por_chave = OrderedDict()
    for x in registros:
        chave = (
            str(x.get("Estabel", "")).strip(),
            str(x.get("Ped Cli", "")).strip(),
            str(x.get("Cod Item", "")).strip(),
            int(x.get("Nr Embarque") or 0),
        )
        linha = {
            "estabel": chave[0],
            "ped_cli": chave[1],
            "cod_item": chave[2],
            "nr_embarque": chave[3],
            "cliente": x.get("Cliente"),
            "matriz": x.get("Matriz"),
            "cidade_cliente": x.get("Cidade Cliente"),
            "uf_cliente": x.get("Uf Cliente"),
            "produtos": x.get("Produtos"),
            "desc_marca": x.get("Desc Marca"),
            "bonificado": (str(x.get("Bonificado", "")).strip().lower() == "sim"),
            "situacao": x.get("Situação"),
            "sit_alocacao": x.get("Sit Alocação"),
            "descricao_motivo": x.get("Descrição motivo") or None,
            "dt_implantacao": iso(parse_data(x.get("Dt Implantação"))),
            "dt_entrega": iso(parse_data(x.get("Data Entrega"))),
            "qtd_pedida": num(x.get("Qtd Pedida")),
            "qtd_atende": num(x.get("Qtd Atende")),
            "qt_aloca": num(x.get("Qt Aloca")),
            "qt_saldo": num(x.get("Qt Saldo")),
            "vl_tot_item": num(x.get("Vl Tot Item")),
            "vl_aberto_merc": num(x.get("Vl Aberto Merc")),
            "dt_embarque": iso(parse_data(x.get("Dt Hr Embarque"))),
            "nfe": (str(x.get("NF-e")).strip() or None) if x.get("NF-e") is not None else None,
            "dt_emissao_nfe": iso(parse_data(x.get("Dt Emissão NF-e"))),
            "qtd_vol_embarque": int(x.get("Qtd Vol Embarque") or 0),
            "qtd_linhas_agregadas": 1,
            "ativo": True,
            "ultima_atualizacao": datetime.utcnow().isoformat() + "Z",
        }
        if chave in por_chave:
            ac = por_chave[chave]
            for c in ("qtd_pedida", "qtd_atende", "qt_aloca", "qt_saldo",
                      "vl_tot_item", "vl_aberto_merc"):
                if linha[c] is not None:
                    ac[c] = (ac[c] or 0) + linha[c]
            ac["qtd_vol_embarque"] += linha["qtd_vol_embarque"]
            ac["qtd_linhas_agregadas"] += 1
        else:
            por_chave[chave] = linha
    duplicadas = sum(1 for l in por_chave.values() if l["qtd_linhas_agregadas"] > 1)
    if duplicadas:
        print(f"  [AVISO] {duplicadas} chave(s) com linhas repetidas na origem foram agregadas "
              "(itens repetidos no pedido — API não expõe nr-sequencia).")
    return list(por_chave.values())


def iso(d):
    return d.isoformat() if d else None


# ---------------------------------------------------------------------------
# Supabase (PostgREST)
# ---------------------------------------------------------------------------

def sb(metodo, caminho, **kw):
    r = requests.request(metodo, f"{REST}/{caminho}", headers={**HEADERS_SB, **kw.pop("headers", {})},
                         timeout=120, **kw)
    if r.status_code >= 300:
        raise RuntimeError(f"Supabase {metodo} {caminho} -> {r.status_code}: {r.text[:300]}")
    return r


def upsert_lote(linhas):
    for i in range(0, len(linhas), BATCH_SIZE):
        lote = linhas[i:i + BATCH_SIZE]
        sb("POST",
           "fato_pedido_api?on_conflict=estabel,ped_cli,cod_item,nr_embarque",
           headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
           data=json.dumps(lote))
        print(f"  upsert {i + len(lote)}/{len(linhas)}")


def desativar_ausentes(ini, fim, chaves_atuais):
    """Marca ativo=false nas linhas da janela cuja chave não voltou no re-fetch."""
    r = sb("GET",
           "fato_pedido_api"
           f"?select=id,estabel,ped_cli,cod_item,nr_embarque"
           f"&dt_implantacao=gte.{ini.isoformat()}&dt_implantacao=lte.{fim.isoformat()}"
           f"&ativo=is.true&limit=100000")
    existentes = r.json()
    sumiram = [e["id"] for e in existentes
               if (e["estabel"], e["ped_cli"], e["cod_item"], e["nr_embarque"]) not in chaves_atuais]
    for i in range(0, len(sumiram), BATCH_SIZE):
        ids = ",".join(str(x) for x in sumiram[i:i + BATCH_SIZE])
        sb("PATCH", f"fato_pedido_api?id=in.({ids})",
           headers={"Prefer": "return=minimal"},
           data=json.dumps({"ativo": False,
                            "ultima_atualizacao": datetime.utcnow().isoformat() + "Z"}))
    if sumiram:
        print(f"  [INFO] {len(sumiram)} registro(s) sumiram da origem e foram marcados ativo=false")


def registrar_janela(jan, ini, fim, status, qtd, qtd_abertos, dur, detalhe=None):
    sb("POST", "controle_janela_api?on_conflict=janela",
       headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
       data=json.dumps([{
           "janela": jan, "data_ini": ini.isoformat(), "data_fim": fim.isoformat(),
           "ultima_carga": datetime.utcnow().isoformat() + "Z",
           "qtd_registros": qtd, "qtd_abertos": qtd_abertos,
           "duracao_seg": round(dur, 1), "status": status, "detalhe": detalhe,
       }]))


def janelas_ja_carregadas():
    r = sb("GET", "controle_janela_api?select=janela,status,qtd_abertos")
    return {x["janela"]: x for x in r.json()}


# ---------------------------------------------------------------------------
# Processamento de uma janela
# ---------------------------------------------------------------------------

def processar_janela(jan, ini, fim):
    print(f"\n=== Janela {jan} ({ini} a {fim}) ===")
    try:
        registros, dur = chamar_api(ini, fim)
    except RuntimeError as e:
        print(f"  [ERRO] {e}")
        registrar_janela(jan, ini, fim, "erro", None, None, 0, str(e))
        return False

    print(f"  API respondeu em {dur:.1f}s com {len(registros)} linha(s)")
    if not registros:
        registrar_janela(jan, ini, fim, "vazia", 0, 0, dur)
        return True

    linhas = transformar(registros)
    upsert_lote(linhas)
    desativar_ausentes(ini, fim, {(l["estabel"], l["ped_cli"], l["cod_item"], l["nr_embarque"])
                                  for l in linhas})
    abertos = sum(1 for l in linhas if l["situacao"] in SITUACOES_ABERTAS)
    registrar_janela(jan, ini, fim, "ok", len(linhas), abertos, dur)
    print(f"  OK: {len(linhas)} chave(s), {abertos} em situação aberta")
    return True


# ---------------------------------------------------------------------------
# Modos de execução
# ---------------------------------------------------------------------------

def modo_backfill(tudo=False):
    carregadas = janelas_ja_carregadas()
    pendentes = [(j, i, f) for j, i, f in todas_janelas()
                 if carregadas.get(j, {}).get("status") not in ("ok", "vazia")]
    if not pendentes:
        print("Backfill completo — nenhuma janela pendente. ✔")
        return
    print(f"{len(pendentes)} janela(s) pendente(s): {', '.join(j for j, _, _ in pendentes)}")
    for jan, ini, fim in pendentes:
        ok = processar_janela(jan, ini, fim)
        if not tudo:
            break
        if not ok:
            print("Interrompendo backfill por erro (rode de novo após corrigir).")
            break


def modo_diario():
    hoje = date.today()
    jan_atual = janela_str(hoje)
    carregadas = janelas_ja_carregadas()
    alvo = OrderedDict()
    for jan, ini, fim in todas_janelas():
        info = carregadas.get(jan)
        if jan == jan_atual or (info and (info.get("qtd_abertos") or 0) > 0):
            alvo[jan] = (ini, fim)
    print(f"Refresh diário: {len(alvo)} janela(s): {', '.join(alvo)}")
    for jan, (ini, fim) in alvo.items():
        processar_janela(jan, ini, fim)


def modo_janela(jan):
    for j, ini, fim in todas_janelas():
        if j == jan:
            processar_janela(j, ini, fim)
            return
    falhar(f"Janela {jan} fora do range ({MES_INICIO} até hoje)")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Ingestão API LOG023 -> Supabase adm_vendas")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--backfill", action="store_true", help="carrega a próxima janela pendente (pra trás)")
    g.add_argument("--diario", action="store_true", help="mês atual + janelas com pedidos abertos")
    g.add_argument("--janela", metavar="YYYY-MM", help="força reprocessar uma janela específica")
    p.add_argument("--tudo", action="store_true", help="com --backfill: processa todas as pendentes")
    args = p.parse_args()

    checar_config()
    if args.backfill:
        modo_backfill(tudo=args.tudo)
    elif args.diario:
        modo_diario()
    else:
        modo_janela(args.janela)
