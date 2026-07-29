# -*- coding: utf-8 -*-
"""
Carga inicial / atualização de cadastros — dim_cliente e dim_item
Fluxo de cadastro fácil: adicione a linha nova em Clientes.xls ou Itens_.xlsx
(nesta mesma pasta) e rode:  python carga_inicial.py
O script faz upsert no Supabase e marca as pendências como resolvidas.

Env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY (mesmas do ingestao_diaria.py)
"""
import os, json, requests
import pandas as pd

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://ennsbpibfnuwlvtodukg.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
HD = {"apikey": KEY, "Authorization": f"Bearer {KEY}",
      "Accept-Profile": "adm_vendas", "Content-Profile": "adm_vendas",
      "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
REST = SUPABASE_URL.rstrip("/") + "/rest/v1"
AQUI = os.path.dirname(os.path.abspath(__file__))

def upsert(tabela, pk, registros):
    for i in range(0, len(registros), 500):
        r = requests.post(f"{REST}/{tabela}?on_conflict={pk}", headers=HD,
                          data=json.dumps(registros[i:i+500]))
        r.raise_for_status()
    print(f"{tabela}: {len(registros)} registros upsert")

def resolve_pendencias(tipo, codigos):
    if not codigos: return
    lst = ",".join(f'"{c}"' for c in codigos)
    requests.patch(f"{REST}/pendencias_cadastro?tipo=eq.{tipo}&codigo=in.({lst})",
                   headers=HD, data=json.dumps({"resolvido": True})).raise_for_status()

def main():
    assert KEY, "Defina a variável de ambiente SUPABASE_SERVICE_KEY"

    cli = pd.read_excel(os.path.join(AQUI, "Clientes.xls"))
    regs = [dict(codigo=int(r["Codigo"]), abreviado=str(r["Abreviado"]).strip(),
                 nome=str(r["Nome"]).strip(),
                 uf=str(r["Estado"]).strip() if pd.notna(r["Estado"]) else "",
                 validade_min_lote=int(r["Validade Min Lote"]) if pd.notna(r["Validade Min Lote"]) else 0,
                 calculo_vencto=str(r["Calculo Vencto"]).strip() if pd.notna(r["Calculo Vencto"]) else "",
                 matriz=str(r["Matriz"]).strip() if pd.notna(r["Matriz"]) else "")
            for _, r in cli.iterrows() if pd.notna(r["Codigo"])]
    upsert("dim_cliente", "codigo", regs)
    resolve_pendencias("cliente", [r["abreviado"] for r in regs])

    it = pd.read_excel(os.path.join(AQUI, "Itens_.xlsx"))
    col_marca = next((c for c in it.columns if "arca" in c), None)
    regs = [dict(cod_item=str(r["Cod Item"]).strip(), descricao=str(r["Descrição Item"]).strip(),
                 marca=str(r[col_marca]).strip() if col_marca and pd.notna(r[col_marca]) else "")
            for _, r in it.iterrows() if pd.notna(r["Cod Item"])]
    upsert("dim_item", "cod_item", regs)
    resolve_pendencias("item", [r["cod_item"] for r in regs])
    print("Cadastros sincronizados. Rode ingestao_diaria.py para atualizar o painel.")

if __name__ == "__main__":
    main()
