# -*- coding: utf-8 -*-
"""
validar_lst.py — Garante que o ESPD022.LST é do dia esperado ANTES da ingestão.

Resolve o problema de "pegar arquivo com data errada": se o RPW ainda não
regenerou o relatório do dia, o script aborta com código 1 e a carga não roda,
em vez de gravar dados velhos como se fossem de hoje.

Uso pelo .bat (aborta a carga se a data não bater):

    python validar_lst.py "Y:\\ERP-12\\rpw\\ESPD022.LST" || goto :erro

Opções:
    --data 04/08/2026      valida contra uma data específica (padrão: hoje)
    --linhas 40            quantas linhas do cabeçalho examinar (padrão: 40)

Uso como módulo dentro do script de ingestão:

    from validar_lst import extrair_data_lst
    data_rel = extrair_data_lst(caminho)   # retorna datetime.date ou None
"""

import argparse
import re
import sys
from datetime import date, datetime

# Formatos de data aceitos no cabeçalho do .LST
PADROES = [
    (re.compile(r"(\d{2})/(\d{2})/(\d{4})"), "dmY"),   # 04/08/2026
    (re.compile(r"(\d{2})-(\d{2})-(\d{4})"), "dmY"),   # 04-08-2026
    (re.compile(r"(\d{4})-(\d{2})-(\d{2})"), "Ymd"),   # 2026-08-04
    (re.compile(r"(\d{2})/(\d{2})/(\d{2})(?!\d)"), "dmy"),  # 04/08/26
]


def _montar_data(grupos, ordem):
    try:
        if ordem == "dmY":
            return date(int(grupos[2]), int(grupos[1]), int(grupos[0]))
        if ordem == "Ymd":
            return date(int(grupos[0]), int(grupos[1]), int(grupos[2]))
        if ordem == "dmy":
            ano = 2000 + int(grupos[2])
            return date(ano, int(grupos[1]), int(grupos[0]))
    except ValueError:
        return None
    return None


def extrair_data_lst(caminho, max_linhas=40):
    """
    Lê as primeiras `max_linhas` do .LST e devolve a primeira data plausível
    encontrada no cabeçalho (datetime.date), ou None se não achar nenhuma.
    Relatórios de ERP geralmente vêm em cp1252/latin-1.
    """
    with open(caminho, "r", encoding="cp1252", errors="replace") as f:
        for i, linha in enumerate(f):
            if i >= max_linhas:
                break
            for padrao, ordem in PADROES:
                m = padrao.search(linha)
                if m:
                    d = _montar_data(m.groups(), ordem)
                    # datas plausíveis para o relatório (evita pegar códigos)
                    if d and 2020 <= d.year <= 2035:
                        return d
    return None


def main():
    p = argparse.ArgumentParser(description="Valida a data do relatório .LST")
    p.add_argument("arquivo", help="Caminho do .LST")
    p.add_argument("--data", default=None,
                   help="Data esperada dd/mm/aaaa (padrão: hoje)")
    p.add_argument("--linhas", type=int, default=40,
                   help="Linhas do cabeçalho a examinar (padrão: 40)")
    args = p.parse_args()

    if args.data:
        esperada = datetime.strptime(args.data, "%d/%m/%Y").date()
    else:
        esperada = date.today()

    try:
        encontrada = extrair_data_lst(args.arquivo, args.linhas)
    except FileNotFoundError:
        print(f"[ERRO] Arquivo não encontrado: {args.arquivo}")
        sys.exit(1)

    if encontrada is None:
        print(f"[ERRO] Nenhuma data reconhecida nas primeiras {args.linhas} "
              f"linhas de {args.arquivo}.")
        print("       Verifique o cabeçalho do relatório ou aumente --linhas.")
        sys.exit(1)

    if encontrada != esperada:
        print(f"[ERRO] Relatório é de {encontrada.strftime('%d/%m/%Y')}, "
              f"esperado {esperada.strftime('%d/%m/%Y')}.")
        print("       O RPW provavelmente ainda não gerou o arquivo do dia. "
              "Carga abortada — nada foi gravado.")
        sys.exit(1)

    print(f"[OK] Relatório validado: {encontrada.strftime('%d/%m/%Y')}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
