#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ajustes manuais aplicados DEPOIS do parser_tabelas.py (rodar a cada novo export).

Uso:  python ajustes_pos_parser.py tabelas_novo.json tabelas.json(atual) tabelas.json(saida)

1. ATIVA: o export traz 2 tabelas (ATIVA 2026 PEDAGIO = frete normal;
   ATIVA 2026 DEVOLUCAO = so devolucao, sem TDE/TRT). O simulador e a auditoria
   do Custo de Servir usam o nome 'ATIVA 2026' -> renomeia a PEDAGIO e descarta a DEVOLUCAO.
2. ATIVA: GRIS 0,1467% e TSO 0,1692% (o PDF mostra so 2 casas: 0,15 / 0,17;
   valores exatos confirmados em CT-e).
3. TRANSCUNHA TRUCK 2026 -> 'TRANSCUNHA TRUCK' (nome usado na auditoria).
4. Tabelas que existem na base atual e nao vieram no export sao mantidas
   (ex.: TRANSCUNHA CARRETA no export de 13/11/2026).
"""
import json, sys
novo, atual, saida = sys.argv[1:4]
new = json.load(open(novo, encoding='utf-8'))
old = json.load(open(atual, encoding='utf-8'))
out = []
for t in new['tabelas']:
    if t['nome'] == 'ATIVA 2026 DEVOLUCAO':
        continue
    if t['nome'] == 'ATIVA 2026 PEDAGIO':
        t['nome'] = 'ATIVA 2026'
    if t['nome'] == 'ATIVA 2026':
        for x in t['taxas_gerais']:
            if x['nome'] == 'GRIS': x['percentual'] = 0.1467
            if x['nome'] == 'TSO':  x['percentual'] = 0.1692
    if t['nome'] == 'TRANSCUNHA TRUCK 2026':
        t['nome'] = 'TRANSCUNHA TRUCK'
    out.append(t)
nomes = {t['nome'] for t in out}
for t in old['tabelas']:
    if t['nome'] not in nomes:
        print('mantida da base atual (ausente no export):', t['nome'])
        out.append(t)
json.dump({'tabelas': out}, open(saida, 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
print(len(out), 'tabelas,', sum(len(t['rotas']) for t in out), 'rotas ->', saida)
