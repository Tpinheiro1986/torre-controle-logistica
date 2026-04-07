@echo off
title Torre de Controle - Republicando dados...
color 0B
cd /d "%~dp0"

echo.
echo  ====================================================
echo   Republicando dados no dashboard
echo   Nao reprocessa XMLs - usa cte_dados.json existente
echo  ====================================================
echo.

python -m pip install requests --quiet --disable-pip-version-check 2>nul

python -c "
import json, requests, processar_cte as p
import os

if not os.path.exists('cte_dados.json'):
    print('  ERRO: cte_dados.json nao encontrado.')
    print('  Execute ATUALIZAR.bat primeiro.')
    exit(1)

dados = json.load(open('cte_dados.json', encoding='utf-8'))
data  = json.dumps(dados, ensure_ascii=False, separators=(',',':')).encode('utf-8')
url   = f'{p.SUPABASE_URL}/storage/v1/object/{p.BUCKET}/{p.PATH_JSON}'
hdrs  = {'Authorization': f'Bearer {p.SUPABASE_KEY}',
         'Content-Type': 'application/json', 'x-upsert': 'true'}

print('  Publicando...')
r = requests.post(url, data=data, headers=hdrs, timeout=60)
if r.status_code in (200, 201, 204):
    print('  OK! Dashboard atualizado.')
    print('  https://tpinheiro1986.github.io/torre-controle-logistica/custo-servir/')
else:
    print(f'  ERRO {r.status_code}: {r.text[:200]}')
    print('  Verifique a SUPABASE_KEY no processar_cte.py')
"

echo.
pause
