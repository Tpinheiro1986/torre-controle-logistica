"""Republica cte_dados.json no Supabase (sem reprocessar).
v9: a chave NAO fica mais neste arquivo. Ela e lida de SUPABASE_SERVICE_KEY /
SUPABASE_KEY (variavel de ambiente) ou do .env da pasta torre-controle-logistica."""
import json, requests
from processar_cte import _carregar_chave, SUPABASE_URL, BUCKET, PATH_JSON

CHAVE = _carregar_chave()
if not CHAVE:
    raise SystemExit("Chave nao encontrada: coloque SUPABASE_SERVICE_KEY=... no arquivo .env")

dados = json.load(open('cte_dados.json', encoding='utf-8'))
data  = json.dumps(dados, ensure_ascii=False, separators=(',',':')).encode('utf-8')

print(f"Publicando {len(data)/1024:.0f} KB...")
r = requests.post(
    f'{SUPABASE_URL}/storage/v1/object/{BUCKET}/{PATH_JSON}',
    data=data,
    headers=({'apikey': CHAVE, 'Content-Type': 'application/json', 'x-upsert': 'true'}
             | ({'Authorization': f'Bearer {CHAVE}'} if CHAVE.startswith('eyJ') else {})),
    timeout=60
)
if r.status_code in (200, 201, 204):
    print("OK! Dashboard atualizado.")
    resumo = dados.get('resumo', {})
    print(f"  Outbound : {resumo.get('outbound_ctes',0):,} CTes")
    print(f"  Inbound  : {resumo.get('inbound_ctes',0):,} CTes")
    print(f"  Reversa  : {resumo.get('reversa_ctes',0):,} CTes")
else:
    print(f"ERRO {r.status_code}: {r.text[:200]}")
