import json, requests

# Cole a chave Legacy service_role aqui (começa com eyJ...)
CHAVE = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVubnNicGliZm51d2x2dG9kdWtnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NDM4NDgzNywiZXhwIjoyMDg5OTYwODM3fQ.gnCLe-XvoWJoiVEG4jRCPCdX8OsevXACk0TISgo9S04python publicar.py"

dados = json.load(open('cte_dados.json', encoding='utf-8'))
data  = json.dumps(dados, ensure_ascii=False, separators=(',',':')).encode('utf-8')

print(f"Publicando {len(data)/1024:.0f} KB...")
r = requests.post(
    'https://ennsbpibfnuwlvtodukg.supabase.co/storage/v1/object/dashboards/custo-servir/dados.json',
    data=data,
    headers={
        'Authorization': f'Bearer {CHAVE}',
        'Content-Type': 'application/json',
        'x-upsert': 'true'
    },
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
