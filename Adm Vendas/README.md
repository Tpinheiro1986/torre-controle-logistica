# Torre de Controle — ADM de Vendas

Painel diário da carteira de pedidos (ESPD022 / Datasul) integrado ao Supabase.

## Estrutura
```
index.html                     -> capa da torre (card "ADM de Vendas" após Auditoria Fiscal)
adm-vendas/index.html          -> painel (gerado; dados do dia embutidos + banner de pendências)
adm-vendas/torre_template.html -> template usado pela ingestão (NÃO editar os __PLACEHOLDERS__)
ingestao/ingestao_diaria.py    -> roda todo dia: LST -> Supabase -> regenera o painel
ingestao/carga_inicial.py      -> sincroniza cadastros (Clientes.xls / Itens_.xlsx -> dims)
ingestao/Clientes.xls          -> base de clientes (código, abreviado, nome, UF, shelf life, DDE/DDF, matriz)
ingestao/Itens_.xlsx           -> base de itens (código, descrição)
sql/schema.sql                 -> schema adm_vendas (já aplicado no projeto ennsbpibfnuwlvtodukg)
```

## Supabase (já criado e carregado)
Projeto `ennsbpibfnuwlvtodukg`, schema **adm_vendas**:
- `dim_cliente` (1.258) · `dim_item` (664) · `fato_carteira` (fotografia diária) · `pendencias_cadastro`
- Leitura pública (anon); escrita apenas com a **service_role key**.

## Configuração (uma vez)
1. Instale dependências: `pip install -r ingestao/requirements.txt`
2. Defina as variáveis de ambiente (Windows: Painel de Controle > Variáveis de ambiente):
   - `SUPABASE_URL` = `https://ennsbpibfnuwlvtodukg.supabase.co`
   - `SUPABASE_SERVICE_KEY` = service_role key (Dashboard > Settings > API). **Nunca commitar essa chave.**
   - `PASTA_LST` = pasta onde o ESPD022.LST cai todo dia (ex: `C:\torre\ruptura`)

## Rotina diária (automática)
1. `instalar.bat` (uma vez): instala dependências e cria o `config.bat`
2. Edite `config.bat`: cole a service_role key e confira a `PASTA_LST`
3. **`atualizar_torre.bat`** faz tudo: ingestão do LST -> Supabase -> regenera painel -> `git push`
4. Agende `atualizar_torre.bat` no Task Scheduler (diário, após o LST ser gerado)

O `config.bat` está no `.gitignore` — a chave nunca sobe para o GitHub.
Manual, se preferir: `python ingestao/ingestao_diaria.py` seguido de commit+push.
Agende no **Task Scheduler** (Windows) para rodar após a geração do LST (ex: 07:00).
O script: parseia o LST -> substitui a carga do dia no `fato_carteira` -> detecta cadastros
faltantes -> regenera `adm-vendas/index.html` com banner amarelo de pendências no topo.

## Cadastro de item ou cliente novo (fácil)
1. O painel mostra o banner "⚠ Cadastros pendentes" com os códigos faltantes
   (também ficam em `adm_vendas.pendencias_cadastro`).
2. Adicione a linha na planilha `ingestao/Clientes.xls` ou `ingestao/Itens_.xlsx`.
3. Rode `python ingestao/carga_inicial.py` (upsert nas dims + marca pendência resolvida).
4. Rode `python ingestao/ingestao_diaria.py` para o banner sumir do painel.

## Publicação (GitHub Pages)
Repo: https://github.com/Tpinheiro1986/torre-controle-logistica
```
git clone https://github.com/Tpinheiro1986/torre-controle-logistica
# copie o conteúdo deste pacote para a raiz do repo (mesclando com o que já existe)
git add -A && git commit -m "ADM de Vendas" && git push
```
Em Settings > Pages, sirva a branch `main` (root). A capa ganha o card **ADM de Vendas**,
que abre `adm-vendas/index.html`; o botão **← Menu inicial** volta para a capa.
