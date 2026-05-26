# Editor de Tabelas — Simulador de Frete

Editor visual integrado ao simulador. Permite ao administrador alterar taxas, pedágios, faixas de peso e rotas, com backup automático no Supabase a cada salvamento.

## Arquivos entregues

```
editor-pack/
└── index.html         ← substitui simulador-frete/index.html
```

Apenas 1 arquivo — toda a lógica do editor foi integrada ao HTML do simulador.

---

## ⚠️ ANTES DE FAZER DEPLOY — Passo crítico

Para o admin conseguir **salvar** alterações no Supabase, é necessário configurar uma **política de segurança (RLS)** no Storage. Sem isso, o botão "Salvar" vai dar erro de permissão.

### Passo 1: Configurar a Storage Policy

1. Abra o Supabase Studio:
   <https://supabase.com/dashboard/project/ennsbpibfnuwlvtodukg/storage/buckets/dashboards>

2. No menu lateral esquerdo do projeto, vá em **SQL Editor**

3. Cole o SQL abaixo e clique em **Run**:

```sql
-- Permissão de escrita no bucket "dashboards" APENAS para o admin
-- (apenas em arquivos com prefixo "simulador-frete/")

create policy "Admin pode escrever simulador-frete"
on storage.objects
for insert
to authenticated
with check (
  bucket_id = 'dashboards'
  and (storage.foldername(name))[1] = 'simulador-frete'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
);

create policy "Admin pode atualizar simulador-frete"
on storage.objects
for update
to authenticated
using (
  bucket_id = 'dashboards'
  and (storage.foldername(name))[1] = 'simulador-frete'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
);
```

4. Confirme que as duas policies foram criadas. Se aparecer erro "policy already exists", está OK — significa que já está configurado.

5. **Importante**: Essas policies usam `auth.jwt() ->> 'email'` para verificar quem é admin. Se você quiser adicionar outros admins no futuro, edite o SQL e troque por `auth.jwt() ->> 'email' in ('email1', 'email2', 'email3')`.

### Passo 2: Verificar leitura pública (já deve estar OK)

O bucket `dashboards` já tem leitura pública (o simulador e o custo-servir já leem). Para confirmar:

- Storage → dashboards → Policies → deve existir uma policy de `SELECT` para `public` ou `authenticated`. Se não existir, rode:

```sql
create policy "Leitura pública dashboards"
on storage.objects
for select
using (bucket_id = 'dashboards');
```

---

## Como funciona o editor

### Acesso
- O botão **"Editor de Tabelas"** aparece no topo do simulador APENAS para o e-mail `thiago_balao@yahoo.com.br`
- Outros usuários nem veem o botão — não há como acessar acidentalmente

### Estrutura em 3 sub-abas

**1. Visão geral**
- Grid com as 19 tabelas (cards clicáveis)
- Mostra: nome, transportador, nº de rotas, nº de taxas
- Clicar em um card → abre a tabela para edição

**2. Taxas gerais**
- Tabela editável estilo planilha
- Cada linha = uma taxa (AD Valorem, GRIS, Pedágio, etc.)
- Colunas: Nome, Tipo (% NF / % Frete / R$ fixo / R$ por fração), Valor, Mínimo, "Aplicar em" (cidades)
- Para taxas com lista de cidades (ex: Pedágio R$ 17,40 só para SP/Guarulhos/Osasco...) → clicar em "5 cidade(s)" abre modal para editar a lista
- Botões "+ Adicionar nova taxa" e "Remover"

**3. Rotas e faixas**
- Filtro por destino / descrição / ID
- Paginação (25 rotas por página)
- Cada rota expande mostrando:
  - ID e descrição (editáveis)
  - Origens e Destinos (botões para abrir modal de edição da lista)
  - **Faixas de peso**: mini-grid com "Até X kg: R$ Y" ou "Acima de X kg × multiplicador"
  - Taxas específicas da rota (raro, mas suportado)
- Botão "+ Nova rota" para criar do zero

### Indicadores visuais

- **Verde**: linha nova (adicionada agora)
- **Laranja**: linha modificada (existia, mas foi alterada)
- **Vermelho riscado**: linha que será removida ao salvar
- **Badge "●" laranja** na aba "Editor de Tabelas" quando há alterações pendentes
- **Toolbar fixa no topo** mostra "X alteração(ões) pendente(s)" e os botões Salvar / Descartar

### Salvamento

Ao clicar em **Salvar alterações**:

1. Modal de confirmação mostra resumo: "X taxas alteradas, Y rotas removidas, etc."
2. Confirmando, o sistema:
   - **Cria backup automático** em `simulador-frete/backups/tabelas-AAAAMMDD-HHMM-email.json`
   - **Sobrescreve** o arquivo principal `simulador-frete/tabelas.json`
   - **Atualiza o estado local** — o simulador passa a usar os novos valores imediatamente
3. Toast verde de confirmação

### Backups versionados

Localização no Supabase:
```
dashboards/
└── simulador-frete/
    ├── tabelas.json                                ← arquivo principal
    └── backups/
        ├── tabelas-20260521-1430-thiago_balao_yahoo_com_br.json
        ├── tabelas-20260522-0915-thiago_balao_yahoo_com_br.json
        └── ...
```

Os backups **nunca são deletados automaticamente** — você decide quando limpar. Para limpar antigos via Supabase Studio:
- Storage → dashboards → simulador-frete → backups → selecione os arquivos antigos → Delete

### Proteções

- **Aviso antes de fechar a aba** com alterações não salvas
- **Botão Descartar** com confirmação ("tem certeza?")
- **Verificação de sessão** antes de salvar (se logout, falha graciosamente)
- **Validação de tipos**: números em campos numéricos, percentuais limitados

---

## Como editar um caso típico

### Exemplo 1: Mudar o GRIS da Ativa de 0,15% para 0,18%

1. Login com `thiago_balao@yahoo.com.br`
2. Clica em **Editor de Tabelas**
3. **Visão geral** → clica no card "ATIVA 2026"
4. Na aba **Taxas gerais**, encontre a linha "GRIS"
5. No campo "Valor", troque `0.15` por `0.18`
6. A linha fica **laranja** (modificada). Aparece "1 alteração pendente" na toolbar
7. Clica em **Salvar alterações** → confirma → toast verde
8. Pronto. O simulador agora calcula GRIS = 0,18% para Ativa

### Exemplo 2: Adicionar pedágio R$ 89,50 para Campinas-SP no Transben Truck

1. Editor → Visão geral → "TRANSBEN TRUCK"
2. Taxas gerais → procure as linhas "PEDAGIO"
3. Clique em **+ Adicionar nova taxa**
4. Nome: `PEDAGIO`, Tipo: `R$ por documento`, Valor: `89.50`
5. Clica em "Todas (clique p/ restringir)"
6. No modal, digite `Campinas-SP` e clica Adicionar → Fechar
7. Salvar

### Exemplo 3: Alterar tarifa Extrema → São Paulo na FL Brasil

1. Editor → "FL BRASIL 2025"
2. Aba **Rotas e faixas** → filtra por "São Paulo" → expande a rota Extrema-MG → São Paulo-SP
3. Em **Faixas de peso**, ajuste os valores
4. Salvar

---

## Deploy

Depois de substituir `simulador-frete/index.html` localmente:

```powershell
cd "C:\Users\thiago.pinheiro\OneDrive - genommalabinternacional\Área de Trabalho\Codigo\torre-controle-logistica"
python deploy.py --so-github
```

Use `--so-github` porque você está alterando só o HTML — não precisa subir o `tabelas.json` (esse é gerenciado pelo próprio editor agora).

---

## Solução de problemas

### "Erro ao salvar: row-level security policy"
A policy do Supabase não foi configurada. Execute o SQL do "Passo 1" acima.

### "Sessão expirou. Faça login novamente."
A sessão Supabase expirou. Faça logout/login e tente de novo.

### O botão "Editor de Tabelas" não aparece
Você não está logado como `thiago_balao@yahoo.com.br`. Confirme o e-mail no topo (canto superior direito).

### Salvei, mas o simulador ainda mostra valores antigos
- Force refresh: Ctrl+Shift+R (Windows) ou Cmd+Shift+R (Mac)
- Aguarde 30s — o Supabase Storage tem cache CDN de até 30 segundos

### Quero reverter uma mudança
1. Supabase Studio → Storage → dashboards → simulador-frete → backups/
2. Baixe o backup desejado (ex: `tabelas-20260521-1430-thiago_balao_yahoo_com_br.json`)
3. Renomeie para `tabelas.json`
4. Faça upload sobrescrevendo o arquivo principal em `simulador-frete/tabelas.json`
5. Recarregue o simulador
