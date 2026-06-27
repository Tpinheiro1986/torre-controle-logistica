# Painel Fiscal — Torre de Controle

Sistema web para importar e visualizar NF-e (XML) e Manifestação do
Destinatário (TXT), sobre o seu Supabase. Login, importação por arrastar
e soltar, e dashboard. Feito em React + Vite.

## Publicar (passo a passo, sem terminal)

### 1) Banco — JÁ ESTÁ PRONTO
As tabelas já foram criadas no projeto Supabase `ennsbpibfnuwlvtodukg`
e já têm dados de exemplo. (Em outro ambiente: rode o arquivo
`supabase/migrations/0001_nfe_modulo_fiscal.sql` no SQL Editor.)

### 2) Criar seu login
1. Acesse https://supabase.com → seu projeto **Torre de Controle - Transportes**
2. Menu lateral: **Authentication** → **Users** → **Add user** → **Create new user**
3. Informe um e-mail e uma senha (anote!) e confirme.
   → Esse será o login do painel.

### 3) Subir o código no GitHub
**Mais fácil (GitHub Desktop):**
1. Baixe o GitHub Desktop: https://desktop.github.com
2. File → New repository (ou clone o seu repositório existente)
3. Copie todos os arquivos desta pasta para dentro do repositório
4. Escreva um resumo e clique **Commit** → depois **Push origin**

**Pelo site (sem instalar nada):**
1. Crie um repositório em https://github.com/new
2. Clique em **uploading an existing file**
3. Arraste TODOS os arquivos/pastas (menos `node_modules`) → **Commit changes**

### 4) Publicar na Vercel (gera o link do painel)
1. Acesse https://vercel.com e entre com sua conta do GitHub
2. **Add New… → Project** → selecione o repositório
3. Em **Framework Preset** confirme **Vite**
4. Abra **Environment Variables** e adicione as duas:
   - `VITE_SUPABASE_URL` = `https://ennsbpibfnuwlvtodukg.supabase.co`
   - `VITE_SUPABASE_ANON_KEY` = `sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONyI5`
5. Clique **Deploy**. Em ~1 min você recebe um link `https://...vercel.app`

### 5) Usar
Abra o link, faça login com o e-mail/senha do passo 2, e:
- **Painel**: vê as notas (clique pra abrir os itens) e manifestações
- **Importar**: arraste os XML e TXT das suas 3 pastas — entram no banco sozinhos

## Importar automaticamente das pastas do Y: (robô)

O painel na nuvem NÃO enxerga o drive `Y:` (é rede interna). Para ler as pastas,
rode o robô no PC/servidor que tem o `Y:` mapeado. Ele lê e envia pro Supabase.

Pastas já configuradas em `scripts/importar.py` (apenas 2026 em diante):
- NF-e → `Y:\ERP-12\ArqXML-MG`
- CT-e → `Y:\ERP-12\TOTVSCOLAB20-PRD\RECEIVED`
- Romaneios → `Y:\ERP-12\TRANSP-PRD\ROMANEIO\Enviados` (com subpastas)

Passos no arquivo `scripts/LEIA-ME_IMPORTACAO.txt`. Resumo:
1. Instale o Python (marque "Add Python to PATH").
2. Em `scripts/importar.py`, preencha `LOGIN_EMAIL` e `LOGIN_SENHA` (o login do painel).
3. Dois cliques em `scripts/EXECUTAR_IMPORTACAO.bat`.
4. (Opcional) Agende no Agendador de Tarefas do Windows pra rodar sozinho.

## Observações
- **Manifestação** (`0`=Ciência, `2`=Desconhecimento) e **CT-e** seguem o padrão
  nacional; valide com 1 arquivo real e ajuste se necessário.
- **Romaneio**: formato ainda não confirmado — o robô registra os arquivos; envie
  1 exemplo para mapear os campos.
- A aba **Importar** do painel também aceita arrastar arquivos manualmente.
- Rodar o site localmente: copie `.env.example` para `.env`, `npm install`, `npm run dev`.

## Análise: a nota como origem (vínculos)

A aba **Vínculos** mostra cada NF-e como origem e se o processo seguiu:
romaneio → CT-e → confirmação de saída.

- **CT-e**: ligado pela chave da NF-e que o CT-e transporta (tabela `cte_nfe_ref`).
- **Confirmação de saída**: o arquivo MANIFE é a confirmação da operação (evento 210200);
  ligado pela `chave_nfe`.
- **Romaneio**: vínculo pendente até definirmos o formato do arquivo.

Tudo consolidado na view `vw_nota_vinculos`.
