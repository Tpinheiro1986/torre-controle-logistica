# Como publicar o Simulador de Frete (passo a passo)

Este guia é pra fazer o **primeiro deploy**. Depois do setup inicial, é só rodar `python deploy.py` sempre que mudar algo.

---

## Visão geral — como sua Torre de Controle funciona hoje

```
GitHub (interface)              Supabase Storage (dados)
─────────────────              ────────────────────────
index.html             ────►   bucket "dashboards"
otd/index.html         ────►     ├─ otd/dados.json
custo-servir/...       ────►     ├─ otd/historico_mestre.csv
simulador-frete/...    ────►     ├─ custo-servir/dados.json
                                  ├─ custo-servir/ctes_raw.json
                                  └─ simulador-frete/tabelas.json  ← NOVO
       │
       └── GitHub Pages publica em
           https://tpinheiro1986.github.io/torre-controle-logistica/
```

- **GitHub** = arquivos da interface (HTMLs estáticos)
- **Supabase Storage** = arquivos de dados que os HTMLs leem
- **Os dois precisam estar sincronizados** — é por isso que às vezes quebra

O `deploy.py` resolve isso: **um único comando** atualiza os dois.

---

## Setup inicial (faz UMA vez só)

### 1. Coloque os arquivos na pasta do seu repositório

Sua pasta local do projeto (a que está no Git) deve ficar assim depois do deploy-pack:

```
torre-controle-logistica/             ← raiz do seu repositório
├── index.html                        ← capa (você já alterou com o card novo)
├── otd/
│   └── index.html
├── custo-servir/
│   └── index.html
├── simulador-frete/                  ← NOVO
│   ├── index.html
│   └── tabelas.json
├── deploy.py                         ← NOVO (script)
├── deploy.config.json                ← NOVO (configuração)
├── .env                              ← NOVO (credenciais — NÃO comitar!)
└── .gitignore                        ← atualizado
```

Os 4 arquivos novos (`simulador-frete/index.html`, `simulador-frete/tabelas.json`, `deploy.py`, `deploy.config.json`) você vai copiar do pacote que te entreguei.

O `.env` o próprio script cria pra você (próximo passo).

### 2. Inicializar o script

Abra o terminal do VS Code na pasta do projeto e rode:

```bash
python deploy.py --init
```

Isso vai criar o `.env` e o `.gitignore` automaticamente.

### 3. Pegar a service_role key do Supabase

Esta é a chave que permite o script subir arquivos no Supabase Storage. Ela fica **SÓ no seu PC**, dentro do `.env`, e o `.gitignore` garante que ela **nunca** vai pro GitHub.

Passo a passo:

1. Acesse: <https://supabase.com/dashboard/project/ennsbpibfnuwlvtodukg/settings/api>
2. Faça login na sua conta Supabase (se já não estiver logado)
3. Role até **"Project API keys"**
4. Você vai ver duas chaves:
   - `anon` (essa é a pública, está no código dos painéis)
   - `service_role` ← **é essa** (tem um ícone de olho 👁 — clique para revelar)
5. Copie o valor todo (uma string bem longa que começa com `eyJhbGciOi...`)

### 4. Colar a chave no `.env`

Abra o arquivo `.env` no VS Code e cole assim:

```
SUPABASE_SERVICE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...sua_chave_completa...
```

⚠️ **Importante**: 
- Sem aspas
- Sem espaços antes/depois do `=`
- O `.gitignore` já bloqueia esse arquivo, mas confira: `git status` não deve mostrar o `.env`

### 5. Faça o deploy

```bash
python deploy.py
```

Você verá algo como:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[1/2] Deploy no Supabase Storage
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ℹ Bucket: dashboards
ℹ URL:    https://ennsbpibfnuwlvtodukg.supabase.co
ℹ Arquivos a enviar: 1
  Enviando simulador-frete/tabelas.json → dashboards/simulador-frete/tabelas.json (864.4 KB)... OK
✓ Supabase: 1 arquivo(s) enviado(s) com sucesso

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[2/2] Deploy no GitHub (GitHub Pages)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Mudanças detectadas:
  A  simulador-frete/index.html
  A  deploy.py
  A  deploy.config.json
  M  index.html

ℹ Adicionando arquivos...
ℹ Commit: "deploy: atualizacao painel (21/05/2026 14:22)"
ℹ Push para o remoto...
✓ GitHub: commit + push concluídos. GitHub Pages atualiza em ~30s a 2min.

✓ Deploy concluído com sucesso!
```

### 6. Verificar no ar

Espere 1-2 minutos e abra:
<https://tpinheiro1986.github.io/torre-controle-logistica/>

Você verá o card "Simulador de Frete" e poderá clicar para entrar.

---

## Uso no dia-a-dia (depois do setup)

### Atualizar tudo (mais comum)

```bash
python deploy.py
```

### Casos específicos

```bash
# Só mudei HTML, não preciso subir nada no Supabase
python deploy.py --so-github

# Só atualizei o tabelas.json (renegociei preços), HTMLs não mudaram
python deploy.py --so-supabase

# Ver o que faria sem executar de verdade
python deploy.py --dry-run

# Com mensagem de commit customizada
python deploy.py --msg "atualiza tabela Ativa 2026"
```

---

## Cenários comuns

### Mudei só o `index.html` da capa
```bash
python deploy.py --so-github
```
Por quê: HTMLs ficam no GitHub. Supabase nem precisa ser tocado.

### Renegociei as tabelas — tem `tabelas.json` novo
Substitua o `simulador-frete/tabelas.json` e rode:
```bash
python deploy.py --so-supabase
```
Por quê: o JSON fica no Supabase, o HTML não muda.

### Mudei o código do simulador (`simulador-frete/index.html`)
```bash
python deploy.py --so-github
```

### Adicionei um painel novo (ex: "estoque")
1. Crie a pasta `estoque/` com o `index.html` dele
2. Se ele precisar de JSON de dados, adicione em `deploy.config.json`:
   ```json
   "supabase_files": [
     { "local": "simulador-frete/tabelas.json", "remote": "simulador-frete/tabelas.json" },
     { "local": "estoque/dados.json", "remote": "estoque/dados.json" }
   ]
   ```
3. Rode `python deploy.py`

---

## Solução de problemas

### `SUPABASE_SERVICE_KEY não encontrada`
- O `.env` não existe → rode `python deploy.py --init`
- A chave está vazia no `.env` → cole a chave do Supabase Studio
- Tem aspas no valor → remova as aspas

### `git push falhou`
- Primeira vez: rode `git push -u origin main`
- Pediu senha: configure SSH ou token do GitHub
- "rejected" / "behind": rode `git pull --rebase` antes

### `Comando não encontrado: git`
- Instale Git: <https://git-scm.com/downloads>

### `python: command not found` (Mac/Linux)
- Use `python3 deploy.py` em vez de `python deploy.py`

### O painel novo não aparece no ar mesmo após o deploy
- Espere 2 min (GitHub Pages tem cache)
- Force refresh no navegador: `Ctrl+Shift+R` (Win) ou `Cmd+Shift+R` (Mac)
- Modo anônimo para descartar cache local

### Supabase deu erro 400 "new row violates row-level security policy"
- Significa que o bucket tem políticas de RLS restritivas para upload
- Solução: use service_role (já é o que o script faz). Se mesmo assim falhar, verifique no Supabase Studio se a chave é a `service_role` (não a `anon`)

---

## Segurança

- ✅ A `service_role key` fica no seu `.env` local
- ✅ O `.gitignore` impede que o `.env` vá para o GitHub
- ✅ Antes de qualquer push, confira: `git status` não deve mostrar `.env`
- ⚠️ Se o `.env` for commitado por engano, **revogue** a chave no Supabase Studio (Settings → API → Reset service_role JWT) e gere uma nova
