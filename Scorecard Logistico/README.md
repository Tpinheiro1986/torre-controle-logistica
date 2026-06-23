# Scorecard Logístico — Torre de Controle

Painel gerencial consolidado (18 indicadores, 6 dimensões, 2021–2026) da
Torre de Controle de Logística — Genomma Lab Brasil.

## Estrutura no repositório

```
/                      (raiz do repo na Torre de Controle — GitHub)
├── index.html         ← CAPA (já inclui o card "Scorecard Logístico")
└── scorecard/
    ├── index.html             ← o painel (dados embarcados + editor admin)
    ├── dados.json             ← semente publicada no Supabase (gerada pelo script)
    ├── publicar_scorecard.py  ← publicação (Supabase + opcional GitHub)
    ├── requirements.txt
    └── .env.example           ← copie para .env e preencha a senha do admin
```

> Coloque `index.html` (a capa) na **raiz** do repositório e a pasta
> `scorecard/` ao lado — exatamente como nos painéis `otd/`, `custo-servir/`, etc.

## Como funciona (mesmo padrão dos demais painéis)

- **Dados**: o painel embarca os dados como constante JS (funciona offline).
  Quando há um `scorecard/dados.json` no bucket `dashboards` do Supabase, o
  painel o baixa e sobrepõe os valores — igual ao `otd/dados.json`.
- **Edição / inclusão de dados futuros**: ao logar como administrador
  (`thiago_balao@yahoo.com.br`), aparece o botão **"Lançar / Atualizar"** no
  topo do scorecard. Ele abre a tabela mensal × anual onde se lança/edita
  valores, projeta o próximo mês pela média histórica, pré-visualiza e salva
  no Supabase. A sessão de login é compartilhada com a Torre.
- **Publicação**: o `publicar_scorecard.py` extrai os dados do HTML, gera o
  `dados.json` e faz upsert no Supabase; com `--git`, versiona a capa + o
  scorecard no GitHub (o GitHub Pages publica no push).

## Publicar

**Jeito mais simples (executável):** dê **duplo-clique em `publicar.bat`**
(Windows) — ele acha o Python, instala as dependências e publica **tudo**:
sobe o `dados.json` no Supabase e faz `git add -A` + commit + push de todas as
alterações (capa, scorecard, dados). No macOS/Linux use `./publicar.sh`.

> Antes do primeiro uso: copie `.env.example` para `.env` e preencha
> `SCORECARD_ADMIN_PASSWORD`.

**Pela linha de comando**, se preferir controle fino:

```bash
cd scorecard
pip install -r requirements.txt
cp .env.example .env          # preencha SCORECARD_ADMIN_PASSWORD

python publicar_scorecard.py             # só os dados no Supabase
python publicar_scorecard.py --git       # dados + capa/scorecard no GitHub
python publicar_scorecard.py --git-all   # dados + TODAS as mudancas no GitHub
python publicar_scorecard.py --apenas-json   # só gera o dados.json local
```

> Quer um `.exe` de verdade (sem depender de Python na maquina)? No Windows:
> `pip install pyinstaller` e depois
> `pyinstaller --onefile --name publicar_scorecard publicar_scorecard.py`.
> O binario sai em `dist\publicar_scorecard.exe`. (Tem que ser gerado no
> proprio Windows.)

## Segurança

- A senha do admin vem do `.env` (ou variável de ambiente), nunca do código.
- Adicione `.env` ao `.gitignore` do repositório.

## Solução de problemas

- **"Não usei o HTML (marcador de dados não localizado)"** — o `index.html`
  desta pasta não é o painel gerado (ou está num formato diferente). O script
  então usa o `dados.json` desta pasta. Para publicar com os dados certos:
  mantenha o `dados.json` aqui **ou** substitua o `scorecard/index.html` pelo
  painel gerado (que traz `const DATA = {…}` embutido).
- **"Defina SCORECARD_ADMIN_PASSWORD"** — falta o `.env`. Copie
  `.env.example` para `.env` e preencha a senha do admin.
- **Erro de `git push`** — rode o `.bat` de dentro do repositório já clonado
  (a pasta `scorecard/` precisa estar dentro do repo) e com o Git já
  autenticado (credential manager ou chave SSH).
