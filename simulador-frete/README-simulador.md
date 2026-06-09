# Simulador de Frete — Torre de Controle

Comparativo de fretes entre todas as transportadoras da Genomma, integrado ao painel existente e ao Supabase.

## Arquivos entregues

```
index.html                      ← painel principal (sobrescrever o existente)
simulador-frete/
  ├── index.html                ← página do simulador
  └── tabelas.json              ← base com 19 tabelas / 2.610 rotas / 5.321 cidades
```

## O que mudou no painel principal

Foi adicionado um terceiro card "Simulador de Frete" (badge laranja, ao lado de Custo de Servir). Nada mais foi alterado: login, upload, demais cards e o `storageKey: torre-controle-auth` continuam idênticos — quem já está logado no painel entra no simulador sem refazer login.

## Como subir no Supabase (3 passos)

### 1. Hospedagem dos HTMLs

Mesma forma que você já faz com `otd/` e `custo-servir/`. A estrutura final do site fica:

```
index.html
otd/index.html
custo-servir/index.html
simulador-frete/index.html      ← novo
```

### 2. Upload do JSON de tabelas no Storage

O simulador lê as tabelas do bucket `dashboards` (o mesmo já usado pelo OTD).

**Via Supabase Studio (mais simples):**

1. Abra https://supabase.com/dashboard/project/ennsbpibfnuwlvtodukg/storage/buckets/dashboards
2. Clique em **Create folder** → nome: `simulador-frete`
3. Entre na pasta e faça upload do arquivo `tabelas.json`
4. Caminho final: `dashboards/simulador-frete/tabelas.json`

**Via CLI (se preferir):**

```bash
supabase storage cp tabelas.json dashboards/simulador-frete/tabelas.json
```

### 3. Política de acesso (RLS)

O bucket `dashboards` já tem permissão de leitura para usuários autenticados (porque o OTD funciona). O simulador usa a mesma política, então **nada precisa ser ajustado**.

Se quiser confirmar, em `Storage → dashboards → Policies` deve existir uma policy do tipo:
- `SELECT` para `authenticated` → expression: `bucket_id = 'dashboards'`

## Como funciona o cálculo

Para cada simulação, o sistema:

1. **Identifica o peso considerado** = `MAX(peso bruto, cubagem × 300 kg/m³)` — fator M³ padrão das tabelas
2. **Procura rotas compatíveis** em cada uma das 19 tabelas: rota cuja origem inclui a cidade origem digitada E destino inclui a cidade destino digitada
3. **Aplica as regras de peso da rota:**
   - **Tabela fracionada** (Ativa, FL Brasil): faixas progressivas — "Até 50kg: R$ X" → "Até 100kg: R$ Y" → "Acima de 100kg: × Z/kg"
   - **Carga fechada** (Ala Cargo, Cootravale, Expresso Minas, LD Cargo, Transben, Transcunha): detectada quando `Acima de X kg: Multiplicar por 0` → valor fixo do veículo dedicado
4. **Soma as taxas aplicáveis:**
   - `AD VALOREM` / `ADVALOREN` → % sobre valor da NF (com mínimo opcional)
   - `GRIS` → % sobre valor da NF (com mínimo opcional)
   - `PEDÁGIO` → R$ fixo por documento OU R$ por fração de N kg (varia por destino — só aplica se a cidade destino estiver na lista de cidades da taxa)
   - `EMEX` / `FRETEVALOR` / `ADICIONAL FRETE` → conforme cada tabela
   - **DEVOLUÇÃO, REENTREGA, TDA, TDE são ignoradas** na simulação normal (são cenários especiais que disparam só se a entrega der errado)

## Validações realizadas

Cenários testados:

| Origem → Destino | Peso | NF | Vencedor | Total |
|---|---|---|---|---|
| Extrema-MG → São Paulo-SP | 100 kg | R$ 1.000 | FL Brasil 2025 | R$ 76,11 |
| Extrema-MG → Rio de Janeiro-RJ | 500 kg | R$ 5.000 | Ativa 2026 | R$ 419,26 |
| Extrema-MG → Brasília-DF | 50 kg | R$ 2.500 | FL Brasil 2025 | R$ 86,55 |
| Extrema-MG → Salvador-BA | 200 kg | R$ 3.000 | FL Brasil 2025 | R$ 293,56 |

A comparação mostra que para encomendas pequenas/médias as **fracionadas (FL Brasil, Ativa)** vencem; para volumes que justificam veículo dedicado, as cargas fechadas passam a competir.

## Atualizar as tabelas no futuro

Quando as tabelas forem renegociadas:

1. Exporte o novo arquivo de tabelas analítico (mesmo formato do `tabelas_analitico_*.pdf` atual)
2. Me peça para regenerar o `tabelas.json`, ou rode o parser Python (`parser_tabelas.py` foi salvo no projeto)
3. Faça upload do novo `tabelas.json` substituindo o anterior no Supabase Storage
4. Como o simulador usa `cacheControl:'0'` implícito do Supabase para downloads, a próxima simulação já pega a versão nova

## Versão

`v1.0-simulador` · 21/05/2026
