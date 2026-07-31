-- ============================================================
-- Torre de Controle · Simulador S&OP  ·  aba S&OE
-- Tabela de faturamento real (execução do S&OP)
-- Rodar no SQL Editor do projeto Supabase e recarregar a página.
-- ============================================================

create table if not exists public.sop_faturamento_real (
  cod_item    text        not null,
  year_month  date        not null,           -- sempre o dia 1 do mês (ex.: 2026-06-01)
  horizonte   text        not null default '2026',
  qty_un      numeric     not null default 0, -- quantidade faturada em VENDA (UN)
  qty_bonif   numeric     not null default 0, -- bonificação / brinde / amostra (UN)
  fonte       text,                           -- 'FT4003' ou 'template'
  updated_at  timestamptz not null default now(),
  primary key (cod_item, year_month, horizonte)
);

create index if not exists sop_faturamento_real_ym_idx
  on public.sop_faturamento_real (horizonte, year_month);

alter table public.sop_faturamento_real enable row level security;

-- Leitura: qualquer usuário autenticado da Torre de Controle
drop policy if exists "sop_fat_real_select" on public.sop_faturamento_real;
create policy "sop_fat_real_select"
  on public.sop_faturamento_real
  for select
  to authenticated
  using (true);

-- Escrita: somente o admin (mesmo padrão das demais tabelas do simulador)
drop policy if exists "sop_fat_real_admin" on public.sop_faturamento_real;
create policy "sop_fat_real_admin"
  on public.sop_faturamento_real
  for all
  to authenticated
  using      (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br')
  with check (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br');

-- ============================================================
-- Conferência rápida depois do primeiro upload:
--
--   select to_char(year_month,'YYYY-MM') as mes,
--          count(distinct cod_item)      as skus,
--          sum(qty_un)                   as venda_un,
--          sum(qty_bonif)                as bonificacao_un
--     from public.sop_faturamento_real
--    where horizonte = '2026'
--    group by 1 order by 1;
-- ============================================================
