-- ===== ADM DE VENDAS · Torre de Controle (já aplicado no projeto ennsbpibfnuwlvtodukg) =====
create schema if not exists adm_vendas;

create table if not exists adm_vendas.dim_cliente (
  codigo integer primary key,
  abreviado text not null,
  nome text, uf text,
  validade_min_lote integer default 0,
  calculo_vencto text,           -- DDE / DDF
  matriz text,
  atualizado_em timestamptz default now()
);
create index if not exists idx_dim_cliente_abrev on adm_vendas.dim_cliente (abreviado);

create table if not exists adm_vendas.dim_item (
  cod_item text primary key,
  descricao text not null,
  marca text,
  atualizado_em timestamptz default now()
);

create table if not exists adm_vendas.fato_carteira (
  id bigint generated always as identity primary key,
  dt_carga date not null,
  cliente text not null, ped_cli text not null, cod_item text not null,
  situacao text not null,
  dt_implantacao date, dt_entrega date,
  qt_pedida numeric default 0, qt_alocada_embarque numeric default 0,
  qt_atendida numeric default 0, qt_saldo numeric default 0,
  vl_tot_item numeric default 0, vl_aberto numeric default 0,
  criado_em timestamptz default now()
);
create index if not exists idx_fato_dtcarga on adm_vendas.fato_carteira (dt_carga);
create index if not exists idx_fato_pedido on adm_vendas.fato_carteira (ped_cli);
create index if not exists idx_fato_item on adm_vendas.fato_carteira (cod_item);

create table if not exists adm_vendas.pendencias_cadastro (
  id bigint generated always as identity primary key,
  tipo text not null check (tipo in ('cliente','item')),
  codigo text not null,
  detectado_em date not null default current_date,
  resolvido boolean not null default false,
  unique (tipo, codigo)
);

alter table adm_vendas.dim_cliente enable row level security;
alter table adm_vendas.dim_item enable row level security;
alter table adm_vendas.fato_carteira enable row level security;
alter table adm_vendas.pendencias_cadastro enable row level security;
create policy "leitura publica dim_cliente" on adm_vendas.dim_cliente for select using (true);
create policy "leitura publica dim_item" on adm_vendas.dim_item for select using (true);
create policy "leitura publica fato" on adm_vendas.fato_carteira for select using (true);
create policy "leitura publica pendencias" on adm_vendas.pendencias_cadastro for select using (true);
grant usage on schema adm_vendas to anon, authenticated, service_role;
grant select on all tables in schema adm_vendas to anon, authenticated;
grant all on all tables in schema adm_vendas to service_role;
grant usage, select on all sequences in schema adm_vendas to service_role;
