-- ============================================================================
-- SUPABASE — SCHEMA DO SIMULADOR S&OP / FORECAST
-- ============================================================================
--
-- INSTRUÇÕES (faça uma vez só):
--   1. Entre no Supabase: https://supabase.com/dashboard
--   2. Abra o projeto "Torre de Controle - Transportes"
--   3. No menu lateral, clique em "SQL Editor" (ícone de terminal)
--   4. Clique em "+ New query"
--   5. Cole TODO este arquivo (Ctrl+A, Ctrl+C aqui, Ctrl+V lá)
--   6. Clique em "Run" (canto inferior direito) ou aperte Ctrl+Enter
--   7. Espere aparecer "Success. No rows returned"
--   8. Volte para o Table Editor — deve aparecer 6 tabelas novas
--
-- Pode rodar este SQL várias vezes sem problema (é idempotente: usa
-- CREATE TABLE IF NOT EXISTS e DROP POLICY IF EXISTS).
--
-- ============================================================================


-- ----------------------------------------------------------------------------
-- PARTE 1: CRIAR AS 6 TABELAS
-- ----------------------------------------------------------------------------

-- Cadastro de SKUs (1 linha por código)
CREATE TABLE IF NOT EXISTS sop_sku (
  cod_item       VARCHAR(20)  PRIMARY KEY,
  descricao      VARCHAR(200),
  marca          VARCHAR(120),
  empresa        VARCHAR(20),
  upp_cadastro   INTEGER,
  upp_override   INTEGER,
  upp_efetiva    INTEGER,
  gtin14         VARCHAR(14),
  status_supply  VARCHAR(30),
  updated_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Plano S&OP — saídas mensais (kg ou UN)
CREATE TABLE IF NOT EXISTS sop_plano_saida (
  id          BIGSERIAL PRIMARY KEY,
  cod_item    VARCHAR(20)  NOT NULL,
  year_month  DATE         NOT NULL,
  qty_un      NUMERIC(14,2),
  horizonte   VARCHAR(10)  DEFAULT '2026',
  UNIQUE (cod_item, year_month, horizonte)
);

-- Forecast Pedidos — entradas mensais (containers/recebimento)
CREATE TABLE IF NOT EXISTS sop_plano_entrada (
  id          BIGSERIAL PRIMARY KEY,
  cod_item    VARCHAR(20)  NOT NULL,
  year_month  DATE         NOT NULL,
  qty_un      NUMERIC(14,2),
  horizonte   VARCHAR(10)  DEFAULT '2026',
  UNIQUE (cod_item, year_month, horizonte)
);

-- Estoque atual — uma foto por dia, mais recente vale
CREATE TABLE IF NOT EXISTS sop_estoque (
  cod_item       VARCHAR(20),
  snapshot_date  DATE,
  qty_un         NUMERIC(14,2),
  PRIMARY KEY (cod_item, snapshot_date)
);

-- Curva semanal por quarter — % de saída em cada semana do trimestre
CREATE TABLE IF NOT EXISTS sop_curva_quarter (
  empresa             VARCHAR(20),
  quarter             INTEGER,
  semana_no_quarter   INTEGER,
  pct                 NUMERIC(7,4),
  PRIMARY KEY (empresa, quarter, semana_no_quarter)
);

-- Metadados e parâmetros (capacidades, safety, %entrada, etc.)
CREATE TABLE IF NOT EXISTS sop_meta (
  chave       VARCHAR(50) PRIMARY KEY,
  valor       TEXT,
  user_email  VARCHAR(100),
  updated_at  TIMESTAMPTZ DEFAULT NOW()
);


-- ----------------------------------------------------------------------------
-- PARTE 2: ÍNDICES (acelera os SELECTs do painel)
-- ----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_saida_cod_ym  ON sop_plano_saida   (cod_item, year_month);
CREATE INDEX IF NOT EXISTS idx_entr_cod_ym   ON sop_plano_entrada (cod_item, year_month);
CREATE INDEX IF NOT EXISTS idx_estq_date     ON sop_estoque       (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_sku_empresa   ON sop_sku           (empresa);


-- ----------------------------------------------------------------------------
-- PARTE 3: ROW-LEVEL SECURITY (RLS)
-- Quem pode ler/escrever cada tabela
-- ----------------------------------------------------------------------------

-- Habilitar RLS em todas
ALTER TABLE sop_sku             ENABLE ROW LEVEL SECURITY;
ALTER TABLE sop_plano_saida     ENABLE ROW LEVEL SECURITY;
ALTER TABLE sop_plano_entrada   ENABLE ROW LEVEL SECURITY;
ALTER TABLE sop_estoque         ENABLE ROW LEVEL SECURITY;
ALTER TABLE sop_curva_quarter   ENABLE ROW LEVEL SECURITY;
ALTER TABLE sop_meta            ENABLE ROW LEVEL SECURITY;

-- Drop policies antigas se existirem (idempotente)
DROP POLICY IF EXISTS read_auth_sop_sku           ON sop_sku;
DROP POLICY IF EXISTS read_auth_sop_plano_saida   ON sop_plano_saida;
DROP POLICY IF EXISTS read_auth_sop_plano_entrada ON sop_plano_entrada;
DROP POLICY IF EXISTS read_auth_sop_estoque       ON sop_estoque;
DROP POLICY IF EXISTS read_auth_sop_curva_quarter ON sop_curva_quarter;
DROP POLICY IF EXISTS read_auth_sop_meta          ON sop_meta;

DROP POLICY IF EXISTS write_admin_sop_sku           ON sop_sku;
DROP POLICY IF EXISTS write_admin_sop_plano_saida   ON sop_plano_saida;
DROP POLICY IF EXISTS write_admin_sop_plano_entrada ON sop_plano_entrada;
DROP POLICY IF EXISTS write_admin_sop_estoque       ON sop_estoque;
DROP POLICY IF EXISTS write_admin_sop_curva_quarter ON sop_curva_quarter;
DROP POLICY IF EXISTS write_admin_sop_meta          ON sop_meta;

-- POLICY DE LEITURA — qualquer usuário autenticado vê tudo
CREATE POLICY read_auth_sop_sku           ON sop_sku           FOR SELECT TO authenticated USING (true);
CREATE POLICY read_auth_sop_plano_saida   ON sop_plano_saida   FOR SELECT TO authenticated USING (true);
CREATE POLICY read_auth_sop_plano_entrada ON sop_plano_entrada FOR SELECT TO authenticated USING (true);
CREATE POLICY read_auth_sop_estoque       ON sop_estoque       FOR SELECT TO authenticated USING (true);
CREATE POLICY read_auth_sop_curva_quarter ON sop_curva_quarter FOR SELECT TO authenticated USING (true);
CREATE POLICY read_auth_sop_meta          ON sop_meta          FOR SELECT TO authenticated USING (true);

-- POLICY DE ESCRITA — apenas thiago_balao@yahoo.com.br pode mudar
CREATE POLICY write_admin_sop_sku ON sop_sku FOR ALL TO authenticated
  USING (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br')
  WITH CHECK (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br');

CREATE POLICY write_admin_sop_plano_saida ON sop_plano_saida FOR ALL TO authenticated
  USING (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br')
  WITH CHECK (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br');

CREATE POLICY write_admin_sop_plano_entrada ON sop_plano_entrada FOR ALL TO authenticated
  USING (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br')
  WITH CHECK (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br');

CREATE POLICY write_admin_sop_estoque ON sop_estoque FOR ALL TO authenticated
  USING (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br')
  WITH CHECK (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br');

CREATE POLICY write_admin_sop_curva_quarter ON sop_curva_quarter FOR ALL TO authenticated
  USING (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br')
  WITH CHECK (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br');

CREATE POLICY write_admin_sop_meta ON sop_meta FOR ALL TO authenticated
  USING (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br')
  WITH CHECK (auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br');


-- ----------------------------------------------------------------------------
-- PARTE 4: VERIFICAÇÃO (informativo, não obrigatório)
-- ----------------------------------------------------------------------------
-- Para confirmar que tudo foi criado, pode rodar depois:
--
--   SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'sop_%';
--
-- Deve retornar 6 linhas:
--   sop_curva_quarter
--   sop_estoque
--   sop_meta
--   sop_plano_entrada
--   sop_plano_saida
--   sop_sku
--
-- ============================================================================
-- FIM
-- ============================================================================
