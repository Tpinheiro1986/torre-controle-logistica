-- ===== Modulo Fiscal: NF-e + Manifestacao do Destinatario =====
-- Aplicado em: Torre de Controle - Transportes (ennsbpibfnuwlvtodukg)

CREATE TABLE IF NOT EXISTS public.nfe_notas (
  id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chave                   varchar(44) NOT NULL UNIQUE,
  numero                  text,
  serie                   text,
  modelo                  text,
  natureza_operacao       text,
  tipo_operacao           text,
  finalidade              text,
  data_emissao            timestamptz,
  uf_emitente             text,
  cnpj_emitente           text,
  nome_emitente           text,
  ie_emitente             text,
  municipio_emitente      text,
  cnpj_destinatario       text,
  nome_destinatario       text,
  uf_destinatario         text,
  municipio_destinatario  text,
  valor_produtos          numeric(15,2),
  valor_total             numeric(15,2),
  valor_icms              numeric(15,2),
  valor_frete             numeric(15,2),
  valor_desconto          numeric(15,2),
  protocolo               text,
  status_codigo           text,
  status_motivo           text,
  data_autorizacao        timestamptz,
  info_complementar       text,
  arquivo_origem          text,
  created_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.nfe_itens (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nota_id         bigint NOT NULL REFERENCES public.nfe_notas(id) ON DELETE CASCADE,
  num_item        int,
  codigo_produto  text,
  ean             text,
  descricao       text,
  ncm             text,
  cfop            text,
  unidade         text,
  quantidade      numeric(15,4),
  valor_unitario  numeric(18,6),
  valor_total     numeric(15,2),
  cst_icms        text,
  aliquota_icms   numeric(7,4),
  valor_icms_item numeric(15,2),
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.nfe_manifestacoes (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sequencia       text,
  codigo_evento   text,
  evento          text,
  lote            text,
  serie           text,
  data_arquivo    date,
  arquivo_origem  text,
  nota_id         bigint REFERENCES public.nfe_notas(id) ON DELETE SET NULL,
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nfe_itens_nota    ON public.nfe_itens(nota_id);
CREATE INDEX IF NOT EXISTS idx_nfe_notas_emissao ON public.nfe_notas(data_emissao);
CREATE INDEX IF NOT EXISTS idx_nfe_notas_dest    ON public.nfe_notas(cnpj_destinatario);
CREATE INDEX IF NOT EXISTS idx_nfe_manif_seq     ON public.nfe_manifestacoes(sequencia);

ALTER TABLE public.nfe_notas         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.nfe_itens         ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.nfe_manifestacoes ENABLE ROW LEVEL SECURITY;

CREATE POLICY "auth full nfe_notas" ON public.nfe_notas
  FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "auth full nfe_itens" ON public.nfe_itens
  FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "auth full nfe_manif" ON public.nfe_manifestacoes
  FOR ALL TO authenticated USING (true) WITH CHECK (true);
