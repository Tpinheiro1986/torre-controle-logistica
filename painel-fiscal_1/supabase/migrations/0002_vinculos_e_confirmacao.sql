-- CT-e: novas tabelas/colunas, vinculo NF-e, manifestacao posicional, view da nota origem

ALTER TABLE public.nfe_manifestacoes
  ADD COLUMN IF NOT EXISTS chave_nfe varchar(44),
  ADD COLUMN IF NOT EXISTS numero_nf text,
  ADD COLUMN IF NOT EXISTS cnpj_empresa text,
  ADD COLUMN IF NOT EXISTS campo_aux text;
CREATE INDEX IF NOT EXISTS idx_manif_chave ON public.nfe_manifestacoes(chave_nfe);
CREATE UNIQUE INDEX IF NOT EXISTS uq_manif_chave_arq ON public.nfe_manifestacoes(chave_nfe, arquivo_origem);

CREATE TABLE IF NOT EXISTS public.cte_nfe_ref (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  cte_id bigint NOT NULL REFERENCES public.cte_conhecimentos(id) ON DELETE CASCADE,
  chave_nfe varchar(44) NOT NULL,
  numero_nf text,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (cte_id, chave_nfe)
);
CREATE INDEX IF NOT EXISTS idx_cteref_chave ON public.cte_nfe_ref(chave_nfe);
ALTER TABLE public.cte_nfe_ref ENABLE ROW LEVEL SECURITY;
CREATE POLICY "auth full cte_ref" ON public.cte_nfe_ref FOR ALL TO authenticated USING (true) WITH CHECK (true);

CREATE OR REPLACE VIEW public.vw_nota_vinculos AS
SELECT n.id, n.chave, n.numero, n.serie, n.data_emissao,
  n.nome_emitente, n.nome_destinatario, n.uf_destinatario, n.valor_total,
  EXISTS (SELECT 1 FROM public.cte_nfe_ref r WHERE r.chave_nfe = n.chave) AS tem_cte,
  (SELECT string_agg(DISTINCT c.numero, ', ') FROM public.cte_nfe_ref r
     JOIN public.cte_conhecimentos c ON c.id = r.cte_id WHERE r.chave_nfe = n.chave) AS ctes,
  EXISTS (SELECT 1 FROM public.nfe_manifestacoes m WHERE m.chave_nfe = n.chave) AS tem_confirmacao,
  (SELECT max(m.data_arquivo) FROM public.nfe_manifestacoes m WHERE m.chave_nfe = n.chave) AS data_confirmacao,
  false AS tem_romaneio
FROM public.nfe_notas n ORDER BY n.data_emissao DESC;
GRANT SELECT ON public.vw_nota_vinculos TO authenticated;
