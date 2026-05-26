-- ============================================================
-- POLICIES DO STORAGE para o Editor de Tabelas do Simulador
-- ============================================================
-- 
-- INSTRUÇÕES:
--   1. Acesse: https://supabase.com/dashboard/project/ennsbpibfnuwlvtodukg/sql/new
--   2. Cole TODO o conteúdo deste arquivo
--   3. Clique em "Run" (canto inferior direito)
--   4. Se aparecer erro "policy already exists" para alguma linha, ignore — 
--      significa que aquela policy já estava configurada.
-- 
-- O QUE FAZ:
--   - Permite o usuário admin (thiago_balao@yahoo.com.br) INSERIR e ATUALIZAR 
--     arquivos no bucket "dashboards" dentro da pasta "simulador-frete/" 
--     (incluindo os backups dentro de "simulador-frete/backups/")
--   - Os demais usuários continuam podendo apenas LER os arquivos 
--     (essa policy de leitura pública já existe, não mexemos nela)
-- 
-- PARA ADICIONAR MAIS ADMINS NO FUTURO:
--   Trocar:  auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
--   Por:     auth.jwt() ->> 'email' in ('email1@ex.com', 'email2@ex.com')
-- ============================================================

-- Permissão de INSERÇÃO (criar arquivos novos, incluindo backups)
create policy "Admin pode escrever simulador-frete"
on storage.objects
for insert
to authenticated
with check (
  bucket_id = 'dashboards'
  and (storage.foldername(name))[1] = 'simulador-frete'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
);

-- Permissão de ATUALIZAÇÃO (sobrescrever o tabelas.json)
create policy "Admin pode atualizar simulador-frete"
on storage.objects
for update
to authenticated
using (
  bucket_id = 'dashboards'
  and (storage.foldername(name))[1] = 'simulador-frete'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
);
