# Restringir alterações a um único e-mail

O `index.html` já bloqueia a interface: somente `thiago_balao@yahoo.com.br`
vê os botões de lançar/editar/projetar e consegue salvar. Qualquer outra
pessoa fica em modo **somente leitura**.

> **Atenção:** a trava da interface impede o uso indevido pelo painel, mas
> **não é segurança de verdade**. Quem souber usar o console do navegador
> pode chamar a API do Supabase diretamente, porque a chave publicável fica
> visível na página (isso é normal e esperado no Supabase). A trava
> definitiva é a política abaixo, aplicada no servidor.

## Passo a passo no Supabase

1. Acesse o painel do projeto → **SQL Editor** → **New query**.
2. Cole e execute o script abaixo.

```sql
-- Bucket usado pelo scorecard
-- (troque o e-mail caso o autorizado mude)

-- 1) Remove políticas antigas de escrita neste bucket, se existirem
drop policy if exists "scorecard_insert_admin" on storage.objects;
drop policy if exists "scorecard_update_admin" on storage.objects;
drop policy if exists "scorecard_delete_admin" on storage.objects;
drop policy if exists "scorecard_select_public" on storage.objects;

-- 2) LEITURA: liberada (o painel precisa ler para exibir)
create policy "scorecard_select_public"
on storage.objects for select
using ( bucket_id = 'dashboards' );

-- 3) ESCRITA: somente o e-mail autorizado
create policy "scorecard_insert_admin"
on storage.objects for insert
to authenticated
with check (
  bucket_id = 'dashboards'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
);

create policy "scorecard_update_admin"
on storage.objects for update
to authenticated
using (
  bucket_id = 'dashboards'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
)
with check (
  bucket_id = 'dashboards'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
);

create policy "scorecard_delete_admin"
on storage.objects for delete
to authenticated
using (
  bucket_id = 'dashboards'
  and auth.jwt() ->> 'email' = 'thiago_balao@yahoo.com.br'
);
```

3. Confirme que **Authentication → Providers → Email** está com
   "Enable email signup" **desligado** (ou restrito), para que ninguém
   crie conta nova sozinho.

## Como validar

- Logado como `thiago_balao@yahoo.com.br`: **Lançar/editar dados → Salvar**
  deve mostrar "✓ Salvo no servidor".
- Sem sessão (janela anônima): os botões de edição **não aparecem** e o
  painel abre normalmente em modo leitura.
- Com outro usuário: mesmo forçando pelo console, o Supabase recusa a
  gravação com erro de *row-level security policy*.

## Trocar o e-mail autorizado no futuro

1. No `index.html`, altere a constante `ADMIN_EMAIL`.
2. No SQL acima, troque o e-mail nas três políticas de escrita e execute
   novamente.
