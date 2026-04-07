====================================================
 TORRE DE CONTROLE LOGISTICA - Genomma Lab
 Processador de CTe XML  v5
====================================================

ARQUIVOS:
  processar_cte.py   - script principal (nao execute diretamente)
  ATUALIZAR.bat      - execucao diaria (duplo clique)
  REPROCESSAR_TUDO.bat - reprocessamento completo (use raramente)
  REPUBLICAR.bat     - republica sem reprocessar XMLs

----------------------------------------------------
 CONFIGURACAO INICIAL (fazer uma unica vez)
----------------------------------------------------
1. Abra processar_cte.py no VS Code ou Bloco de Notas

2. Confirme as configuracoes na secao CONFIGURACAO:
   PASTA_CTE    = caminho dos XMLs (ja configurado)
   SUPABASE_KEY = cole aqui a chave Legacy service_role
                  (Supabase > Settings > API Keys > Legacy)
   CNPJ_INOVALAB = confirme o CNPJ correto

3. Execute REPROCESSAR_TUDO.bat uma vez para criar o historico

4. Copie index.html para a pasta custo-servir do repositorio
   e publique no GitHub

----------------------------------------------------
 ROTINA DIARIA
----------------------------------------------------
Duplo clique em ATUALIZAR.bat

O que acontece:
  1. Lista os XMLs novos desde a ultima execucao (por data)
  2. Processa somente os novos (214* = CTe, 383* = cancelamento)
  3. Classifica: OUTBOUND / INBOUND / REVERSA
  4. Publica no Supabase
  Tempo estimado: 2-3 minutos

----------------------------------------------------
 CLASSIFICACAO DAS OPERACOES
----------------------------------------------------
OUTBOUND  : remetente = Genomma/Inovalab -> cliente
INBOUND   : remetente = TBC, MARIOL, BELLA PLUS, CRA MAIS,
            SEAL LACRES, GLENMARK, BRASTERAPIC,
            INOVAT GUARU, THERASKIN -> dest = Genomma/Inovalab
REVERSA   : cliente devolvendo -> dest = Genomma/Inovalab

----------------------------------------------------
 SE A PUBLICACAO FALHAR
----------------------------------------------------
Execute REPUBLICAR.bat (nao reprocessa nada, so publica)

----------------------------------------------------
 AGENDAR NO WINDOWS (opcional)
----------------------------------------------------
schtasks /create /tn "Torre-CTE-Diario" 
         /tr "CAMINHO_COMPLETO\ATUALIZAR.bat"
         /sc daily /st 06:00

====================================================
