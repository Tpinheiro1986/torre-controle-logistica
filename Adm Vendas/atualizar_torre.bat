@echo off
chcp 65001 >nul
setlocal

REM ============================================================
REM  Torre de Controle — Carga diária ADM de Vendas (ESPD022)
REM  Fluxo: config -> valida data do LST -> ingestão -> publica
REM  Este arquivo SUBSTITUI o atualizar_torre.bat antigo.
REM  (git_utils.py e validar_lst.py devem estar nesta MESMA pasta)
REM ============================================================

cd /d "%~dp0"

if not exist config.bat (
  echo [ERRO] config.bat nao encontrado nesta pasta.
  pause
  exit /b 1
)
call config.bat

echo [1/4] Validando data do relatorio...
python "%~dp0validar_lst.py" "%PASTA_LST%\ESPD022.LST"
if errorlevel 1 goto :erro

echo [2/4] Rodando ingestao do ESPD022...
python "%~dp0ingestao\ingestao_diaria.py"
if errorlevel 1 (
  echo [ERRO] Falha na ingestao. Verifique o LST e a chave do Supabase.
  goto :erro
)

echo [3/4] Publicando no GitHub (commit + pull + push blindados)...
python "%~dp0git_utils.py" --repo "%~dp0." --msg "carga diaria %date%"
if errorlevel 1 goto :erro

echo [4/4] Concluido.
echo.
echo [SUCESSO] Torre atualizada e publicada com sucesso!
pause
exit /b 0

:erro
echo.
echo [ERRO] Falha na carga diaria. Nada foi publicado alem do mostrado acima.
pause
exit /b 1
