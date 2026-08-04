@echo off
chcp 65001 >nul
setlocal

REM ============================================================
REM  Carga diária ADM-Vendas (ESPD022) — Torre de Controle
REM  Fluxo: valida data do LST -> ingestão -> publica no GitHub
REM ============================================================

set "REPO=C:\Users\thiago.pinheiro\OneDrive - genommalabinternacional\Área de Trabalho\Codigo\torre-controle-logistica"
set "LST=Y:\ERP-12\rpw\ESPD022.LST"

REM >>> AJUSTE AQUI: nome do seu script de ingestão atual <<<
set "INGESTAO=%REPO%\adm-vendas\ingestao_espd022.py"

cd /d "%REPO%"

echo [1/3] Validando data do relatório...
python "%REPO%\validar_lst.py" "%LST%"
if errorlevel 1 goto :erro

echo [2/3] Rodando ingestão do ESPD022...
python "%INGESTAO%"
if errorlevel 1 goto :erro

echo [3/3] Publicando no GitHub...
python "%REPO%\git_utils.py" --repo "%REPO%" --msg "carga diaria %date%"
if errorlevel 1 goto :erro

echo.
echo [SUCESSO] Carga publicada com sucesso.
pause
exit /b 0

:erro
echo.
echo [ERRO] Falha na carga diária. Nada além do que foi mostrado acima foi publicado.
pause
exit /b 1
