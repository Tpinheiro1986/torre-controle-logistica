@echo off
title Torre de Controle — Processando CTes...
color 0A
echo.
echo  ====================================================
echo   TORRE DE CONTROLE LOGISTICA — Genomma Lab
echo   Processador de CTe XML
echo  ====================================================
echo.

cd /d "%~dp0"

echo [1/3] Verificando Python...
python --version >nul 2>&1
if errorlevel 1 (
    color 0C
    echo.
    echo  ERRO: Python nao encontrado!
    echo  Instale em: https://www.python.org/downloads/
    echo  Marque "Add Python to PATH" durante a instalacao.
    pause
    exit /b 1
)

echo [2/3] Instalando dependencias...
pip install requests --quiet --disable-pip-version-check

echo [3/3] Processando CTes (somente novos)...
echo.
python processar_cte.py %*

if errorlevel 1 (
    color 0C
    echo.
    echo  ====================================================
    echo   ERRO durante o processamento!
    echo   Verifique o arquivo: cte_processador.log
    echo  ====================================================
    pause
) else (
    color 0A
)
