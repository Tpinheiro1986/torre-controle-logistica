@echo off
title Torre de Controle - Atualizando CTes...
color 0A
cd /d "%~dp0"

echo.
echo  ====================================================
echo   TORRE DE CONTROLE LOGISTICA - Genomma Lab
echo   Processador de CTe XML - INCREMENTAL
echo   Processa somente CTes novos desde a ultima execucao
echo  ====================================================
echo.

python -m pip install requests --quiet --disable-pip-version-check 2>nul

python processar_cte.py

if errorlevel 1 (
    color 0C
    echo.
    echo  ERRO! Verifique: cte_processador.log
    pause
)
